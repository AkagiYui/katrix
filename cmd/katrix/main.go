// Command katrix is the Matrix homeserver binary. It bundles the API server,
// the embedded web panel, and operational subcommands (healthcheck, genkey).
package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AkagiYui/katrix/internal/appservice"
	"github.com/AkagiYui/katrix/internal/config"
	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpserver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// certPEMToX509 parses a PEM-encoded certificate.
func certPEMToX509(pemBytes []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found")
	}
	return x509.ParseCertificate(block.Bytes)
}

// keyPEMToPrivateKey parses a PEM-encoded private key (PKCS8 or PKCS1).
func keyPEMToPrivateKey(pemBytes []byte) (interface{}, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found")
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

// version is set via -ldflags at build time.
var version = "0.1.0-dev"

func main() {
	homeserver.Version = version

	cmd := "serve"
	args := os.Args[1:]
	if len(args) > 0 && !isFlag(args[0]) {
		cmd = args[0]
		args = args[1:]
	}

	var err error
	switch cmd {
	case "serve":
		err = runServe(args)
	case "healthcheck":
		err = runHealthcheck(args)
	case "genkey":
		err = runGenKey(args)
	case "gencert":
		err = runGenCert(args)
	case "version":
		fmt.Println("katrix", version)
	default:
		err = fmt.Errorf("unknown command %q (want serve|healthcheck|genkey|gencert|version)", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "katrix:", err)
		os.Exit(1)
	}
}

func isFlag(s string) bool { return len(s) > 0 && s[0] == '-' }

func configPath(args []string) string {
	for i := 0; i < len(args); i++ {
		if args[i] == "-config" || args[i] == "--config" {
			if i+1 < len(args) {
				return args[i+1]
			}
		}
	}
	return os.Getenv("KATRIX_CONFIG")
}

// loadOrCreateKey reads the signing key file, generating one on first run.
func loadOrCreateKey(path string) (*crypto.SigningKey, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		key, gerr := crypto.GenerateSigningKey("a_" + randVersion())
		if gerr != nil {
			return nil, gerr
		}
		if werr := os.WriteFile(path, []byte(crypto.EncodeSigningKey(key)+"\n"), 0o600); werr != nil {
			return nil, fmt.Errorf("write signing key: %w", werr)
		}
		return key, nil
	}
	if err != nil {
		return nil, err
	}
	return crypto.DecodeSigningKey(string(data))
}

func randVersion() string {
	// Short random key version so multiple keys can coexist.
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}

func runServe(args []string) error {
	cfg, err := config.Load(configPath(args))
	if err != nil {
		return err
	}
	key, err := loadOrCreateKey(cfg.SigningKeyPath)
	if err != nil {
		return fmt.Errorf("signing key: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store, err := storage.OpenWithConfig(ctx, cfg.Database.DSN, cfg.Database.MaxConns, cfg.Database.MinConns)
	if err != nil {
		return err
	}
	defer store.Close()

	// Load application-service registrations (spec "Application services"): the
	// registered as_tokens become valid access tokens for their sender
	// localparts, letting bridge users act through the normal client API.
	asRegistry := appservice.NewRegistry()
	if cfg.AppServiceDir != "" {
		if err := appservice.LoadDir(ctx, store, asRegistry, cfg.AppServiceDir); err != nil {
			return fmt.Errorf("appservice: %w", err)
		}
	}

	hs := homeserver.New(cfg, store, key)
	hs.SetAppServices(asRegistry)
	handler, err := httpserver.New(hs)
	if err != nil {
		return err
	}
	// Start background workers: the MSC4140 delayed-event firing worker and the
	// outbound federation EDU/PDU delivery worker (device-list updates, presence,
	// typing, room events — spec transaction delivery with retries). Both run
	// on goroutines so the HTTP servers below can start immediately.
	handler.CSAPI().StartDelayedWorker(ctx)
	go handler.Federation().RunEDUWorker(ctx)
	// Resume any partial-state resyncs that were interrupted by a restart
	// (MSC3902): a room still flagged partial-state must keep fetching its full
	// state or eager /sync omits it indefinitely.
	if cfg.FederationEnabled {
		handler.Federation().ResumePartialStateResyncs(ctx)
	}

	clientSrv := &http.Server{Addr: cfg.Listen.Client, Handler: handler, ReadHeaderTimeout: 30 * time.Second}
	fedSrv := &http.Server{Addr: cfg.Listen.Federation, Handler: handler, ReadHeaderTimeout: 30 * time.Second}

	errCh := make(chan error, 2)
	go func() {
		fmt.Printf("katrix: client/admin API listening on %s (server_name=%s)\n", cfg.Listen.Client, cfg.ServerName)
		if err := clientSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	if cfg.FederationEnabled && cfg.Listen.Federation != cfg.Listen.Client {
		go func() {
			fmt.Printf("katrix: federation API listening on %s\n", cfg.Listen.Federation)
			var err error
			if cfg.FederationTLS.CertPath != "" && cfg.FederationTLS.KeyPath != "" {
				err = fedSrv.ListenAndServeTLS(cfg.FederationTLS.CertPath, cfg.FederationTLS.KeyPath)
			} else {
				err = fedSrv.ListenAndServe()
			}
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
	}

	select {
	case <-ctx.Done():
		fmt.Println("katrix: shutting down")
	case err := <-errCh:
		return err
	}
	shutdownCtx, sc := context.WithTimeout(context.Background(), 10*time.Second)
	defer sc()
	_ = clientSrv.Shutdown(shutdownCtx)
	_ = fedSrv.Shutdown(shutdownCtx)
	return nil
}

func runHealthcheck(args []string) error {
	url := os.Getenv("KATRIX_HEALTHCHECK_URL")
	if url == "" {
		cfg, err := config.Load(configPath(args))
		if err == nil {
			addr := cfg.Listen.Client
			if len(addr) > 0 && addr[0] == ':' {
				addr = "127.0.0.1" + addr
			}
			url = "http://" + addr + "/health"
		} else {
			url = "http://127.0.0.1:8008/health"
		}
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unhealthy: status %d", resp.StatusCode)
	}
	fmt.Println("ok")
	return nil
}

func runGenKey(args []string) error {
	key, err := crypto.GenerateSigningKey("a_" + randVersion())
	if err != nil {
		return err
	}
	fmt.Println(crypto.EncodeSigningKey(key))
	return nil
}

// runGenCert generates a TLS leaf certificate signed by a CA, for the
// federation listener. Used by the Complement entrypoint: Complement mounts
// its CA at /complement/ca/ca.crt + /complement/ca/ca.key, and the homeserver
// must present a certificate signed by that CA on :8448.
//
// Usage: katrix gencert -ca-cert <path> -ca-key <path> -server-name <name>
//
//	-out-cert <path> -out-key <path>
func runGenCert(args []string) error {
	var caCertPath, caKeyPath, serverName, outCert, outKey string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-ca-cert", "--ca-cert":
			i++
			if i < len(args) {
				caCertPath = args[i]
			}
		case "-ca-key", "--ca-key":
			i++
			if i < len(args) {
				caKeyPath = args[i]
			}
		case "-server-name", "--server-name":
			i++
			if i < len(args) {
				serverName = args[i]
			}
		case "-out-cert", "--out-cert":
			i++
			if i < len(args) {
				outCert = args[i]
			}
		case "-out-key", "--out-key":
			i++
			if i < len(args) {
				outKey = args[i]
			}
		}
	}
	if caCertPath == "" {
		caCertPath = os.Getenv("KATRIX_GENCERT_CA_CERT")
	}
	if caKeyPath == "" {
		caKeyPath = os.Getenv("KATRIX_GENCERT_CA_KEY")
	}
	if serverName == "" {
		serverName = os.Getenv("KATRIX_SERVER_NAME")
	}
	if outCert == "" {
		outCert = os.Getenv("KATRIX_GENCERT_OUT_CERT")
	}
	if outKey == "" {
		outKey = os.Getenv("KATRIX_GENCERT_OUT_KEY")
	}
	if caCertPath == "" || caKeyPath == "" || serverName == "" || outCert == "" || outKey == "" {
		return fmt.Errorf("gencert: -ca-cert, -ca-key, -server-name, -out-cert, -out-key are required")
	}
	caCertPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return fmt.Errorf("gencert: read CA cert: %w", err)
	}
	caKeyPEM, err := os.ReadFile(caKeyPath)
	if err != nil {
		return fmt.Errorf("gencert: read CA key: %w", err)
	}
	caCert, err := certPEMToX509(caCertPEM)
	if err != nil {
		return fmt.Errorf("gencert: parse CA cert: %w", err)
	}
	caKey, err := keyPEMToPrivateKey(caKeyPEM)
	if err != nil {
		return fmt.Errorf("gencert: parse CA key: %w", err)
	}

	// Generate a leaf private key + certificate signed by the CA.
	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("gencert: generate key: %w", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: serverName},
		DNSNames:     []string{serverName, "localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("gencert: sign cert: %w", err)
	}

	certOut, err := os.Create(outCert)
	if err != nil {
		return err
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		return err
	}
	certOut.Close()

	keyOut, err := os.OpenFile(outKey, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer keyOut.Close()
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		return err
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}); err != nil {
		return err
	}
	fmt.Printf("gencert: wrote %s and %s for %s\n", outCert, outKey, serverName)
	return nil
}
