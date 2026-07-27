// Command katrix is the Matrix homeserver binary. It bundles the API server,
// the embedded web panel, and operational subcommands (healthcheck, genkey).
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AkagiYui/katrix/internal/config"
	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpserver"
	"github.com/AkagiYui/katrix/internal/storage"
)

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
	case "version":
		fmt.Println("katrix", version)
	default:
		err = fmt.Errorf("unknown command %q (want serve|healthcheck|genkey|version)", cmd)
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

	hs := homeserver.New(cfg, store, key)
	handler, err := httpserver.New(hs)
	if err != nil {
		return err
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
			if err := fedSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
