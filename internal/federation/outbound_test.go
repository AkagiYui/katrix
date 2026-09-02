package federation

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/config"
	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/homeserver"
)

// testAPI builds an API wired to the test store, with federation TLS
// verification disabled so httptest TLS servers can stand in for peers.
func testAPI(t *testing.T) *API {
	t.Helper()
	store := testStore(t)
	key, err := crypto.GenerateSigningKey("1")
	if err != nil {
		t.Fatal(err)
	}
	hs := homeserver.New(&config.Config{ServerName: "local.test", FederationInsecure: true}, store, key)
	return New(hs)
}

// peer is a stand-in remote homeserver that records the transactions it is
// given and can be told to fail.
type peer struct {
	srv  *httptest.Server
	host string

	mu      sync.Mutex
	txns    []peerTxn
	failing bool
}

type peerTxn struct {
	txnID string
	pdus  int
	edus  int
}

func newPeer(t *testing.T) *peer {
	t.Helper()
	p := &peer{}
	p.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			PDUs []json.RawMessage `json:"pdus"`
			EDUs []json.RawMessage `json:"edus"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		p.mu.Lock()
		failing := p.failing
		if !failing {
			p.txns = append(p.txns, peerTxn{
				txnID: path.Base(r.URL.Path),
				pdus:  len(body.PDUs),
				edus:  len(body.EDUs),
			})
		}
		p.mu.Unlock()
		if failing {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pdus":{}}`))
	}))
	t.Cleanup(p.srv.Close)
	u, err := url.Parse(p.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	p.host = u.Host
	return p
}

func (p *peer) setFailing(v bool) {
	p.mu.Lock()
	p.failing = v
	p.mu.Unlock()
}

func (p *peer) received() []peerTxn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]peerTxn(nil), p.txns...)
}

// deadPeer is a destination nothing listens on, so every attempt fails at once.
const deadPeer = "127.0.0.1:1"

// TestDrainOutboundIsolatesFailingDestination is the regression test for the
// outbound worker wedging on an unreachable peer. A single EDU queued for a
// dead server used to make drainOutbound loop forever re-fetching it, so every
// PDU behind it — for every other destination — was never delivered at all.
func TestDrainOutboundIsolatesFailingDestination(t *testing.T) {
	api := testAPI(t)
	live := newPeer(t)
	ctx := context.Background()

	if err := api.Store.InsertOutboundEDU(ctx, "t1", "m.presence",
		json.RawMessage(`{"user_id":"@a:local.test"}`), []string{deadPeer}, api.Now()); err != nil {
		t.Fatal(err)
	}
	if err := api.Store.InsertOutboundPDU(ctx, "t2", "", "$e1",
		json.RawMessage(`{"type":"m.room.message"}`), []string{live.host}, api.Now()); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := api.drainOutbound(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drainOutbound: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("drainOutbound did not return: the dead destination wedged the worker")
	}

	got := live.received()
	if len(got) != 1 || got[0].pdus != 1 {
		t.Fatalf("healthy peer got %+v, want one transaction carrying one PDU", got)
	}
}

// TestDrainOutboundBacksOffFailingDestination checks that a failed transaction
// parks its destination instead of being retried inline, and that the backoff
// grows with consecutive failures.
func TestDrainOutboundBacksOffFailingDestination(t *testing.T) {
	api := testAPI(t)
	broken := newPeer(t)
	broken.setFailing(true)
	ctx := context.Background()

	if err := api.Store.InsertOutboundPDU(ctx, "t1", "", "$e1",
		json.RawMessage(`{"type":"m.room.message"}`), []string{broken.host}, api.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := api.drainOutbound(ctx); err != nil {
		t.Fatal(err)
	}
	// Parked: the destination is no longer due, so a second pass finds no work.
	dests, err := api.Store.DueDestinations(ctx, api.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dests) != 0 {
		t.Fatalf("failing destination still due immediately: %v", dests)
	}
	// ...and it is due again once its backoff elapses.
	dests, err = api.Store.DueDestinations(ctx, api.Now()+destMaxBackoff.Milliseconds()+1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dests) != 1 || dests[0] != broken.host {
		t.Fatalf("destination not retried after backoff: %v", dests)
	}
	// The PDU is still queued: PDUs are room history and are never dropped.
	pdus, err := api.Store.PendingPDUsForDestination(ctx, broken.host, 10)
	if err != nil || len(pdus) != 1 {
		t.Fatalf("queued PDU lost: %v (%d rows)", err, len(pdus))
	}
}

// TestDeliverToDestinationBatchesAndRecovers covers the happy path: everything
// owed to one server goes in a single transaction within the spec's size
// limits, and a destination that comes back is delivered to again.
func TestDeliverToDestinationBatchesAndRecovers(t *testing.T) {
	api := testAPI(t)
	p := newPeer(t)
	p.setFailing(true)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := api.Store.InsertOutboundPDU(ctx, "t", "", "$e",
			json.RawMessage(`{"type":"m.room.message"}`), []string{p.host}, api.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if err := api.Store.InsertOutboundEDU(ctx, "e", "m.typing",
		json.RawMessage(`{"user_id":"@a:local.test"}`), []string{p.host}, api.Now()); err != nil {
		t.Fatal(err)
	}
	api.deliverToDestination(ctx, p.host)
	if got := p.received(); len(got) != 0 {
		t.Fatalf("failing peer recorded %+v", got)
	}

	p.setFailing(false)
	api.NoteDestinationAlive(ctx, p.host)
	if dests, err := api.Store.DueDestinations(ctx, api.Now(), 10); err != nil || len(dests) != 1 {
		t.Fatalf("destination not due after NoteDestinationAlive: %v %v", dests, err)
	}
	api.deliverToDestination(ctx, p.host)

	got := p.received()
	if len(got) != 1 {
		t.Fatalf("want a single batched transaction, got %+v", got)
	}
	if got[0].pdus != 3 || got[0].edus != 1 {
		t.Fatalf("batch = %d PDUs/%d EDUs, want 3/1", got[0].pdus, got[0].edus)
	}
	if got[0].pdus > fedMaxPDUsPerTxn || got[0].edus > fedMaxEDUsPerTxn {
		t.Fatalf("batch exceeds the spec transaction limits: %+v", got[0])
	}
	// Fully acknowledged, so nothing is owed and no backoff row survives.
	if pdus, err := api.Store.PendingPDUsForDestination(ctx, p.host, 10); err != nil || len(pdus) != 0 {
		t.Fatalf("PDUs still queued after ack: %v (%d rows)", err, len(pdus))
	}
	// The rows must actually leave the table, not linger with an empty
	// destination set: nothing else ever revisits a drained row.
	var leftover int
	if err := api.Store.Pool().QueryRow(ctx,
		`SELECT (SELECT count(*) FROM outbound_pdus) + (SELECT count(*) FROM outbound_edus)`).Scan(&leftover); err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Fatalf("%d drained queue rows left behind", leftover)
	}
	if dests, err := api.Store.DueDestinations(ctx, api.Now(), 10); err != nil || len(dests) != 0 {
		t.Fatalf("destination still has work: %v %v", dests, err)
	}
}

// TestBatchTxnIDTracksBatchContents pins the transaction-ID contract: the
// receiving server de-duplicates on (origin, txnId) and answers a repeat with
// an empty result, so a retry of the same batch must reuse the ID while a
// batch with different contents must not.
func TestBatchTxnIDTracksBatchContents(t *testing.T) {
	api := testAPI(t)
	p := newPeer(t)
	p.setFailing(true)
	ctx := context.Background()

	if err := api.Store.InsertOutboundPDU(ctx, "t", "", "$e1",
		json.RawMessage(`{"type":"m.room.message"}`), []string{p.host}, api.Now()); err != nil {
		t.Fatal(err)
	}
	api.deliverToDestination(ctx, p.host) // fails, nothing acknowledged
	p.setFailing(false)
	api.deliverToDestination(ctx, p.host)
	first := p.received()

	// Same row, delivered again after a failure: same transaction ID.
	if err := api.Store.InsertOutboundPDU(ctx, "t", "", "$e2",
		json.RawMessage(`{"type":"m.room.message"}`), []string{p.host}, api.Now()); err != nil {
		t.Fatal(err)
	}
	api.deliverToDestination(ctx, p.host)
	all := p.received()
	if len(all) != 2 {
		t.Fatalf("want two transactions, got %+v", all)
	}
	if all[0].txnID == all[1].txnID {
		t.Fatalf("different batches reused txnID %s — the peer would drop the second", all[0].txnID)
	}
	if len(first) != 1 || first[0].txnID != all[0].txnID {
		t.Fatalf("txnID changed across the retry of an unchanged batch")
	}
}
