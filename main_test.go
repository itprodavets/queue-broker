package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// do runs a single request against h and returns the recorded response.
func do(h http.Handler, method, target string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, target, nil))
	return w
}

func newHandler() handler {
	b := newBroker()
	return handler{producer: b, consumer: b}
}

func TestPutThenGetIsFIFO(t *testing.T) {
	h := newHandler()
	do(h, "PUT", "/pet?v=cat")
	do(h, "PUT", "/pet?v=dog")
	for _, want := range []string{"cat", "dog"} {
		if got := do(h, "GET", "/pet").Body.String(); got != want {
			t.Fatalf("GET /pet = %q, want %q", got, want)
		}
	}
	if code := do(h, "GET", "/pet").Code; code != http.StatusNotFound {
		t.Fatalf("empty queue: code %d, want 404", code)
	}
}

func TestQueuesAreIndependent(t *testing.T) {
	h := newHandler()
	do(h, "PUT", "/pet?v=cat")
	do(h, "PUT", "/role?v=manager")
	if got := do(h, "GET", "/role").Body.String(); got != "manager" {
		t.Fatalf("GET /role = %q, want manager", got)
	}
	if got := do(h, "GET", "/pet").Body.String(); got != "cat" {
		t.Fatalf("GET /pet = %q, want cat", got)
	}
}

func TestPutWithoutValueIsBadRequest(t *testing.T) {
	if code := do(newHandler(), "PUT", "/pet").Code; code != http.StatusBadRequest {
		t.Fatalf("code %d, want 400", code)
	}
}

func TestGetEmptyWithoutTimeoutIsNotFound(t *testing.T) {
	if code := do(newHandler(), "GET", "/pet").Code; code != http.StatusNotFound {
		t.Fatalf("code %d, want 404", code)
	}
}

func TestUnsupportedMethodIsRejectedWithoutConsuming(t *testing.T) {
	h := newHandler()
	do(h, "PUT", "/pet?v=cat")
	if code := do(h, "POST", "/pet").Code; code != http.StatusMethodNotAllowed {
		t.Fatalf("code %d, want 405", code)
	}
	if got := do(h, "GET", "/pet").Body.String(); got != "cat" {
		t.Fatalf("message consumed by POST: GET = %q, want cat", got)
	}
}

func TestGetBlocksUntilPut(t *testing.T) {
	h := newHandler()
	got := make(chan string, 1)
	go func() { got <- do(h, "GET", "/pet?timeout=5").Body.String() }()
	time.Sleep(50 * time.Millisecond) // let the getter block
	do(h, "PUT", "/pet?v=cat")
	select {
	case msg := <-got:
		if msg != "cat" {
			t.Fatalf("woken GET = %q, want cat", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("GET did not return after PUT")
	}
}

func TestGetTimeoutExpires(t *testing.T) {
	start := time.Now()
	w := do(newHandler(), "GET", "/pet?timeout=1")
	if w.Code != http.StatusNotFound {
		t.Fatalf("code %d, want 404", w.Code)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("returned after %v, expected to wait ~1s", elapsed)
	}
}

// TestWaitersServedInRequestOrder is the headline requirement: the consumer
// that asked first must receive the first message.
func TestWaitersServedInRequestOrder(t *testing.T) {
	b := newBroker()
	first, second := make(chan string, 1), make(chan string, 1)
	go func() { m, _ := b.Take(context.Background(), "q", 5*time.Second); first <- m }()
	time.Sleep(50 * time.Millisecond)
	go func() { m, _ := b.Take(context.Background(), "q", 5*time.Second); second <- m }()
	time.Sleep(50 * time.Millisecond)
	b.Put("q", "one")
	b.Put("q", "two")
	if m := <-first; m != "one" {
		t.Fatalf("first waiter got %q, want one", m)
	}
	if m := <-second; m != "two" {
		t.Fatalf("second waiter got %q, want two", m)
	}
}

func TestTakeReturnsOnContextCancel(t *testing.T) {
	b := newBroker()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	if _, ok := b.Take(ctx, "q", 5*time.Second); ok {
		t.Fatal("Take returned a message after cancel")
	}
}

// TestMessageSurvivesConsumerDisconnect proves a message is not lost when a
// waiting consumer disconnects before delivery: it goes to the next consumer.
func TestMessageSurvivesConsumerDisconnect(t *testing.T) {
	b := newBroker()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Take(ctx, "q", 5*time.Second); close(done) }()
	time.Sleep(50 * time.Millisecond) // consumer registers as a waiter
	cancel()                          // consumer disconnects
	<-done                            // and has removed itself from the queue
	b.Put("q", "survivor")
	if msg, ok := b.Take(context.Background(), "q", time.Second); !ok || msg != "survivor" {
		t.Fatalf("message lost on disconnect: ok=%v msg=%q", ok, msg)
	}
}

// TestConcurrentProducersNoLoss floods one queue from many goroutines and then
// drains it, asserting every message comes out exactly once.
func TestConcurrentProducersNoLoss(t *testing.T) {
	b := newBroker()
	const n = 500
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); b.Put("q", strconv.Itoa(i)) }(i)
	}
	wg.Wait()
	seen := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		msg, ok := b.Take(context.Background(), "q", 0)
		if !ok {
			t.Fatalf("missing message after %d reads", i)
		}
		if seen[msg] {
			t.Fatalf("duplicate message %q", msg)
		}
		seen[msg] = true
	}
	if _, ok := b.Take(context.Background(), "q", 0); ok {
		t.Fatal("unexpected extra message")
	}
}

// TestConcurrentProducersAndConsumers runs N producers against N waiting
// consumers; every consumer must receive a distinct message.
func TestConcurrentProducersAndConsumers(t *testing.T) {
	b := newBroker()
	const n = 300
	got := make(chan string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m, ok := b.Take(context.Background(), "q", 5*time.Second); ok {
				got <- m
			}
		}()
	}
	for i := 0; i < n; i++ {
		go b.Put("q", strconv.Itoa(i))
	}
	wg.Wait()
	close(got)
	seen := make(map[string]bool, n)
	for m := range got {
		if seen[m] {
			t.Fatalf("duplicate message %q", m)
		}
		seen[m] = true
	}
	if len(seen) != n {
		t.Fatalf("received %d distinct messages, want %d", len(seen), n)
	}
}

// stubBroker is a fake Producer/Consumer used to show the handler is decoupled
// from the concrete broker (the payoff of depending on interfaces).
type stubBroker struct {
	put  func(queue, message string)
	take func(context.Context, string, time.Duration) (string, bool)
}

func (s stubBroker) Put(q, m string) { s.put(q, m) }
func (s stubBroker) Take(ctx context.Context, q string, d time.Duration) (string, bool) {
	return s.take(ctx, q, d)
}

func TestHandlerDelegatesToProducerAndConsumer(t *testing.T) {
	var gotQueue, gotValue string
	stub := stubBroker{
		put:  func(q, v string) { gotQueue, gotValue = q, v },
		take: func(context.Context, string, time.Duration) (string, bool) { return "stubbed", true },
	}
	h := handler{producer: stub, consumer: stub}

	do(h, "PUT", "/pet?v=cat")
	if gotQueue != "pet" || gotValue != "cat" {
		t.Fatalf("producer received (%q, %q), want (pet, cat)", gotQueue, gotValue)
	}
	if body := do(h, "GET", "/pet").Body.String(); body != "stubbed" {
		t.Fatalf("consumer body = %q, want stubbed", body)
	}
}
