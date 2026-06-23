// Command broker is an HTTP message-queue broker: FIFO messages per named
// queue, with a blocking GET that waits up to a timeout for a message.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// Producer enqueues a message into a named queue.
type Producer interface {
	Put(queue, message string)
}

// Consumer dequeues a message from a named queue in FIFO order, waiting up to
// timeout or until ctx is cancelled. ok is false when no message was obtained.
type Consumer interface {
	Take(ctx context.Context, queue string, timeout time.Duration) (message string, ok bool)
}

// queue holds either buffered messages or waiting consumers. Its invariant is
// that only one of the two slices is ever non-empty at a time.
type queue struct {
	messages []string
	waiters  []chan string
}

// broker is a thread-safe set of named in-memory queues. It implements both
// Producer and Consumer.
type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

// queue returns the named queue, creating it on first use. Must hold b.mu.
func (b *broker) queue(name string) *queue {
	q := b.queues[name]
	if q == nil {
		q = &queue{}
		b.queues[name] = q
	}
	return q
}

// Put hands the message to the oldest waiting consumer if one exists, otherwise
// appends it to the queue's buffer.
func (b *broker) Put(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queue(name)
	if len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		w <- msg // buffered (cap 1): never blocks while holding the lock
		return
	}
	q.messages = append(q.messages, msg)
}

// Take removes the next message in FIFO order. If the queue is empty it waits
// until a message arrives, the timeout elapses, or the request is cancelled.
func (b *broker) Take(ctx context.Context, name string, timeout time.Duration) (string, bool) {
	b.mu.Lock()
	q := b.queue(name)
	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		b.mu.Unlock()
		return msg, true
	}
	if timeout <= 0 {
		b.mu.Unlock()
		return "", false
	}
	w := make(chan string, 1)
	q.waiters = append(q.waiters, w) // join the waiter queue in arrival order
	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg := <-w:
		return msg, true
	case <-timer.C:
	case <-ctx.Done():
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	for i, c := range q.waiters {
		if c == w { // still queued: cancel our spot, the next message goes to someone else
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return "", false
		}
	}
	return <-w, true // a Put delivered just as we woke up: take it rather than drop it
}

// handler exposes a Producer/Consumer over HTTP. It depends on the interfaces,
// not on the concrete broker.
type handler struct {
	producer Producer
	consumer Consumer
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[1:]
	switch r.Method {
	case http.MethodPut:
		v := r.URL.Query().Get("v")
		if v == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		h.producer.Put(name, v)
	case http.MethodGet:
		timeout, _ := strconv.Atoi(r.URL.Query().Get("timeout"))
		if msg, ok := h.consumer.Take(r.Context(), name, time.Duration(timeout)*time.Second); ok {
			fmt.Fprint(w, msg)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: broker <port>")
		os.Exit(1)
	}
	b := newBroker()
	if err := http.ListenAndServe(":"+os.Args[1], handler{producer: b, consumer: b}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
