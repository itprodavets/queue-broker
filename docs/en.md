# queue-broker — Documentation (English)

A minimal HTTP message‑queue broker written in Go using **only the standard
library**. Messages are stored per named queue and delivered in **FIFO** order;
`GET` can **block** until a message arrives or a timeout elapses, and waiting
consumers are served in the order they asked.

← [Back to README](../README.md) · 📗 [Русская версия](ru.md)

## Build & run

```bash
go build -o broker .
./broker 8080          # the listening port is a required CLI argument
```

The assignment examples use port 80 (`curl http://127.0.0.1/pet`); run
`./broker 80` (may require elevated privileges) to match them verbatim.

## API

| Method & path | Behaviour |
| --- | --- |
| `PUT /{queue}?v={message}` | Enqueue `message`. `200` on success; `400` if `v` is absent. |
| `GET /{queue}` | Dequeue the oldest message → `200` + body, or `404` if empty. |
| `GET /{queue}?timeout={N}` | Wait up to `N` seconds for a message; `404` if none arrives. |

Queue names are arbitrary. If several consumers wait on the same queue, the one
that asked first receives the first message.

## Example (the exact assignment sequence)

```bash
curl -XPUT 'http://127.0.0.1:8080/pet?v=cat'        # 200
curl -XPUT 'http://127.0.0.1:8080/pet?v=dog'        # 200
curl -XPUT 'http://127.0.0.1:8080/role?v=manager'   # 200
curl -XPUT 'http://127.0.0.1:8080/role?v=executive' # 200

curl 'http://127.0.0.1:8080/pet'   # => cat
curl 'http://127.0.0.1:8080/pet'   # => dog
curl 'http://127.0.0.1:8080/pet'   # => (empty, 404)
curl 'http://127.0.0.1:8080/role'  # => manager
curl 'http://127.0.0.1:8080/role'  # => executive
curl 'http://127.0.0.1:8080/role'  # => (empty, 404)

curl 'http://127.0.0.1:8080/pet?timeout=10'  # blocks up to 10s, then 404
```

## Design

- One mutex guards a map of queues. Each queue holds **either** buffered
  messages **or** an ordered list of waiting consumers — never both.
- A blocked consumer registers a one‑slot channel and is woken **directly** by
  the next `Put`, which preserves request order without polling or broadcasts.
- A consumer that times out or disconnects removes itself; a message handed to
  it at that exact instant is re‑delivered rather than dropped.
- Emptied queues are reclaimed so unused names do not accumulate.
- No third‑party packages; the whole service lives in [`main.go`](../main.go).

## Tests

```bash
go test -race ./...
```

Covers FIFO order, the `400`/`404`/`405` cases, blocking and timeout, request
ordering between waiters, message safety under concurrency and disconnects, and
queue reclamation.

---

*Test assignment for the Golang Developer (Middle+) position at НДМ Системы —
[hh.ru/vacancy/134426884](https://hh.ru/vacancy/134426884) (archived in
[`vacancy.md`](../vacancy.md)).*
