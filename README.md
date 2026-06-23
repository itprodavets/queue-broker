# queue-broker

Minimal HTTP message‑queue broker in Go — FIFO per named queue, blocking `GET`
with timeout, **standard library only**.
<br>
Минимальный брокер очередей по HTTP на Go — FIFO по очередям, блокирующий `GET`
с таймаутом, **только стандартная библиотека**.

## Documentation · Документация

- 📘 [English](docs/en.md)
- 📗 [Русский](docs/ru.md)

## Quick start

```bash
go build -o broker .
./broker 8080          # port is a required CLI argument
go test -race ./...
```

```bash
curl -XPUT 'http://127.0.0.1:8080/pet?v=cat'   # 200
curl       'http://127.0.0.1:8080/pet'         # => cat
curl       'http://127.0.0.1:8080/pet'         # => (empty, 404)
curl       'http://127.0.0.1:8080/pet?timeout=10'  # blocks up to 10s, then 404
```

---

> Test assignment for the **Golang Developer (Middle+)** position at
> **НДМ Системы / NDM Systems** — [hh.ru/vacancy/134426884](https://hh.ru/vacancy/134426884).
> A local copy of the posting is kept in [`vacancy.md`](vacancy.md).
