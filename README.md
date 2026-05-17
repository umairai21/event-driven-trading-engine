# 📈 Event-Driven Trading Engine

A high-performance, distributed microservices architecture simulating a real-time cryptocurrency trading backend.

This project demonstrates enterprise-grade backend engineering patterns, utilizing **NATS JetStream** for asynchronous event streaming, **gRPC** for synchronous service-to-service communication, and **PostgreSQL** for persistent, ACID-compliant ledger transactions.

---

## 🏗️ Architecture Overview

The system is fully decoupled into four distinct microservices communicating securely across an internal trust boundary (East-West traffic).

| Service | Role | Description |
|---|---|---|
| **Market Data Service** | Ingestor | Polls the live Binance public API for real-time crypto prices (BTC, ETH, SOL) and broadcasts them to a NATS JetStream topic (`MARKET.prices.*`) |
| **Strategy Engine** | The Brain | Durable consumer that listens to the live price stream. When a price drops below a configurable threshold, it generates an `OrderRequest` and publishes it to the NATS `ORDERS.new` stream |
| **Execution Service** | The Ledger | Consumes pending orders from NATS. Before executing, makes a synchronous RPC call to the Risk Service. Upon approval, executes a safe DB transaction to deduct funds and record the trade |
| **Risk Service** | The Gatekeeper | A standalone gRPC server that evaluates trade requests against internal risk rules (e.g., rejecting single trades exceeding $5,000) and returns an approval/rejection boolean |

---

## 🚀 Key Technologies

- **Go (Golang)** — Core language for all microservices, chosen for its concurrency and performance
- **NATS JetStream** — Message broker handling async pub/sub with durable consumers and message persistence
- **gRPC & Protocol Buffers** — High-speed, synchronous point-to-point communication for immediate risk verification
- **PostgreSQL** — Relational database ensuring strict ACID compliance for financial transactions
- **Docker & CI/CD** — Containerized infrastructure and automated GitHub Actions pipeline

---

## ⚙️ Prerequisites

- [Go](https://golang.org/doc/install) `1.19+`
- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- Ensure the following ports are free on your machine:
  - `5433` — PostgreSQL
  - `4222` — NATS
  - `50051` — gRPC

---

## 🛠️ How to Run Locally

### 1. Start the Infrastructure

Spin up the NATS broker and PostgreSQL database using Docker Compose:

```bash
docker-compose up -d
```

### 2. Boot the Microservices

Open **four separate terminal windows** and start the microservices in this sequence to ensure all listeners are active before data starts flowing:

**Terminal 1 — Risk Service (gRPC Server)**
```bash
go run ./cmd/risk_service/main.go
```

**Terminal 2 — Execution Service (Ledger)**
```bash
go run ./cmd/execution_service/main.go
```

**Terminal 3 — Strategy Engine**
```bash
go run ./cmd/strategy_engine/main.go
```

**Terminal 4 — Market Data Feed**
```bash
go run ./cmd/market_data/main.go
```

---

## 🧪 Testing & CI/CD

This project includes automated table-driven unit tests to verify core risk and execution logic.

**Run the test suite locally:**
```bash
go test -v ./...
```

**Continuous Integration:** A GitHub Actions workflow (`ci.yml`) is configured to automatically build all microservices and execute the test suite on every push and pull request to `main`, ensuring code quality and deployment readiness.

---

## 📊 System Flow Example

When the system is running, the terminal logs display the sub-second execution pipeline:

```
📡 REALTIME  Published SOLUSDT: $92.30                          (Market Service)
💡 SIGNAL    SOLUSDT dropped to $92.30! Initiating BUY order.  (Strategy Engine)
📞 CALLING   Risk Service via gRPC...                           (Execution Service)
✅ APPROVED  Risk Service approved the trade.                   (Risk Service)
✅ EXECUTED  Order: SOLUSDT. New Balance: $9077.00              (Execution Service)
```

> **Note:** The Execution Service uses a **durable NATS consumer**. If taken offline, NATS will persist unhandled orders and instantly deliver the backlog the moment the service reboots.