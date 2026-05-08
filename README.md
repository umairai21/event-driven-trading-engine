# 📈 Event-Driven Trading Engine

A high-performance, distributed microservices architecture simulating a real-time cryptocurrency trading backend. 

This project demonstrates enterprise-grade backend engineering patterns, utilizing **NATS JetStream** for asynchronous event streaming, **gRPC** for synchronous service-to-service communication, and **PostgreSQL** for persistent, ACID-compliant ledger transactions.

## 🏗️ Architecture Overview

The system is fully decoupled into four distinct microservices communicating securely across an internal trust boundary (East-West traffic). 

1. **Market Data Service (Ingestor):** Polls the live Binance public API for real-time cryptocurrency prices (BTC, ETH, SOL) and broadcasts them to a NATS JetStream topic (`MARKET.prices.*`).
2. **Strategy Engine (The Brain):** A durable consumer that listens to the live price stream. When a price drops below a configurable threshold, it generates an `OrderRequest` and publishes it to the NATS `ORDERS.new` stream.
3. **Execution Service (The Ledger):** Consumes pending orders from NATS. Before executing a trade, it makes a synchronous RPC call to the Risk Service. Upon approval, it executes a safe database transaction to deduct user funds and record the trade.
4. **Risk Service (The Gatekeeper):** A standalone gRPC server that evaluates incoming trade requests against internal risk rules (e.g., rejecting single trades exceeding $5,000) and instantly returns an approval/rejection boolean.

## 🚀 Key Technologies

*   **Go (Golang):** Core language for all microservices, chosen for its concurrency and performance.
*   **NATS JetStream:** Message broker handling asynchronous pub/sub data with durable consumers and message persistence.
*   **gRPC & Protocol Buffers:** High-speed, synchronous point-to-point communication for immediate risk verification.
*   **PostgreSQL:** Relational database ensuring strict ACID compliance for financial transactions.
*   **Docker:** Containerized infrastructure for the message broker and database.

## ⚙️ Prerequisites

*   [Go](https://golang.org/doc/install) (1.19+)
*   [Docker Desktop](https://www.docker.com/products/docker-desktop)
*   Make sure port `5433` (PostgreSQL), `4222` (NATS), and `50051` (gRPC) are available on your machine.

## 🛠️ How to Run Locally

**1. Start the Infrastructure**
Spin up the NATS broker and PostgreSQL database using Docker Compose:
```bash
docker-compose up -d