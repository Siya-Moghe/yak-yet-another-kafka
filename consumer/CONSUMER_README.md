# YAK Consumer Client

## Overview

The YAK Consumer Client is a distributed message consumption application designed to work with the YAK message broker system. It provides reliable, ordered message consumption with automatic leader discovery, local persistence, and offset tracking.

## Architecture

### Component Design

The consumer client is built on three core components:

1. **ConsumerStorage**: Handles local persistence of consumed messages
2. **TopicConsumer**: Manages the consumption loop, leader discovery, and offset tracking
3. **ColoredFormatter**: Provides enhanced logging with visual distinction between log levels

### Data Flow

```
Consumer Client
    ↓
Leader Discovery (via /metadata/leader)
    ↓
Fetch Messages (via /consume?topic=X&offset=Y)
    ↓
Local Storage (filesystem persistence)
    ↓
Offset Update (in-memory tracking)
```

## Features

### 1. Automatic Leader Discovery

The consumer implements a broker discovery mechanism that queries all configured broker addresses to identify the current leader. This ensures the consumer can connect to the correct broker even in the event of leader failover.

**Algorithm:**
- Iterate through configured broker addresses
- Query each broker's `/metadata/leader` endpoint
- Extract leader information (host, port)
- Cache leader address for subsequent requests
- Fallback to first broker if discovery fails

### 2. Offset Management

The consumer maintains strict offset tracking to ensure:
- **No duplicate consumption**: Each message is processed exactly once
- **Resume capability**: Consumer can restart from last consumed offset
- **Crash recovery**: Offsets are persisted to local storage

**Offset Calculation:**
```
Initial Offset = max(local_messages.offset) + 1
Current Offset = last_consumed_message.offset + 1
```

### 3. Local Persistence

Messages are stored locally in a structured directory format:

```
./data/
└── {topic}/
    └── partition-0/
        └── messages.log
```

Each message is stored as a JSON object on a separate line, following the JSON Lines format for efficient append-only writes and sequential reads.

### 4. Fault Tolerance

**Connection Failures:**
- Automatic retry with exponential backoff (configurable via `SLEEP_BETWEEN_POLL`)
- Leader rediscovery on connection timeout
- Graceful degradation with warning logs

**Data Consistency:**
- Atomic offset updates after successful message storage
- No offset advancement on storage failure
- Local storage acts as write-ahead log

## Configuration

### Command-Line Arguments

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `--brokers` | Yes | Comma-separated list of broker addresses | `localhost:8001,localhost:8002` |
| `--topic` | Yes | Topic name to consume from | `orders` |

### Environment Variables

The following constants can be modified in the source code:

| Variable | Default | Description |
|----------|---------|-------------|
| `REQUEST_TIMEOUT` | 5 seconds | HTTP request timeout for broker communication |
| `SLEEP_BETWEEN_POLL` | 2 seconds | Interval between consumption attempts |

## Usage

### Basic Usage

```bash
python consumer.py --brokers localhost:8001,localhost:8002 --topic orders
```

### Distributed Deployment

When deploying across multiple machines:

```bash
# Consumer on Machine A
python consumer.py --brokers 192.168.1.100:8001,192.168.1.101:8002 --topic transactions

# Consumer on Machine B (different topic)
python consumer.py --brokers 192.168.1.100:8001,192.168.1.101:8002 --topic logs
```

### Multiple Consumers

Each consumer instance maintains independent offset tracking. To run multiple consumers:

```bash
# Consumer 1
python consumer.py --brokers localhost:8001 --topic orders

# Consumer 2 (different data directory recommended)
DATA_DIR=./data_consumer2 python consumer.py --brokers localhost:8001 --topic orders
```

## API Endpoints Consumed

### 1. GET /metadata/leader

**Purpose:** Discover current leader broker

**Response Format:**
```json
{
  "leader": {
    "broker_id": 1,
    "host": "192.168.1.100",
    "port": 8001,
    "epoch": 5
  }
}
```

### 2. GET /consume

**Purpose:** Fetch messages from a topic starting at specified offset

**Query Parameters:**
- `topic` (string): Topic name
- `offset` (integer): Starting offset for consumption

**Response Format:**
```json
{
  "messages": [
    {
      "offset": 1,
      "topic": "orders",
      "message": "Order #123"
    }
  ],
  "hwm": 5,
  "total_available": 4,
  "requested_offset": 1
}
```

## Message Format

Messages are stored and processed in the following JSON schema:

```json
{
  "offset": integer,
  "topic": string,
  "message": string | object,
  "timestamp": string (optional),
  "metadata": object (optional)
}
```
