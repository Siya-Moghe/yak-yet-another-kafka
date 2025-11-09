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

## Error Handling

### Connection Errors

When a broker becomes unreachable:
1. Log warning with connection details
2. Set `self.leader = None` to force rediscovery
3. Wait for `SLEEP_BETWEEN_POLL` seconds
4. Retry with leader discovery

### Storage Errors

If local storage fails:
1. Log error with exception details
2. Do not advance offset
3. Retry on next poll iteration

### JSON Parsing Errors

Invalid JSON lines in local storage:
1. Skip malformed lines
2. Continue processing valid entries
3. Log decoding errors (silent by design)

## Performance Considerations

### Throughput

- **Polling Interval:** Adjust `SLEEP_BETWEEN_POLL` based on message frequency
- **Batch Size:** Determined by broker's HWM (High Water Mark)
- **Network Latency:** Configure `REQUEST_TIMEOUT` for WAN deployments

### Storage

- **Disk I/O:** Append-only writes optimize for sequential disk access
- **Directory Structure:** Topic-based partitioning enables parallel consumer instances
- **File Growth:** No automatic log rotation; implement external log management

### Memory

- **Message Buffering:** All fetched messages loaded into memory during processing
- **Offset Tracking:** Single integer per topic (minimal memory footprint)
- **Connection Pooling:** Uses requests library default connection pooling

## Logging

The consumer implements a colored logging system with the following levels:

| Level | Color | Use Case |
|-------|-------|----------|
| INFO | Cyan/Green | Normal operations, successful consumption |
| WARNING | Yellow | Connection issues, retry attempts |
| ERROR | Red | Critical failures, unrecoverable errors |

**Log Format:**
```
HH:MM:SS - LEVEL - Message
```

**Sample Output:**
```
14:32:01 - INFO - Leader discovered for topic 'orders' via localhost:8001 → localhost:8001
14:32:01 - INFO - Starting consumer loop for topic 'orders'
14:32:03 - INFO - Consumed 5 messages, next offset=6, HWM=10
14:32:05 - INFO - Stored message locally for topic 'orders' offset=6
```

## Best Practices

### 1. Offset Management
- Never manually modify offset files
- Use separate data directories for multiple consumer instances
- Implement backup strategy for offset storage

### 2. Network Configuration
- Ensure all broker addresses are reachable
- Configure firewalls to allow outbound HTTP connections
- Use static IPs or DNS names for broker addresses

### 3. Monitoring
- Monitor log output for warning messages
- Track consumption lag (HWM - current offset)
- Alert on repeated connection failures

### 4. Scaling
- Deploy multiple consumers for different topics
- Use separate machines for high-throughput topics
- Implement consumer groups for parallel processing (future enhancement)

## Troubleshooting

### Consumer Not Receiving Messages

**Symptoms:** No messages appearing despite successful connection

**Diagnosis:**
1. Check HWM value in logs
2. Verify current offset is less than HWM
3. Confirm messages exist on broker using `/consume?topic=X&offset=0`

**Resolution:**
- Reset offset by deleting local storage directory
- Verify broker replication is working

### Connection Timeouts

**Symptoms:** Repeated "Could not reach leader" warnings

**Diagnosis:**
1. Verify broker is running: `curl http://broker:port/metadata/leader`
2. Check network connectivity: `ping broker_host`
3. Verify firewall rules allow HTTP traffic

**Resolution:**
- Increase `REQUEST_TIMEOUT` for high-latency networks
- Check broker logs for errors
- Verify broker's advertise-host configuration

### Offset Drift

**Symptoms:** Consumer re-processes old messages after restart

**Diagnosis:**
1. Check for multiple consumer instances writing to same directory
2. Verify filesystem integrity
3. Check for manual offset file modifications

**Resolution:**
- Use unique data directories per consumer instance
- Implement file locking if running concurrent consumers
- Restore from backup if offset file is corrupted

## Dependencies

```
requests>=2.28.0
```

**Installation:**
```bash
pip install requests
```

## Future Enhancements

1. **Consumer Groups:** Coordinate multiple consumers for parallel processing
2. **Automatic Offset Commits:** Periodic offset persistence to external store
3. **Metrics Export:** Prometheus-compatible metrics endpoint
4. **Compression:** Message compression for reduced storage footprint
5. **Dead Letter Queue:** Handle messages that fail processing
6. **Exactly-Once Semantics:** Idempotent message processing with deduplication

## License

This software is part of the YAK distributed message broker system.

## Support

For issues, questions, or contributions, please contact the development team or refer to the main YAK broker documentation.