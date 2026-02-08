# YAK — Yet Another Kafka (Big Data Course Project)

## Components
### Leader Broker
- Accepts writes
- Assigns offsets
- Replicates messages to followers

### Follower Brokers
- Replicate leader data
- Automatically catch up if they fall behind

### Producer
- Discovers leader
- Handles redirects & retries

### Consumer
- Polls leader
- Tracks offsets locally

### Redis
- Leader lease
- Epoch tracking
- Offsets & HWMs
- Broker heartbeats


## Project Structure
```
.
├── producer/
│   └── producer.py
├── consumer/
│   └── consumer.py
├── leader_broker/
│   ├── leader.py
│   ├── lease_manager.py
│   ├── storage.py
│   └── config.py
├── follower_broker/
│   ├── follower.py
│   ├── lease_manager.py
│   ├── storage.py
│   └── config.py
├── data/
│   └── broker-*/topic/partition-0/messages.log

```


## Requirements
- Python 3.9+
- Redis 6+
- Packages:
```
pip install fastapi uvicorn redis requests psutil
```

## Steps To Run

#### Start Redis
```
redis-server
```

#### Start Brokers
You can start multiple brokers; leadership is automatic
```
python leader.py --broker-id 1 --port 8001 --redis-host localhost

python follower.py --broker-id 2 --port 8002 --redis-host localhost
```
Only one broker becomes leader at a time. Others act as followers.

#### Register a Topic
```
curl -X POST http://localhost:8001/register_topic \
  -H "Content-Type: application/json" \
  -d '{"topic":"test"}'
```

#### Start a Producer
Interactive Mode
```
python producer.py  --brokers localhost:8001,localhost:8002,localhost:8003 --topic test
```
Batch Mode (File or CSV)
```
python producer.py --brokers localhost:8001,localhost:8002 --topic test --file messages.txt
```

#### Start a Consumer
```
python consumer.py --brokers localhost:8001,localhost:8002 --topic test
```
The consumer discovers the leader, tracks offsets, stores messages locally under ./data/

## Acknowledgements
Inspired by:
- Apache Kafka
- Raft-style leader leasing
- Distributed systems coursework
