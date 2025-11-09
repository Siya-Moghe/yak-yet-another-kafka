import argparse
import logging
import threading
import time
import requests
import socket
import psutil
import os
import json
from fastapi import FastAPI, Request
import uvicorn
import redis

from config import LEASE_KEY, EPOCH_KEY, LEASE_TTL, RENEW_INTERVAL
from lease_manager import LeaseManager

app = FastAPI()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ===== Colored logging =====
class CustomLogFormatter(logging.Formatter):
    RESET = "\033[0m"
    COLORS = {
        "[INFO]": "\033[94m",
        "[SUCCESS]": "\033[92m",
        "[WARN]": "\033[33m",
        "[ERROR]": "\033[91m",
        "[SYNC]": "\033[96m",
        "[ACK]": "\033[38;5;172m",
        "[REPLICATED]": "\033[95m",
        "[REDIRECT]": "\033[35m",
        "[NEW]": "\033[38;5;220m",
        "[LEASE]": "\033[90m",
        "[REGISTERED]": "\033[38;5;208m"
    }

    def format(self, record):
        msg = super().format(record)
        for key, color in self.COLORS.items():
            if key in record.getMessage():
                return f"{color}{msg}{self.RESET}"
        if record.levelno == logging.WARNING:
            return f"\033[93m{msg}{self.RESET}"
        elif record.levelno == logging.ERROR:
            return f"\033[91m{msg}{self.RESET}"
        return msg


formatter = CustomLogFormatter("%(asctime)s - %(levelname)s - %(message)s")
for handler in logging.getLogger().handlers:
    handler.setFormatter(formatter)


# ===== MessageStorage =====
class MessageStorage:
    def __init__(self, broker_id, base_dir="./data"):
        self.broker_id = broker_id
        self.base_dir = os.path.join(base_dir, f"broker-{broker_id}")
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_partition_dir(self, topic):
        topic_dir = os.path.join(self.base_dir, topic, "partition-0")
        os.makedirs(topic_dir, exist_ok=True)
        return topic_dir

    def append(self, message, topic):
        partition_dir = self._get_partition_dir(topic)
        file_path = os.path.join(partition_dir, "messages.log")
        with open(file_path, "a") as f:
            f.write(json.dumps(message) + "\n")
        logger.info(f"[ACK] Stored message at {file_path}")

    def read_all(self, topic):
        messages = []
        partition_dir = os.path.join(self.base_dir, topic, "partition-0")
        file_path = os.path.join(partition_dir, "messages.log")
        if not os.path.exists(file_path):
            return messages
        with open(file_path, "r") as f:
            for line in f:
                try:
                    messages.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    continue
        return messages

    def list_topics(self):
        topics = []
        if os.path.exists(self.base_dir):
            for name in os.listdir(self.base_dir):
                topic_path = os.path.join(self.base_dir, name)
                if os.path.isdir(topic_path):
                    topics.append(name)
        return topics


# ===== BrokerNode =====
class BrokerNode:
    def __init__(self, broker_id, port, redis_host, redis_port, advertise_host=None):
        self.broker_id = broker_id
        self.port = port
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.host = advertise_host or self._detect_zerotier_ip() or self._detect_local_ip()

        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.lease_manager = LeaseManager(
            redis_host, redis_port, broker_id, self.host, port,
            LEASE_KEY, EPOCH_KEY, LEASE_TTL
        )

        self.storage = MessageStorage(broker_id)
        self.is_leader = False
        self.register_broker()

    # ===== IP Detection =====
    def _detect_zerotier_ip(self):
        try:
            for iface_name, iface_addrs in psutil.net_if_addrs().items():
                if iface_name.startswith("zt"):
                    for addr in iface_addrs:
                        if addr.family == socket.AF_INET:
                            logger.info(f"[INFO] Detected ZeroTier IP: {addr.address}")
                            return addr.address
        except Exception as e:
            logger.warning(f"[WARNING] Failed to detect ZeroTier IP: {e}")
        return None

    def _detect_local_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        finally:
            s.close()
        return ip

    # ===== Redis registration =====
    def register_broker(self):
        key = f"yak:broker:{self.broker_id}"
        self.redis.hset(key, mapping={
            "broker_id": self.broker_id,
            "host": self.host,
            "port": self.port,
            "last_seen": int(time.time())
        })
        self.redis.expire(key, LEASE_TTL * 3)
        logger.info(f"[REGISTERED] Registered broker {self.broker_id} in Redis: {self.host}:{self.port}")

    def update_heartbeat(self):
        key = f"yak:broker:{self.broker_id}"
        self.redis.hset(key, "last_seen", int(time.time()))
        self.redis.expire(key, LEASE_TTL * 3)

    # ===== Leader / Follower detection =====
    def get_leader(self):
        leader_info = self.lease_manager.current_leader()
        if not leader_info:
            return None
        return {
            "broker_id": leader_info.get("broker_id"),
            "host": leader_info.get("host"),
            "port": leader_info.get("port")
        }

    def get_followers(self):
        followers = []
        for key in self.redis.keys("yak:broker:*"):
            data = self.redis.hgetall(key)
            if not data or int(data.get("broker_id")) == self.broker_id:
                continue
            last_seen = int(data.get("last_seen", 0))
            if time.time() - last_seen > LEASE_TTL * 2:
                continue
            followers.append({"host": data["host"], "port": int(data["port"])})
        return followers

    # ===== Catch-up =====
    def catch_up_from_leader(self):
        leader_info = self.get_leader()
        if not leader_info or leader_info.get("broker_id") == self.broker_id:
            return

        known_topics = self.storage.list_topics()
        try:
            resp = requests.get(
                f"http://{leader_info['host']}:{leader_info['port']}/metadata/topics",
                timeout=3
            )
            if resp.status_code == 200:
                leader_topics = resp.json().get("topics", [])
                known_topics = list(set(known_topics + leader_topics))
        except Exception as e:
            logger.warning(f"[WARNING] Could not fetch topics from leader: {e}")

        for topic in known_topics:
            follower_hwm = self.redis.get(f"yak:follower_hwm:{self.broker_id}:{topic}")
            follower_hwm = int(follower_hwm) if follower_hwm else 0
            try:
                resp = requests.post(
                    f"http://{leader_info['host']}:{leader_info['port']}/internal/catchup",
                    json={"topic": topic, "from_offset": follower_hwm},
                    timeout=10
                )
                if resp.status_code == 200:
                    data = resp.json()
                    messages = data.get("messages", [])
                    if messages:
                        logger.info(f"[SYNC] Catching up {len(messages)} messages for topic '{topic}'")
                        for msg in messages:
                            existing = {m.get("offset") for m in self.storage.read_all(topic)}
                            if msg["offset"] not in existing:
                                self.storage.append(msg, topic=topic)
                                self.redis.set(f"yak:follower_hwm:{self.broker_id}:{topic}", msg["offset"])
                        logger.info(f"[SUCCESS] Caught up topic '{topic}' to offset {messages[-1]['offset']}")
                    else:
                        logger.info(f"[INFO] Topic '{topic}' already up-to-date")
            except Exception as e:
                logger.warning(f"[WARNING] Catch-up failed for topic '{topic}': {e}")

    # ===== Start broker =====
    def start(self):
        threading.Thread(target=self._lease_monitor, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        uvicorn.run(app, host="0.0.0.0", port=self.port, workers=1)

    def _lease_monitor(self):
        while True:
            if not self.is_leader:
                acquired = self.lease_manager.try_acquire_lease()
                if acquired:
                    self.is_leader = True
                    logger.info(f"[SUCCESS] Broker {self.broker_id} became leader ({self.host}:{self.port})")
            else:
                if not self.lease_manager.renew_lease():
                    logger.warning("[WARNING] Lease renewal failed. Lost leadership.")
                    self.is_leader = False
            if not self.is_leader:
                self.catch_up_from_leader()
            time.sleep(RENEW_INTERVAL)

    def _heartbeat_loop(self):
        while True:
            self.update_heartbeat()
            time.sleep(LEASE_TTL // 2)

    def update_hwm(self, topic, offset):
        self.redis.set(f"yak:hwm:{topic}", offset)

    def get_hwm(self, topic):
        val = self.redis.get(f"yak:hwm:{topic}")
        return int(val) if val else 0

    def get_next_offset(self, topic):
        return self.redis.incr(f"yak:offset:{topic}")


broker_node = None

# ===== API =====

@app.post("/register_topic")
async def register_topic(request: Request):
    data = await request.json()
    topic = data.get("topic")
    if not topic:
        return {"status": "error", "message": "Missing topic name"}

    # ---- Only leader can register topics ----
    if not broker_node.is_leader:
        leader_info = broker_node.get_leader()
        logger.info(f"[REDIRECT] Only leader can create topics. Redirecting to {leader_info}")
        return {
            "status": "redirect",
            "message": "Only leader can register topics",
            "leader": leader_info
        }

    existing_topics = broker_node.storage.list_topics()
    if topic in existing_topics:
        return {"status": "exists", "topic": topic}

    broker_node.storage._get_partition_dir(topic)
    broker_node.redis.set(f"yak:hwm:{topic}", 0)
    broker_node.redis.set(f"yak:offset:{topic}", 0)
    logger.info(f"[NEW] Created new topic '{topic}' by leader broker {broker_node.broker_id}")
    return {"status": "ok", "topic": topic}


@app.post("/produce")
async def produce(request: Request):
    data = await request.json()
    topic = data.get("topic", "default")

    # ---- Disallow topic auto-creation ----
    if topic not in broker_node.storage.list_topics():
        logger.warning(f"[ERROR] Producer tried producing to unknown topic '{topic}'")
        return {"status": "error", "message": f"Topic '{topic}' not registered"}

    if not broker_node.is_leader:
        leader_info = broker_node.get_leader()
        logger.info(f"[REDIRECT] Not the Leader. Redirecting to {leader_info}")
        return {"status": "redirect", "leader": leader_info, "message": "Not the Leader"}

    offset = broker_node.get_next_offset(topic)
    message_with_offset = {"offset": offset, **data}
    broker_node.storage.append(message_with_offset, topic=topic)
    logger.info(f"[ACK] Message offset={offset} stored locally on leader")

    followers = broker_node.get_followers()
    if not followers:
        logger.warning(f"[WARNING] No followers available for replication.")
    else:
        for f in followers:
            try:
                resp = requests.post(
                    f"http://{f['host']}:{f['port']}/internal/replicate",
                    json={"topic": topic, "message": message_with_offset},
                    timeout=5,
                )
                if resp.status_code == 200:
                    logger.info(f"[REPLICATED] Message offset={offset} replicated to {f['host']}:{f['port']}")
            except Exception as e:
                logger.warning(f"[WARNING] Replication exception to {f['host']}:{f['port']}: {e}")

    broker_node.update_hwm(topic, offset)
    return {
        "status": "ok",
        "topic": topic,
        "offset": offset,
        "hwm": broker_node.get_hwm(topic)
    }


@app.get("/consume")
async def consume(topic: str = "default", offset: int = 0):
    # ---- Consumers cannot create new topics ----
    if topic not in broker_node.storage.list_topics():
        logger.warning(f"[ERROR] Consumer tried consuming from unknown topic '{topic}'")
        return {"status": "error", "message": f"Topic '{topic}' does not exist"}

    hwm = broker_node.get_hwm(topic)
    all_messages = broker_node.storage.read_all(topic)
    committed_messages = [m for m in all_messages if offset <= m.get("offset", 0) <= hwm]
    return {"messages": committed_messages, "hwm": hwm, "total_available": len(committed_messages)}


@app.post("/internal/replicate")
async def internal_replicate(request: Request):
    data = await request.json()
    topic = data.get("topic")
    message = data.get("message")

    if not message:
        return {"error": "Missing message content"}

    # ---- Follower cannot auto-create topics ----
    if topic not in broker_node.storage.list_topics():
        logger.warning(f"[ERROR] Replication received for unknown topic '{topic}' — ignoring")
        return {"error": f"Unknown topic '{topic}'"}

    offset = message.get("offset")
    existing_offsets = {m.get("offset") for m in broker_node.storage.read_all(topic)}
    if offset not in existing_offsets:
        broker_node.storage.append(message, topic=topic)
        logger.info(f"[REPLICATED] Message offset={offset} for topic '{topic}' to follower {broker_node.broker_id}")

    broker_node.redis.set(f"yak:follower_hwm:{broker_node.broker_id}:{topic}", offset)
    return {"status": "replicated", "topic": topic, "offset": offset}


@app.post("/internal/catchup")
async def internal_catchup(request: Request):
    data = await request.json()
    topic = data.get("topic")
    from_offset = data.get("from_offset", 0)

    if topic not in broker_node.storage.list_topics():
        return {"status": "error", "message": f"Unknown topic '{topic}'"}

    all_messages = broker_node.storage.read_all(topic)
    messages_to_send = [m for m in all_messages if m.get("offset", 0) > from_offset]
    logger.info(f"[SYNC] Sending {len(messages_to_send)} messages for catch-up on topic '{topic}'")
    return {"status": "ok", "topic": topic, "messages": messages_to_send}


@app.get("/metadata/leader")
async def metadata_leader():
    leader_info = broker_node.lease_manager.current_leader()
    if leader_info and leader_info.get("broker_id") == broker_node.broker_id:
        leader_info["host"] = broker_node.host
    return {"leader": leader_info}


@app.get("/metadata/topics")
async def metadata_topics():
    topics = broker_node.storage.list_topics()
    return {"topics": topics, "count": len(topics)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-id", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--redis-host", type=str, required=True)
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--advertise-host", type=str)
    args = parser.parse_args()

    global broker_node
    broker_node = BrokerNode(
        args.broker_id, args.port,
        args.redis_host, args.redis_port,
        advertise_host=args.advertise_host
    )
    broker_node.start()


if __name__ == "__main__":
    main()