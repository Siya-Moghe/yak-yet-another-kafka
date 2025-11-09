import requests
import os
import json
import argparse
import time
import logging

# ================================
# ANSI Color Codes
# ================================
class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'

# ================================
# Colored Logging Formatter
# ================================
class ColoredFormatter(logging.Formatter):
    FORMATS = {
        logging.INFO: Colors.CYAN + "%(asctime)s" + Colors.RESET + " - " + 
                      Colors.GREEN + "%(levelname)s" + Colors.RESET + " - %(message)s",
        logging.WARNING: Colors.CYAN + "%(asctime)s" + Colors.RESET + " - " + 
                         Colors.YELLOW + "%(levelname)s" + Colors.RESET + " - " + 
                         Colors.YELLOW + "%(message)s" + Colors.RESET,
        logging.ERROR: Colors.CYAN + "%(asctime)s" + Colors.RESET + " - " + 
                       Colors.RED + "%(levelname)s" + Colors.RESET + " - " + 
                       Colors.RED + "%(message)s" + Colors.RESET,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno, self.FORMATS[logging.INFO])
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

# Setup colored logging
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])

# ================================
# Config
# ================================
REQUEST_TIMEOUT = 5
SLEEP_BETWEEN_POLL = 2


# ================================
# Local Storage
# ================================
class ConsumerStorage:
    """Stores messages locally per topic."""
    def __init__(self, base_dir="./data"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_partition_dir(self, topic):
        topic_dir = os.path.join(self.base_dir, topic, "partition-0")
        os.makedirs(topic_dir, exist_ok=True)
        return topic_dir

    def append(self, topic, message):
        partition_dir = self._get_partition_dir(topic)
        file_path = os.path.join(partition_dir, "messages.log")
        with open(file_path, "a") as f:
            f.write(json.dumps(message) + "\n")
        logging.info(f"{Colors.MAGENTA}Stored message locally{Colors.RESET} for topic '{topic}' offset={message.get('offset')}")

    def read_all(self, topic):
        messages = []
        partition_dir = self._get_partition_dir(topic)
        file_path = os.path.join(partition_dir, "messages.log")
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                for line in f:
                    try:
                        messages.append(json.loads(line.strip()))
                    except json.JSONDecodeError:
                        continue
        return messages


# ================================
# Consumer Logic
# ================================
class TopicConsumer:
    """Consumes messages from a single hardcoded topic."""
    def __init__(self, brokers, topic, storage):
        self.brokers = brokers
        self.topic = topic
        self.storage = storage
        self.offset = self._load_offset()
        self.leader = None

    def _load_offset(self):
        messages = self.storage.read_all(self.topic)
        if messages:
            return max(m.get("offset", 0) for m in messages) + 1
        return 0

    def discover_leader(self):
        for b in self.brokers:
            try:
                resp = requests.get(f"http://{b}/metadata/leader", timeout=REQUEST_TIMEOUT)
                leader_info = resp.json().get("leader", {})
                if leader_info.get("host") and leader_info.get("port"):
                    self.leader = f"{leader_info['host']}:{leader_info['port']}"
                    logging.info(f"{Colors.YELLOW}Leader discovered{Colors.RESET} for topic '{self.topic}' via {b} -> {self.leader}")
                    return self.leader
            except Exception:
                continue
        self.leader = self.brokers[0]
        return self.leader

    def consume_loop(self):
        logging.info(f"Starting consumer for topic '{self.topic}'")
        while True:
            if not self.leader:
                self.discover_leader()

            try:
                url = f"http://{self.leader}/consume"
                params = {"topic": self.topic, "offset": self.offset}
                resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                data = resp.json()
                
                # Check if topic doesn't exist
                if data.get("status") == "error":
                    logging.error(f"{Colors.RED}Error from broker:{Colors.RESET} {data.get('message')}")
                    logging.error(f"{Colors.RED}Topic '{self.topic}' does not exist. Exiting.{Colors.RESET}")
                    return  # Exit the loop
                
                messages = data.get("messages", [])
                hwm = data.get("hwm", 0)

                if messages:
                    for msg in messages:
                        self.storage.append(self.topic, msg)
                        self.offset = msg.get("offset", self.offset) + 1
                    logging.info(f"{Colors.GREEN}Consumed {len(messages)} messages{Colors.RESET}, next offset={self.offset}")

            except requests.exceptions.RequestException as e:
                logging.warning(f"Could not reach leader {self.leader} for topic '{self.topic}': {e}")
                self.leader = None  # force rediscovery next loop

            time.sleep(SLEEP_BETWEEN_POLL)


# ================================
# CLI
# ================================
def main():
    parser = argparse.ArgumentParser(description="YAK Consumer Client")
    parser.add_argument("--brokers", required=True, help="Comma-separated list of known brokers (host:port)")
    parser.add_argument("--topic", required=True, help="Hardcoded topic to consume from")
    args = parser.parse_args()

    brokers = [b.strip() for b in args.brokers.split(",") if b.strip()]
    storage = ConsumerStorage()
    consumer = TopicConsumer(brokers, args.topic, storage)
    consumer.consume_loop()


if __name__ == "__main__":
    main()