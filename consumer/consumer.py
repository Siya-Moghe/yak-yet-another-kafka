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
    
    # Regular colors
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    
    # Background colors
    BG_RED = '\033[101m'
    BG_GREEN = '\033[102m'
    BG_YELLOW = '\033[103m'

# ================================
# Colored Logging Formatter
# ================================
class ColoredFormatter(logging.Formatter):
    """Custom formatter to add colors to log messages"""
    
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
        formatter = logging.Formatter(log_fmt, datefmt='%H:%M:%S')
        return formatter.format(record)

# Setup colored logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter())
logger.addHandler(handler)

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
        logger.info(f"{Colors.MAGENTA}Stored message locally{Colors.RESET} for topic '{Colors.BOLD}{topic}{Colors.RESET}' offset={Colors.CYAN}{message.get('offset')}{Colors.RESET}")

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
        self.first_poll = True  # Track if this is the first poll
        self.topic_exists_warned = False  # Track if we've warned about topic

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
                    logger.info(f"{Colors.YELLOW}Leader discovered{Colors.RESET} via {Colors.BLUE}{b}{Colors.RESET} → {Colors.GREEN}{self.leader}{Colors.RESET}")
                    return self.leader
            except Exception as e:
                logger.warning(f"{Colors.RED}Failed to query broker{Colors.RESET} {Colors.YELLOW}{b}{Colors.RESET}: {e}")
                continue
        
        # Could not find leader
        logger.error(f"{Colors.RED}❌ Could not discover leader from any broker!{Colors.RESET}")
        self.leader = None
        return None

    def consume_loop(self):
        print(f"\n{Colors.BOLD}{Colors.GREEN}{'='*60}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.CYAN}YAK Consumer Started{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.GREEN}{'='*60}{Colors.RESET}")
        print(f"{Colors.CYAN}Topic:{Colors.RESET} {Colors.BOLD}{self.topic}{Colors.RESET}")
        print(f"{Colors.CYAN}Brokers:{Colors.RESET} {Colors.YELLOW}{', '.join(self.brokers)}{Colors.RESET}")
        print(f"{Colors.CYAN}Starting Offset:{Colors.RESET} {Colors.MAGENTA}{self.offset}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.GREEN}{'='*60}{Colors.RESET}\n")
        
        logger.info(f"{Colors.BLUE}Starting consumer loop{Colors.RESET} for topic '{Colors.BOLD}{self.topic}{Colors.RESET}'")
        
        while True:
            # Discover leader if we don't have one
            if not self.leader:
                discovered = self.discover_leader()
                if not discovered:
                    logger.error(f"{Colors.RED}⚠ No leader available. Retrying in {SLEEP_BETWEEN_POLL}s...{Colors.RESET}")
                    time.sleep(SLEEP_BETWEEN_POLL)
                    continue

            try:
                url = f"http://{self.leader}/consume"
                params = {"topic": self.topic, "offset": self.offset}
                resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                data = resp.json()
                messages = data.get("messages", [])
                hwm = data.get("hwm", 0)

                if messages:
                    # Reset warning flag since we got messages
                    self.topic_exists_warned = False
                    self.first_poll = False
                    
                    for msg in messages:
                        # Print the actual message content in a nice format
                        print(f"\n{Colors.BG_GREEN}{Colors.BOLD} NEW MESSAGE {Colors.RESET}")
                        print(f"{Colors.CYAN}Offset:{Colors.RESET} {Colors.BOLD}{msg.get('offset')}{Colors.RESET}")
                        print(f"{Colors.CYAN}Topic:{Colors.RESET} {Colors.BOLD}{self.topic}{Colors.RESET}")
                        print(f"{Colors.CYAN}Content:{Colors.RESET} {Colors.WHITE}{msg.get('msg', msg)}{Colors.RESET}")
                        print(f"{Colors.GREEN}{'─'*50}{Colors.RESET}")
                        
                        self.storage.append(self.topic, msg)
                        self.offset = msg.get("offset", self.offset) + 1
                    
                    logger.info(f"{Colors.GREEN}Consumed {Colors.BOLD}{len(messages)}{Colors.RESET}{Colors.GREEN} messages{Colors.RESET}, next offset={Colors.CYAN}{self.offset}{Colors.RESET}, HWM={Colors.MAGENTA}{hwm}{Colors.RESET}")
                else:
                    # No messages returned
                    # Check if this is first poll and topic likely doesn't exist
                    if self.first_poll and self.offset == 0 and hwm < 0:
                        logger.warning(f"{Colors.YELLOW}⚠ Topic '{Colors.BOLD}{self.topic}{Colors.RESET}{Colors.YELLOW}' appears to be empty or does not exist{Colors.RESET} (offset=0, hwm={hwm})")
                        self.topic_exists_warned = True
                    elif self.first_poll and self.offset == 0:
                        logger.warning(f"{Colors.YELLOW}⚠ Topic '{Colors.BOLD}{self.topic}{Colors.RESET}{Colors.YELLOW}' exists but has no messages yet{Colors.RESET} (waiting...)")
                        self.topic_exists_warned = True
                    # else: quiet poll, no logging spam
                    
                    self.first_poll = False

            except requests.exceptions.RequestException as e:
                logger.warning(f"{Colors.RED}⚠ Could not reach leader{Colors.RESET} {Colors.YELLOW}{self.leader}{Colors.RESET}: {e}")
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
    
    try:
        consumer.consume_loop()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Consumer stopped gracefully{Colors.RESET}\n")


if __name__ == "__main__":
    main()