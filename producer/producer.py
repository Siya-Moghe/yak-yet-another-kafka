import requests
import time
import argparse
import os
import logging

REQUEST_TIMEOUT = 8
MAX_RETRIES = 5
SLEEP_BETWEEN_RETRIES = 2

logger = logging.getLogger("producer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class ColoredFormatter(logging.Formatter):
    COLORS = {
        'INFO': '\033[94m',      # Blue
        'TRY': '\033[95m',       # Magenta
        'OK': '\033[92m',        # Green
        'WARN': '\033[93m',      # Yellow
        'ERROR': '\033[91m',     # Red
        'FAIL': '\033[91m',      # Red
        'DEBUG': '\033[96m',     # Cyan
        'BATCH': '\033[96m',     # Cyan
        'DISCOVERY': '\033[94m', # Blue
    }
    RESET = '\033[0m'

    def format(self, record):
        msg = record.msg
        for key, color in self.COLORS.items():
            if msg.startswith(f"[{key}]") or msg.startswith(f"[{key} "):
                record.msg = f"{color}{msg}{self.RESET}"
                break
        return super().format(record)

for handler in logging.getLogger().handlers:
    handler.setFormatter(ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s"))

def discover_leader(brokers):
    for b in brokers:
        try:
            r = requests.get(f"http://{b}/metadata/leader", timeout=REQUEST_TIMEOUT)
            info = r.json().get("leader", {}) or {}
            host, port = info.get("host"), info.get("port")
            if host and port:
                leader = f"{host}:{port}"
                logger.info(f"[DISCOVERY] Leader discovered via {b} → {leader}")
                return leader
        except Exception:
            continue
    return None

def produce_message(brokers, topic, message):
    #leader = brokers[0]
    leader = discover_leader(brokers) or brokers[0]
    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(f"[TRY {attempt}] Sending message to {leader} ...")
        try:
            url = f"http://{leader}/produce"
            data = {"topic": topic, "msg": message}
            resp = requests.post(url, json=data, timeout=REQUEST_TIMEOUT)
            rjson = resp.json()
            if rjson.get("status") == "ok":
                logger.info(f"[OK] Message delivered successfully to leader {leader}")
                return True
            elif rjson.get("status") == "redirect":
                leader_info = rjson.get("leader", {}) or {}
                host, port = leader_info.get("host"), leader_info.get("port")
                if host and port:
                    leader = f"{host}:{port}"
                    logger.info(f"[INFO] Redirected to new leader {leader}")
                    continue
                else:
                    logger.warning("[WARN] Redirected but no valid leader info; rediscovering...")
                    new_leader = discover_leader(brokers)
                    if new_leader:
                        leader = new_leader
                        continue
            else:
                logger.warning(f"[WARN] Unexpected broker response: {rjson}")
        except requests.exceptions.ConnectTimeout:
            logger.error(f"[ERROR] Could not reach {leader} (connection timeout). Retrying discovery...")
            leader = discover_leader(brokers) or leader
            time.sleep(SLEEP_BETWEEN_RETRIES)
            continue
        except requests.exceptions.ReadTimeout:
            logger.warning(f"[WARN] Leader {leader} took too long to respond — assuming replication in progress.")
            return True
        except requests.exceptions.ConnectionError as e:
            logger.error(f"[ERROR] Connection to {leader} failed: {e}")
            leader = discover_leader(brokers) or leader
            time.sleep(SLEEP_BETWEEN_RETRIES)
            continue
        except Exception as e:
            logger.error(f"[ERROR] Unexpected error: {e}")
            time.sleep(SLEEP_BETWEEN_RETRIES)
            continue
    logger.error("[FAIL] Could not deliver message after retries.")
    return False

def send_file(brokers, topic, file_path):
    if not os.path.exists(file_path):
        logger.error(f"[ERROR] File not found: {file_path}")
        return
    ext = os.path.splitext(file_path)[1].lower()
    lines = []
    if ext == ".csv":
        import csv
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if row:
                    lines.append(" | ".join(row))
    else:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
    logger.info(f"[INFO] Sending {len(lines)} messages from file '{file_path}'")
    success_count = 0
    for i, line in enumerate(lines, start=1):
        logger.info(f"[BATCH] {i}/{len(lines)} → {line}")
        if produce_message(brokers, topic, line):
            success_count += 1
        else:
            logger.warning(f"[WARN] Failed to send line {i}")
    logger.info(f"\n[INFO] Sent {success_count}/{len(lines)} messages successfully.")

def main():
    parser = argparse.ArgumentParser(description="YAK Producer Client (Interactive + Batch Support)")
    parser.add_argument("--brokers", required=True,
                        help="Comma-separated list of known brokers (host:port)")
    parser.add_argument("--topic", required=True,
                        help="Topic name to produce to")
    parser.add_argument("--file", help="Path to text or CSV file for batch sending (optional)")
    args = parser.parse_args()
    brokers = [b.strip() for b in args.brokers.split(",") if b.strip()]
    logger.info(f"Known brokers: {brokers}")
    if args.file:
        send_file(brokers, args.topic, args.file)
    else:
        logger.info("Type messages to send (empty line to exit):")
        while True:
            msg = input("> ").strip()
            if not msg:
                logger.info("Exiting producer.")
                break
            produce_message(brokers, args.topic, msg)

if __name__ == "__main__":
    main()

