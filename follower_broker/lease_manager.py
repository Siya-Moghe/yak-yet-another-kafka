# lease_manager.py
import json
import time
import redis
import logging

# ===== Colored logging for lease_manager =====
class LeaseColoredFormatter(logging.Formatter):
    COLORS = {
        'INFO': '\033[97m',      # White
        'SUCCESS': '\033[92m',   # Bright Green
        'RENEW': '\033[96m',     # Cyan
        'WARNING': '\033[93m',   # Yellow
        'ERROR': '\033[91m',     # Red
    }
    RESET = '\033[0m'

    def format(self, record):
        msg = str(record.msg)
        for tag, color in self.COLORS.items():
            if msg.startswith(f"[{tag}]"):
                record.msg = f"{color}{msg}{self.RESET}"
                break
        return super().format(record)

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(LeaseColoredFormatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class LeaseManager:
    def __init__(self, redis_host, redis_port, broker_id, broker_host, broker_port, lease_key, epoch_key, ttl=10):
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.broker_id = broker_id
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.lease_key = lease_key
        self.epoch_key = epoch_key
        self.ttl = ttl

    def _get_current_lease(self):
        lease = self.redis.get(self.lease_key)
        return json.loads(lease) if lease else None

    def try_acquire_lease(self):
        lease_data = {
            "broker_id": self.broker_id,
            "epoch": int(self.redis.get(self.epoch_key) or 0) + 1,
            "host": self.broker_host,
            "port": self.broker_port,
        }

        acquired = self.redis.set(self.lease_key, json.dumps(lease_data), nx=True, ex=self.ttl)
        if acquired:
            self.redis.set(self.epoch_key, lease_data["epoch"])
            logger.info(f"[SUCCESS] Broker {self.broker_id} acquired leadership. Epoch={lease_data['epoch']}")
            return True
        return False

    def renew_lease(self):
        lease = self._get_current_lease()
        if lease and lease["broker_id"] == self.broker_id:
            self.redis.expire(self.lease_key, self.ttl)
            logger.info(f"[RENEW] Lease renewed by broker {self.broker_id}")
            return True
        return False

    def current_leader(self):
        return self._get_current_lease()

    def is_leader(self):
        lease = self._get_current_lease()
        return lease and lease["broker_id"] == self.broker_id
