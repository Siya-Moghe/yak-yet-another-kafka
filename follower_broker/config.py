# config.py
import os

DATA_DIR = "./data"
LEASE_KEY = "yak:leader_lease"
EPOCH_KEY = "yak:epoch"
LEASE_TTL = 10  # seconds
RENEW_INTERVAL = 5  # seconds

# Helper paths
def get_log_path(broker_id: int, topic: str = "default", partition: int = 0) -> str:
    path = os.path.join(DATA_DIR, f"broker-{broker_id}", topic, f"partition-{partition}", "messages.log")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path
