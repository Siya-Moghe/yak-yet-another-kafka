import json
import logging
from config import get_log_path

logger = logging.getLogger(__name__)

class MessageStorage:
    def __init__(self, broker_id, topic="default", partition=0):
        self.broker_id = broker_id          # store broker_id here
        self.topic = topic
        self.partition = partition
        self.path = get_log_path(broker_id, topic, partition)
        open(self.path, "a").close()

    def append(self, message, topic=None):
        topic = topic or self.topic
        path = get_log_path(broker_id=self.broker_id, topic=topic, partition=self.partition)
        with open(path, "a") as f:
            f.write(json.dumps(message) + "\n")
        logger.info(f"📝 Stored message at {path}")

    def read_all(self, topic=None):
        topic = topic or self.topic
        path = get_log_path(broker_id=self.broker_id, topic=topic, partition=self.partition)
        with open(path, "r") as f:
            return [json.loads(line.strip()) for line in f if line.strip()]
