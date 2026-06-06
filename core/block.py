import hashlib
import json
import time
from dataclasses import dataclass, field


@dataclass
class Block:
    index: int
    votes: list[dict]
    previous_hash: str
    nonce: int = 0
    timestamp: float = field(default_factory=time.time)
    hash: str = field(default="", init=False)

    def __post_init__(self):
        self.hash = self.calculate_hash()

    def calculate_hash(self) -> str:
        block_data = json.dumps({
            "index": self.index,
            "timestamp": self.timestamp,
            "votes": self.votes,
            "previous_hash": self.previous_hash,
            "nonce": self.nonce,
        }, sort_keys=True)
        return hashlib.sha256(block_data.encode()).hexdigest()

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "timestamp": self.timestamp,
            "votes": self.votes,
            "previous_hash": self.previous_hash,
            "nonce": self.nonce,
            "hash": self.hash,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Block":
        block = cls(
            index=data["index"],
            votes=data["votes"],
            previous_hash=data["previous_hash"],
            nonce=data["nonce"],
        )
        block.timestamp = data["timestamp"]
        block.hash = data["hash"]
        return block