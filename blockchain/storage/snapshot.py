import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from blockchain.node.block import Block
from shared.config import settings
from shared.logging_config import setup_logging

logger = setup_logging("snapshot")


@dataclass
class Snapshot:
    node_id: str
    timestamp: str
    chain_length: int
    chain: list[dict]


class SnapshotManager:
    """
    Upravlja snapshotovima blockchaina.
    Korisno za debugging i audit trail u distribuiranom sustavu.
    """

    def __init__(self, node_id: str | None = None):
        self.node_id = node_id or settings.node_id
        self.snapshot_dir = os.path.join(settings.data_path, "snapshots")
        os.makedirs(self.snapshot_dir, exist_ok=True)

    def create(self, chain: list[dict], label: str = "") -> str:
        """Kreiraj novi snapshot, vrati putanju."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{self.node_id}_{ts}"
        if label:
            filename += f"_{label}"
        filename += ".json"

        snapshot = Snapshot(
            node_id=self.node_id,
            timestamp=ts,
            chain_length=len(chain),
            chain=chain,
        )

        path = os.path.join(self.snapshot_dir, filename)
        with open(path, "w") as f:
            json.dump({
                "node_id": snapshot.node_id,
                "timestamp": snapshot.timestamp,
                "chain_length": snapshot.chain_length,
                "chain": snapshot.chain,
            }, f, indent=2)

        logger.info(f"Snapshot kreiran: {path}")
        return path

    def list_snapshots(self) -> list[dict]:
        """Vrati listu svih snapshotova za ovaj čvor."""
        snapshots = []
        for filename in sorted(os.listdir(self.snapshot_dir)):
            if filename.startswith(self.node_id) and filename.endswith(".json"):
                path = os.path.join(self.snapshot_dir, filename)
                snapshots.append({
                    "filename": filename,
                    "path": path,
                    "size_bytes": os.path.getsize(path),
                })
        return snapshots

    def load_snapshot(self, filename: str) -> list[Block] | None:
        """Učitaj snapshot i vrati listu blokova."""
        path = os.path.join(self.snapshot_dir, filename)
        if not os.path.exists(path):
            logger.warning(f"Snapshot ne postoji: {path}")
            return None
        try:
            with open(path) as f:
                data = json.load(f)
            return [Block.from_dict(b) for b in data["chain"]]
        except Exception as e:
            logger.error(f"Greška pri učitavanju snapshota: {e}")
            return None