import json
import os
import threading
from blockchain.node.blockchain import Blockchain
from blockchain.node.block import Block
from shared.config import settings
from shared.logging_config import setup_logging

logger = setup_logging("storage")


class BlockchainStore:
    """
    Upravlja persistencijom blockchaina.
    Thread-safe čitanje i pisanje na disk.
    Auto-save nakon svakog novog bloka.
    """

    def __init__(self, blockchain: Blockchain, node_id: str | None = None):
        self.blockchain = blockchain
        self.node_id = node_id or settings.node_id
        self._lock = threading.Lock()
        self._path = os.path.join(settings.data_path, f"{self.node_id}.json")

    def save(self) -> bool:
        """Spremi cijeli blockchain na disk."""
        with self._lock:
            try:
                os.makedirs(os.path.dirname(self._path), exist_ok=True)
                tmp_path = self._path + ".tmp"
                with open(tmp_path, "w") as f:
                    json.dump(self.blockchain.to_dict(), f, indent=2)
                # Atomično pisanje — zamijeni tek kad je tmp kompletan
                os.replace(tmp_path, self._path)
                logger.info(f"Blockchain spremljen ({len(self.blockchain.chain)} blokova): {self._path}")
                return True
            except Exception as e:
                logger.error(f"Greška pri spremanju: {e}")
                return False

    def load(self) -> bool:
        """Učitaj blockchain s diska."""
        with self._lock:
            if not os.path.exists(self._path):
                logger.info(f"Nema spremljenog blockchaina na: {self._path}")
                return False
            try:
                with open(self._path) as f:
                    data = json.load(f)
                self.blockchain.chain = [Block.from_dict(b) for b in data]
                logger.info(f"Blockchain učitan ({len(self.blockchain.chain)} blokova): {self._path}")
                return True
            except Exception as e:
                logger.error(f"Greška pri učitavanju: {e}")
                return False

    def save_snapshot(self, label: str) -> bool:
        """Spremi snapshot blockchaina s labelom (npr. 'before_sync')."""
        with self._lock:
            try:
                snapshot_dir = os.path.join(settings.data_path, "snapshots")
                os.makedirs(snapshot_dir, exist_ok=True)
                path = os.path.join(snapshot_dir, f"{self.node_id}_{label}.json")
                with open(path, "w") as f:
                    json.dump(self.blockchain.to_dict(), f, indent=2)
                logger.info(f"Snapshot spremljen: {path}")
                return True
            except Exception as e:
                logger.error(f"Greška pri snapshottu: {e}")
                return False

    def get_storage_info(self) -> dict:
        """Vrati informacije o pohrani."""
        size = 0
        if os.path.exists(self._path):
            size = os.path.getsize(self._path)
        return {
            "path": self._path,
            "exists": os.path.exists(self._path),
            "size_bytes": size,
            "blocks": len(self.blockchain.chain),
        }