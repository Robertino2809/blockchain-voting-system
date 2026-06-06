import json
import os
from blockchain.node.block import Block
from shared.config import settings
from shared.logging_config import setup_logging

logger = setup_logging("blockchain")


class Blockchain:
    def __init__(self):
        self.chain: list[Block] = []
        self.pending_votes: list[dict] = []
        self._create_genesis_block()

    def _create_genesis_block(self):
        genesis = Block(index=0, votes=[], previous_hash="0" * 64, term=0)
        genesis.timestamp = 0.0  # Fiksni timestamp — isti hash na svim čvorovima
        genesis.hash = genesis.calculate_hash()
        self.chain.append(genesis)

    @property
    def last_block(self) -> Block:
        return self.chain[-1]

    def add_pending_vote(self, vote: dict) -> bool:
        """Dodaj glas u pending pool. Vraća False ako je duplikat."""
        voter_id = vote.get("voter_id")
        if self.has_voted(voter_id):
            return False
        for v in self.pending_votes:
            if v["voter_id"] == voter_id:
                return False
        self.pending_votes.append(vote)
        return True

    def has_voted(self, voter_id: str) -> bool:
        for block in self.chain[1:]:
            for vote in block.votes:
                if vote["voter_id"] == voter_id:
                    return True
        return False

    def mine_block(self, term: int) -> Block | None:
        """Rudari novi blok — poziva samo leader."""
        if not self.pending_votes:
            return None

        block = Block(
            index=len(self.chain),
            votes=self.pending_votes.copy(),
            previous_hash=self.last_block.hash,
            term=term,
        )

        target = "0" * settings.pow_difficulty
        while not block.hash.startswith(target):
            block.nonce += 1
            block.hash = block.calculate_hash()

        self.chain.append(block)
        self.pending_votes = []
        logger.info(f"Blok #{block.index} iskopan | term={term} | nonce={block.nonce} | hash={block.hash[:12]}...")
        return block

    def append_block(self, block_data: dict) -> bool:
        """Follower prima gotovi blok od leadera."""
        block = Block.from_dict(block_data)

        if block.previous_hash != self.last_block.hash:
            logger.warning(f"Blok #{block.index} odbijen — neispravan previous_hash")
            return False

        if block.hash != block.calculate_hash():
            logger.warning(f"Blok #{block.index} odbijen — neispravan hash")
            return False

        if not block.hash.startswith("0" * settings.pow_difficulty):
            logger.warning(f"Blok #{block.index} odbijen — PoW nije zadovoljen")
            return False

        # Ukloni glasove iz pending koji su ušli u blok
        voted_ids = {v["voter_id"] for v in block.votes}
        self.pending_votes = [v for v in self.pending_votes if v["voter_id"] not in voted_ids]

        self.chain.append(block)
        logger.info(f"Blok #{block.index} prihvaćen od leadera | term={block.term}")
        return True

    def is_valid(self) -> bool:
        target = "0" * settings.pow_difficulty
        for i in range(1, len(self.chain)):
            curr = self.chain[i]
            prev = self.chain[i - 1]
            if curr.hash != curr.calculate_hash():
                return False
            if curr.previous_hash != prev.hash:
                return False
            if not curr.hash.startswith(target):
                return False
        return True

    def get_results(self) -> dict[str, int]:
        results: dict[str, int] = {}
        for block in self.chain[1:]:
            for vote in block.votes:
                c = vote["candidate"]
                results[c] = results.get(c, 0) + 1
        return results

    def to_dict(self) -> list[dict]:
        return [b.to_dict() for b in self.chain]

    def save(self):
        path = os.path.join(settings.data_path, f"{settings.node_id}.json")
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info(f"Blockchain spremljen: {path}")

    def load(self) -> bool:
        path = os.path.join(settings.data_path, f"{settings.node_id}.json")
        if not os.path.exists(path):
            return False
        try:
            with open(path) as f:
                data = json.load(f)
            self.chain = [Block.from_dict(b) for b in data]
            logger.info(f"Blockchain učitan: {len(self.chain)} blokova")
            return True
        except Exception as e:
            logger.error(f"Greška pri učitavanju blockchaina: {e}")
            return False