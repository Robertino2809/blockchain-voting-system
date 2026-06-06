from core.block import Block
import json
import os

class Blockchain:
    def __init__(self, pow_difficulty: int = 3):
        self.pow_difficulty = pow_difficulty
        self.chain: list[Block] = []
        self.pending_votes: list[dict] = []
        self._create_genesis_block()

    def _create_genesis_block(self):
        """Prvi blok u lancu — nema prethodnog hasha."""
        genesis = Block(index=0, votes=[], previous_hash="0" * 64)
        self.chain.append(genesis)

    @property
    def last_block(self) -> Block:
        return self.chain[-1]

    # ── Glasanje ────────────────────────────────────────────

    def add_vote(self, voter_id: str, candidate: str) -> dict:
        """Dodaj glas u pending pool nakon validacije."""
        if self.has_voted(voter_id):
            return {"success": False, "error": f"Glasač '{voter_id}' je već glasao."}

        # Provjera duplikata u pending poolu
        for v in self.pending_votes:
            if v["voter_id"] == voter_id:
                return {"success": False, "error": f"Glasač '{voter_id}' već ima pending glas."}

        vote = {"voter_id": voter_id, "candidate": candidate}
        self.pending_votes.append(vote)
        return {"success": True, "vote": vote}

    def has_voted(self, voter_id: str) -> bool:
        """Provjeri je li glasač već glasao u potvrđenim blokovima."""
        for block in self.chain[1:]:  # preskačemo genesis
            for vote in block.votes:
                if vote["voter_id"] == voter_id:
                    return True
        return False

    # ── Rudarenje ───────────────────────────────────────────

    def mine_block(self) -> Block | None:
        """Rudari novi blok s pending glasovima (Proof-of-Work)."""
        if not self.pending_votes:
            return None

        block = Block(
            index=len(self.chain),
            votes=self.pending_votes.copy(),
            previous_hash=self.last_block.hash,
        )

        target = "0" * self.pow_difficulty
        while not block.hash.startswith(target):
            block.nonce += 1
            block.hash = block.calculate_hash()

        self.chain.append(block)
        self.pending_votes = []
        return block

    # ── Validacija ──────────────────────────────────────────

    def is_valid(self, chain: list[Block] | None = None) -> bool:
        """Provjeri integritet cijelog lanca."""
        chain = chain or self.chain
        target = "0" * self.pow_difficulty

        for i in range(1, len(chain)):
            current = chain[i]
            previous = chain[i - 1]

            if current.hash != current.calculate_hash():
                return False

            if current.previous_hash != previous.hash:
                return False

            if not current.hash.startswith(target):
                return False

        return True

    # ── Rezultati ───────────────────────────────────────────

    def get_results(self) -> dict[str, int]:
        """Prebroji glasove iz svih potvrđenih blokova."""
        results: dict[str, int] = {}
        for block in self.chain[1:]:
            for vote in block.votes:
                candidate = vote["candidate"]
                results[candidate] = results.get(candidate, 0) + 1
        return results

    # ── Serijalizacija ──────────────────────────────────────

    def to_dict(self) -> list[dict]:
        return [block.to_dict() for block in self.chain]

    def replace_chain(self, new_chain_data: list[dict]) -> bool:
        """Zamijeni lanac dužim valjanim lancem (longest chain rule)."""
        new_chain = [Block.from_dict(b) for b in new_chain_data]

        if len(new_chain) <= len(self.chain):
            return False

        if not self.is_valid(new_chain):
            return False

        self.chain = new_chain
        return True
    
    def save_to_disk(self, path: str = "blockchain.json"):
        """Spremi blockchain na disk."""
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    def load_from_disk(self, path: str = "blockchain.json") -> bool:
        """Učitaj blockchain s diska. Vraća True ako je uspješno."""
        if not os.path.exists(path):
            return False
        try:
            with open(path, "r") as f:
                chain_data = json.load(f)
            self.chain = [Block.from_dict(b) for b in chain_data]
            return True
        except Exception:
            return False