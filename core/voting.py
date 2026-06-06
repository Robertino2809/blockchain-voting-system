import time
from dataclasses import dataclass

from core.blockchain import Blockchain


@dataclass
class VoteResult:
    success: bool
    message: str
    vote: dict | None = None


@dataclass
class CandidateResult:
    candidate: str
    votes: int
    percentage: float


@dataclass
class VotingResults:
    candidates: list[CandidateResult]
    total_votes: int
    pending_votes: int
    blocks_mined: int


class VotingService:
    """
    Servis koji enkapsulira svu logiku glasanja.
    Routeri u app/ sloju komuniciraju samo s ovim servisom,
    nikad direktno s Blockchain klasom.
    """

    def __init__(self, blockchain: Blockchain):
        self.blockchain = blockchain

    def cast_vote(self, voter_id: str, candidate: str) -> VoteResult:
        """Validacija i dodavanje glasa u pending pool."""
        voter_id = voter_id.strip()
        candidate = candidate.strip()

        if not voter_id:
            return VoteResult(success=False, message="voter_id ne smije biti prazan.")

        if not candidate:
            return VoteResult(success=False, message="Kandidat ne smije biti prazan.")

        result = self.blockchain.add_vote(voter_id, candidate)

        if not result["success"]:
            return VoteResult(success=False, message=result["error"])

        vote_with_time = {**result["vote"], "timestamp": time.time()}
        return VoteResult(
            success=True,
            message="Glas uspješno dodan u pending pool.",
            vote=vote_with_time,
        )

    def get_results(self) -> VotingResults:
        """Vraća trenutne rezultate glasanja s postotcima."""
        raw = self.blockchain.get_results()
        total = sum(raw.values())

        candidates = [
            CandidateResult(
                candidate=name,
                votes=count,
                percentage=round(count / total * 100, 2) if total > 0 else 0.0,
            )
            for name, count in sorted(raw.items(), key=lambda x: -x[1])
        ]

        return VotingResults(
            candidates=candidates,
            total_votes=total,
            pending_votes=len(self.blockchain.pending_votes),
            blocks_mined=len(self.blockchain.chain) - 1,  # ne računamo genesis
        )

    def get_pending(self) -> list[dict]:
        """Vraća listu glasova koji čekaju rudarenje."""
        return self.blockchain.pending_votes.copy()

    def has_voted(self, voter_id: str) -> bool:
        """Provjeri je li glasač već glasao."""
        return self.blockchain.has_voted(voter_id.strip())