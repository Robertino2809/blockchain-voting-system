import asyncio
import random
import time
from enum import Enum
from dataclasses import dataclass, field
from shared.logging_config import setup_logging
from shared.config import settings

logger = setup_logging("raft")


class RaftState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    data: dict


@dataclass
class RaftNode:
    node_id: str
    peers: list[str] = field(default_factory=list)

    # ── Persistent state ─────────────────────────────────────
    current_term: int = 0
    voted_for: str | None = None
    log: list[LogEntry] = field(default_factory=list)

    # ── Volatile state ───────────────────────────────────────
    state: RaftState = RaftState.FOLLOWER
    commit_index: int = -1
    last_applied: int = -1
    votes_received: set = field(default_factory=set)

    # ── Leader volatile state ────────────────────────────────
    next_index: dict[str, int] = field(default_factory=dict)
    match_index: dict[str, int] = field(default_factory=dict)

    # ── Timing ───────────────────────────────────────────────
    last_heartbeat: float = field(default_factory=time.time)
    election_timeout: float = field(default_factory=lambda: random.uniform(
        settings.raft_election_timeout_min,
        settings.raft_election_timeout_max,
    ))

    # ── Callbacks ────────────────────────────────────────────
    _on_become_leader: list = field(default_factory=list)
    _on_commit: list = field(default_factory=list)

    def __post_init__(self):
        logger.info(f"Raft čvor inicijaliziran: {self.node_id} | timeout: {self.election_timeout:.2f}s")

    # ── Javno sučelje ────────────────────────────────────────

    def on_become_leader(self, callback):
        """Registriraj callback koji se poziva kad čvor postane leader."""
        self._on_become_leader.append(callback)

    def on_commit(self, callback):
        """Registriraj callback koji se poziva kad se entry commita."""
        self._on_commit.append(callback)

    def receive_heartbeat(self, term: int, leader_id: str):
        """Primljeni heartbeat od leadera."""
        if term < self.current_term:
            return False

        self.last_heartbeat = time.time()

        if term > self.current_term:
            self._become_follower(term)
        elif self.state == RaftState.CANDIDATE:
            self._become_follower(term)

        logger.debug(f"Heartbeat primljen od {leader_id} (term {term})")
        return True

    def request_vote(self, term: int, candidate_id: str, last_log_index: int, last_log_term: int) -> dict:
        """Odgovori na zahtjev za glasom."""
        # Odbij ako je naš term veći
        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        if term > self.current_term:
            self._become_follower(term)

        # Glasaj samo ako još nismo glasali ili smo glasali za istog kandidata
        already_voted = self.voted_for is not None and self.voted_for != candidate_id
        if already_voted:
            return {"term": self.current_term, "vote_granted": False}

        # Provjeri je li kandidatov log ažurniji od našeg
        our_last_index = len(self.log) - 1
        our_last_term = self.log[-1].term if self.log else 0

        log_ok = (last_log_term > our_last_term) or \
                 (last_log_term == our_last_term and last_log_index >= our_last_index)

        if not log_ok:
            return {"term": self.current_term, "vote_granted": False}

        self.voted_for = candidate_id
        self.last_heartbeat = time.time()
        logger.info(f"Glasao za {candidate_id} u termu {term}")
        return {"term": self.current_term, "vote_granted": True}

    def receive_vote(self, voter_id: str, term: int, vote_granted: bool):
        """Primljeni odgovor na RequestVote."""
        if self.state != RaftState.CANDIDATE:
            return

        if term > self.current_term:
            self._become_follower(term)
            return

        if not vote_granted:
            return

        self.votes_received.add(voter_id)
        majority = (len(self.peers) + 1) // 2 + 1
        logger.info(f"Glasovi: {len(self.votes_received)}/{majority} potrebno")

        if len(self.votes_received) >= majority:
            self._become_leader()

    def append_entry(self, data: dict) -> LogEntry | None:
        """Leader dodaje novi entry u log (CQRS write)."""
        if self.state != RaftState.LEADER:
            logger.warning("Samo leader može dodavati entries!")
            return None

        entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            data=data,
        )
        self.log.append(entry)
        logger.info(f"Log entry dodan: index={entry.index}, term={entry.term}")
        return entry

    def commit_entry(self, index: int):
        """Commita entry kad ga potvrdi većina čvorova."""
        if index <= self.commit_index:
            return

        self.commit_index = index
        logger.info(f"Entry committan: index={index}")

        for callback in self._on_commit:
            callback(self.log[index])

    def is_election_timeout(self) -> bool:
        """Provjeri je li prošlo dovoljno vremena bez heartbeata."""
        return (
            self.state != RaftState.LEADER and
            time.time() - self.last_heartbeat > self.election_timeout
        )

    def start_election(self):
        """Pokreni izbore za novog leadera."""
        self.current_term += 1
        self.state = RaftState.CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.election_timeout = random.uniform(
            settings.raft_election_timeout_min,
            settings.raft_election_timeout_max,
        )
        logger.info(f"Izbori pokrenuti | term={self.current_term} | peers={len(self.peers)}")

        # Ako nema peerova — odmah postani leader
        if not self.peers:
            logger.info("Nema peerova — automatski postajim LEADER")
            self._become_leader()

    def get_status(self) -> dict:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log_length": len(self.log),
            "commit_index": self.commit_index,
            "peers": self.peers,
        }

    # ── Privatne metode ──────────────────────────────────────

    def _become_follower(self, term: int):
        logger.info(f"Prelazim u FOLLOWER | term {self.current_term} → {term}")
        self.state = RaftState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.votes_received = set()
        self.last_heartbeat = time.time()

    def _become_leader(self):
        logger.info(f"Postao LEADER u termu {self.current_term}!")
        self.state = RaftState.LEADER

        # Inicijaliziraj next_index i match_index za sve peere
        for peer in self.peers:
            self.next_index[peer] = len(self.log)
            self.match_index[peer] = -1

        for callback in self._on_become_leader:
            callback(self.node_id, self.current_term)