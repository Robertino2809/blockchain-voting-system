import asyncio
import httpx
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, APIRouter
from pydantic import BaseModel

from blockchain.consensus.raft import RaftNode, RaftState
from shared.config import settings
from shared.logging_config import setup_logging

logger = setup_logging("raft_server")
router = APIRouter()


# ── Pydantic modeli za Raft HTTP poruke ──────────────────────

class HeartbeatRequest(BaseModel):
    term: int
    leader_id: str
    commit_index: int = -1


class HeartbeatResponse(BaseModel):
    term: int
    success: bool
    node_id: str


class VoteRequest(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


class VoteResponse(BaseModel):
    term: int
    vote_granted: bool
    node_id: str


class AppendEntryRequest(BaseModel):
    term: int
    leader_id: str
    entry: dict
    commit_index: int


class AppendEntryResponse(BaseModel):
    term: int
    success: bool
    node_id: str


# ── Raft HTTP endpointi ──────────────────────────────────────

@router.post("/raft/heartbeat", response_model=HeartbeatResponse)
def receive_heartbeat(request: HeartbeatRequest, raft: RaftNode = None):
    success = raft.receive_heartbeat(request.term, request.leader_id)

    if request.commit_index > raft.commit_index and raft.log:
        idx = min(request.commit_index, len(raft.log) - 1)
        raft.commit_entry(idx)

    return HeartbeatResponse(
        term=raft.current_term,
        success=success,
        node_id=raft.node_id,
    )


@router.post("/raft/vote", response_model=VoteResponse)
def request_vote(request: VoteRequest, raft: RaftNode = None):
    result = raft.request_vote(
        term=request.term,
        candidate_id=request.candidate_id,
        last_log_index=request.last_log_index,
        last_log_term=request.last_log_term,
    )
    return VoteResponse(
        term=result["term"],
        vote_granted=result["vote_granted"],
        node_id=raft.node_id,
    )


@router.post("/raft/append", response_model=AppendEntryResponse)
def append_entry(request: AppendEntryRequest, raft: RaftNode = None):
    if request.term < raft.current_term:
        return AppendEntryResponse(
            term=raft.current_term,
            success=False,
            node_id=raft.node_id,
        )

    raft.receive_heartbeat(request.term, request.leader_id)

    from blockchain.consensus.raft import LogEntry
    entry = LogEntry(
        term=request.term,
        index=len(raft.log),
        data=request.entry,
    )
    raft.log.append(entry)

    if request.commit_index >= entry.index:
        raft.commit_entry(entry.index)

    logger.info(f"Append entry primljen od {request.leader_id}: index={entry.index}")
    return AppendEntryResponse(
        term=raft.current_term,
        success=True,
        node_id=raft.node_id,
    )


@router.get("/raft/status")
def raft_status(raft: RaftNode = None):
    return raft.get_status()


# ── Raft background loop ─────────────────────────────────────

class RaftRunner:
    """
    Pokreće Raft petlju u pozadini:
    - šalje heartbeatove ako smo leader
    - pokreće izbore ako je prošao election timeout
    """

    def __init__(self, raft: RaftNode):
        self.raft = raft
        self._task: asyncio.Task | None = None

    def start(self):
        self._task = asyncio.create_task(self._loop())
        logger.info(f"Raft runner pokrenut za {self.raft.node_id}")

    def stop(self):
        if self._task:
            self._task.cancel()

    async def _loop(self):
        while True:
            try:
                if self.raft.state == RaftState.LEADER:
                    await self._send_heartbeats()
                    await asyncio.sleep(settings.raft_heartbeat_interval)

                elif self.raft.is_election_timeout():
                    logger.warning(f"Election timeout! Pokrećem izbore...")
                    self.raft.start_election()
                    await self._request_votes()
                    await asyncio.sleep(0.1)

                else:
                    await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Greška u Raft petlji: {e}")
                await asyncio.sleep(0.5)

    async def _send_heartbeats(self):
        """Leader šalje heartbeat svim peerovima."""
        payload = {
            "term": self.raft.current_term,
            "leader_id": self.raft.node_id,
            "commit_index": self.raft.commit_index,
        }
        async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
            for peer in self.raft.peers:
                try:
                    resp = await client.post(f"{peer}/raft/heartbeat", json=payload)
                    data = resp.json()
                    if data["term"] > self.raft.current_term:
                        logger.warning(f"Viši term od {peer}, vraćam se u follower")
                        self.raft._become_follower(data["term"])
                        break
                except Exception:
                    logger.warning(f"Heartbeat nije stigao do {peer}")

    async def _request_votes(self):
        """Candidate šalje RequestVote svim peerovima."""
        last_log_index = len(self.raft.log) - 1
        last_log_term = self.raft.log[-1].term if self.raft.log else 0

        payload = {
            "term": self.raft.current_term,
            "candidate_id": self.raft.node_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term,
        }
        async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
            for peer in self.raft.peers:
                try:
                    resp = await client.post(f"{peer}/raft/vote", json=payload)
                    data = resp.json()
                    self.raft.receive_vote(
                        voter_id=data["node_id"],
                        term=data["term"],
                        vote_granted=data["vote_granted"],
                    )
                except Exception:
                    logger.warning(f"Vote request nije stigao do {peer}")