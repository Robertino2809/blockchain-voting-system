import asyncio
import httpx
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from blockchain.consensus.raft import RaftNode, RaftState, LogEntry
from shared.config import settings
from shared.logging_config import setup_logging

logger = setup_logging("raft_server")


# ── Dependency provider ──────────────────────────────────────

_raft_instance: RaftNode | None = None

def set_raft_instance(raft: RaftNode):
    global _raft_instance
    _raft_instance = raft

def get_raft() -> RaftNode:
    if _raft_instance is None:
        raise RuntimeError("RaftNode nije inicijaliziran!")
    return _raft_instance


# ── Pydantic modeli ──────────────────────────────────────────

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
    prev_log_index: int = -1
    prev_log_term: int = 0
    commit_index: int = -1

class AppendEntryResponse(BaseModel):
    term: int
    success: bool
    node_id: str


# ── Router ───────────────────────────────────────────────────

router = APIRouter()


@router.post("/raft/heartbeat", response_model=HeartbeatResponse)
async def receive_heartbeat(request: HeartbeatRequest, raft: RaftNode = Depends(get_raft)):
    async with raft._lock:
        success = raft.receive_heartbeat(request.term, request.leader_id)
        if success and request.commit_index > raft.commit_index and raft.log:
            idx = min(request.commit_index, len(raft.log) - 1)
            raft.commit_entry(idx)
        return HeartbeatResponse(term=raft.current_term, success=success, node_id=raft.node_id)


@router.post("/raft/vote", response_model=VoteResponse)
async def request_vote(request: VoteRequest, raft: RaftNode = Depends(get_raft)):
    async with raft._lock:
        result = raft.request_vote(
            term=request.term,
            candidate_id=request.candidate_id,
            last_log_index=request.last_log_index,
            last_log_term=request.last_log_term,
        )
        return VoteResponse(term=result["term"], vote_granted=result["vote_granted"], node_id=raft.node_id)


@router.post("/raft/append", response_model=AppendEntryResponse)
async def append_entry(request: AppendEntryRequest, raft: RaftNode = Depends(get_raft)):
    async with raft._lock:
        if request.term < raft.current_term:
            return AppendEntryResponse(term=raft.current_term, success=False, node_id=raft.node_id)

        raft.receive_heartbeat(request.term, request.leader_id)

        if request.prev_log_index >= 0:
            if len(raft.log) <= request.prev_log_index:
                logger.warning(f"Log gap: imam {len(raft.log)} entries, trebam index {request.prev_log_index}")
                return AppendEntryResponse(term=raft.current_term, success=False, node_id=raft.node_id)
            if raft.log[request.prev_log_index].term != request.prev_log_term:
                logger.warning(f"Log term mismatch na index {request.prev_log_index}")
                raft.log = raft.log[:request.prev_log_index]
                return AppendEntryResponse(term=raft.current_term, success=False, node_id=raft.node_id)

        entry = LogEntry(term=request.term, index=len(raft.log), data=request.entry)
        raft.log.append(entry)

        if request.commit_index >= entry.index:
            raft.commit_entry(entry.index)

        logger.info(f"AppendEntry od {request.leader_id}: index={entry.index}")
        return AppendEntryResponse(term=raft.current_term, success=True, node_id=raft.node_id)


@router.get("/raft/status")
async def raft_status(raft: RaftNode = Depends(get_raft)):
    return raft.get_status()


# ── RaftRunner ───────────────────────────────────────────────

class RaftRunner:
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
                async with self.raft._lock:
                    state = self.raft.state
                    timed_out = self.raft.is_election_timeout()

                if state == RaftState.LEADER:
                    await self._replicate_logs()
                    await self._send_heartbeats()
                    await asyncio.sleep(settings.raft_heartbeat_interval)
                elif timed_out:
                    logger.warning("Election timeout! Pokrećem izbore...")
                    async with self.raft._lock:
                        self.raft.start_election()
                    await self._request_votes()
                    await asyncio.sleep(0.5)
                else:
                    await asyncio.sleep(0.05)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Greška u Raft petlji: {e}")
                await asyncio.sleep(0.5)

    async def _replicate_logs(self):
        async with self.raft._lock:
            if not self.raft.log:
                return
            peers = list(self.raft.peers)

        async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
            for peer in peers:
                async with self.raft._lock:
                    next_idx = self.raft.next_index.get(peer, 0)
                    log_len = len(self.raft.log)

                if next_idx >= log_len:
                    continue

                for idx in range(next_idx, log_len):
                    async with self.raft._lock:
                        if idx >= len(self.raft.log):
                            break
                        entry = self.raft.log[idx]
                        prev_log_index = idx - 1
                        prev_log_term = self.raft.log[prev_log_index].term if prev_log_index >= 0 else 0
                        payload = {
                            "term": self.raft.current_term,
                            "leader_id": self.raft.node_id,
                            "entry": entry.data,
                            "prev_log_index": prev_log_index,
                            "prev_log_term": prev_log_term,
                            "commit_index": self.raft.commit_index,
                        }

                    try:
                        resp = await client.post(f"{peer}/raft/append", json=payload)
                        data = resp.json()
                        async with self.raft._lock:
                            if data["term"] > self.raft.current_term:
                                self.raft._become_follower(data["term"])
                                return
                            if data["success"]:
                                self.raft.next_index[peer] = idx + 1
                                self.raft.match_index[peer] = idx
                                logger.info(f"Entry {idx} repliciran na {peer}")
                                match_count = 1 + sum(
                                    1 for m in self.raft.match_index.values() if m >= idx
                                )
                                majority = (len(self.raft.peers) + 1) // 2 + 1
                                if match_count >= majority and idx > self.raft.commit_index:
                                    if entry.term == self.raft.current_term:
                                        self.raft.commit_entry(idx)
                            else:
                                self.raft.next_index[peer] = max(0, next_idx - 1)
                                break
                    except Exception:
                        logger.warning(f"Replikacija nije stigla do {peer} (index={idx})")
                        break

    async def _send_heartbeats(self):
        async with self.raft._lock:
            payload = {
                "term": self.raft.current_term,
                "leader_id": self.raft.node_id,
                "commit_index": self.raft.commit_index,
            }
            peers = list(self.raft.peers)

        async def send_one(peer: str):
            try:
                async with httpx.AsyncClient(timeout=1.0) as client:
                    resp = await client.post(f"{peer}/raft/heartbeat", json=payload)
                    data = resp.json()
                    async with self.raft._lock:
                        if data["term"] > self.raft.current_term:
                            logger.warning(f"Viši term od {peer}, vraćam se u follower")
                            self.raft._become_follower(data["term"])
            except Exception:
                logger.warning(f"Heartbeat nije stigao do {peer}")

        await asyncio.gather(*[send_one(peer) for peer in peers])

    async def _request_votes(self):
        async with self.raft._lock:
            last_log_index = len(self.raft.log) - 1
            last_log_term = self.raft.log[-1].term if self.raft.log else 0
            payload = {
                "term": self.raft.current_term,
                "candidate_id": self.raft.node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term,
            }
            peers = list(self.raft.peers)

        async def request_one(peer: str):
            try:
                async with httpx.AsyncClient(timeout=1.0) as client:
                    resp = await client.post(f"{peer}/raft/vote", json=payload)
                    data = resp.json()
                    async with self.raft._lock:
                        self.raft.receive_vote(
                            voter_id=data["node_id"],
                            term=data["term"],
                            vote_granted=data["vote_granted"],
                        )
            except Exception:
                logger.warning(f"Vote request nije stigao do {peer}")

        await asyncio.gather(*[request_one(peer) for peer in peers])