import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    # ── Node identitet ───────────────────────────────────────
    node_id: str = field(default_factory=lambda: os.getenv("NODE_ID", "node-1"))
    node_host: str = field(default_factory=lambda: os.getenv("NODE_HOST", "0.0.0.0"))
    node_port: int = field(default_factory=lambda: int(os.getenv("NODE_PORT", "8000")))

    # ── Blockchain ───────────────────────────────────────────
    pow_difficulty: int = field(default_factory=lambda: int(os.getenv("POW_DIFFICULTY", "3")))
    data_dir: str = field(default_factory=lambda: os.getenv("DATA_DIR", "./data"))

    # ── Raft konsenzus ───────────────────────────────────────
    raft_election_timeout_min: float = field(default_factory=lambda: float(os.getenv("RAFT_ELECTION_TIMEOUT_MIN", "1.5")))
    raft_election_timeout_max: float = field(default_factory=lambda: float(os.getenv("RAFT_ELECTION_TIMEOUT_MAX", "3.0")))
    raft_heartbeat_interval: float = field(default_factory=lambda: float(os.getenv("RAFT_HEARTBEAT_INTERVAL", "0.5")))

    # ── RabbitMQ ─────────────────────────────────────────────
    rabbitmq_host: str = field(default_factory=lambda: os.getenv("RABBITMQ_HOST", "localhost"))
    rabbitmq_port: int = field(default_factory=lambda: int(os.getenv("RABBITMQ_PORT", "5672")))
    rabbitmq_user: str = field(default_factory=lambda: os.getenv("RABBITMQ_USER", "guest"))
    rabbitmq_password: str = field(default_factory=lambda: os.getenv("RABBITMQ_PASSWORD", "guest"))

    # ── Redis ────────────────────────────────────────────────
    redis_host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    redis_port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))

    # ── Peer Discovery ───────────────────────────────────────
    peer_discovery_host: str = field(default_factory=lambda: os.getenv("PEER_DISCOVERY_HOST", "localhost"))
    peer_discovery_port: int = field(default_factory=lambda: int(os.getenv("PEER_DISCOVERY_PORT", "8500")))
    seed_peers: list[str] = field(default_factory=lambda: [
        p.strip() for p in os.getenv("SEED_PEERS", "").split(",") if p.strip()
    ])

    # ── API Gateway ──────────────────────────────────────────
    api_gateway_host: str = field(default_factory=lambda: os.getenv("API_GATEWAY_HOST", "localhost"))
    api_gateway_port: int = field(default_factory=lambda: int(os.getenv("API_GATEWAY_PORT", "8080")))

    # ── HTTP timeouts ────────────────────────────────────────
    http_timeout: int = field(default_factory=lambda: int(os.getenv("HTTP_TIMEOUT", "3")))

    @property
    def node_url(self) -> str:
        return f"http://{self.node_host}:{self.node_port}"

    @property
    def rabbitmq_url(self) -> str:
        return f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password}@{self.rabbitmq_host}:{self.rabbitmq_port}/"

    @property
    def data_path(self) -> str:
        os.makedirs(self.data_dir, exist_ok=True)
        return self.data_dir


settings = Settings()