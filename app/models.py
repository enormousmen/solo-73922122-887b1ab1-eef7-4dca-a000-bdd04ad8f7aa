"""领域模型与时间工具。所有时间统一为 UTC 的 aware datetime。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class LatestReport:
    """某节点采样时间最新的一份探活报告。"""

    node_id: str
    sampled_at: datetime
    healthy: bool
    boot_id: str


@dataclass(frozen=True)
class PermitRecord:
    """一条许可。status: open（未结束）| finished（已结束）。

    released_nodes 记录已通过单节点完工释放的节点及其释放时间。
    已释放节点不再被本许可占用，可重新计票或被新许可占用。
    """

    request_id: str
    nodes: tuple[str, ...]  # 有序、去重（原始名单始终保留）
    status: str
    approved_at: datetime
    finished_at: datetime | None
    boot_ids_at_approval: dict[str, str]  # 获批时各节点最新报告中的启动标识
    # node -> 释放时间（UTC）；升级前的旧库无释放记录，默认空 = 尚未释放任何节点
    released_nodes: dict[str, datetime] = field(default_factory=dict)

    @property
    def occupied_nodes(self) -> tuple[str, ...]:
        """仍被本许可占用、尚未释放的节点（保持原名单顺序）。"""
        return tuple(n for n in self.nodes if n not in self.released_nodes)

    def is_released(self, node_id: str) -> bool:
        return node_id in self.released_nodes
