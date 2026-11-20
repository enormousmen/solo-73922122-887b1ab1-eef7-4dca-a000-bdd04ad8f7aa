"""关键流程自测：探活、批准、多数票、幂等、结束、持久化、并发。"""
from __future__ import annotations

import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402

MANIFEST = {
    "shards": {
        "shard-a": ["n1", "n2", "n3"],
        "shard-b": ["n2", "n3", "n4"],
        "shard-c": ["n3", "n4", "n5"],
    }
}
ALL_NODES = ["n1", "n2", "n3", "n4", "n5"]
T0 = datetime.now(timezone.utc) - timedelta(minutes=30)  # 初始探活采样时间


@pytest.fixture()
def make_client(tmp_path):
    """工厂：同一 db 路径可反复建 app，模拟服务重启。"""
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(MANIFEST))
    db_path = tmp_path / "permits.db"
    clients = []

    def _make():
        app = create_app(db_path=str(db_path), manifest_path=str(manifest_path))
        c = TestClient(app)
        clients.append(c)
        return c

    yield _make
    for c in clients:
        c.close()


@pytest.fixture()
def client(make_client):
    return make_client()


def report(client, node, healthy=True, boot="boot-1", sampled_at=None):
    sampled_at = sampled_at or datetime.now(timezone.utc)
    return client.post(
        "/liveness",
        json={
            "reports": [
                {
                    "node_id": node,
                    "sampled_at": sampled_at.isoformat(),
                    "healthy": healthy,
                    "boot_id": boot,
                }
            ]
        },
    )


def seed_all_healthy(client, boot="boot-1", sampled_at=T0):
    for n in ALL_NODES:
        r = report(client, n, healthy=True, boot=boot, sampled_at=sampled_at)
        assert r.status_code == 201, r.text


def request_permit(client, request_id, nodes):
    return client.post("/permits", json={"request_id": request_id, "nodes": nodes})


# ---------- 基础 ----------


def test_healthz_and_manifest(client):
    assert client.get("/healthz").json() == {"status": "ok"}
    m = client.get("/manifest").json()["shards"]
    assert m["shard-a"] == {"voters": ["n1", "n2", "n3"], "majority_needed": 2}
    assert m["shard-c"]["voters"] == ["n3", "n4", "n5"]


def test_liveness_latest_by_sample_time_wins(client):
    t1 = datetime(2026, 11, 19, 10, 0, tzinfo=timezone.utc)
    t0 = t1 - timedelta(hours=1)
    t2 = t1 + timedelta(hours=1)
    assert report(client, "n1", healthy=True, boot="b1", sampled_at=t1).status_code == 201
    # 更旧的采样时间后到达，不得覆盖
    assert report(client, "n1", healthy=False, boot="b0", sampled_at=t0).status_code == 201
    got = client.get("/liveness/n1").json()
    assert got["healthy"] is True and got["boot_id"] == "b1"
    # 更新的采样时间覆盖
    assert report(client, "n1", healthy=False, boot="b2", sampled_at=t2).status_code == 201
    got = client.get("/liveness/n1").json()
    assert got["healthy"] is False and got["boot_id"] == "b2"


def test_liveness_unknown_node_rejected(client):
    r = report(client, "ghost-node")
    assert r.status_code == 422
    assert r.json()["detail"]["unknown_nodes"] == ["ghost-node"]
    assert client.get("/liveness/ghost-node").status_code == 404


# ---------- 批准与幂等 ----------


def test_permit_happy_path_and_idempotent_replay(client):
    seed_all_healthy(client)
    r = request_permit(client, "req-1", ["n1"])
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == "open"
    assert body["boot_ids_at_approval"] == {"n1": "boot-1"}

    # 同号同批重发 -> 200 返回原许可（approved_at 不变）
    replay = request_permit(client, "req-1", ["n1"])
    assert replay.status_code == 200
    assert replay.json()["approved_at"] == body["approved_at"]

    # 节点顺序/重复不影响"同批"判定
    replay2 = request_permit(client, "req-1", ["n1", "n1"])
    assert replay2.status_code == 200

    # 同号不同批 -> 409
    conflict = request_permit(client, "req-1", ["n2"])
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["error"] == "request_id_conflict"


def test_permit_unknown_node_rejected(client):
    seed_all_healthy(client)
    r = request_permit(client, "req-x", ["n1", "ghost"])
    assert r.status_code == 422
    assert r.json()["detail"]["unknown_nodes"] == ["ghost"]


def test_reject_unhealthy_or_unknown_health(client):
    report(client, "n1", healthy=True, sampled_at=T0)
    report(client, "n3", healthy=False, sampled_at=T0)
    # n2 从未上报 -> 健康未知
    r = request_permit(client, "req-u1", ["n2"])
    assert r.status_code == 409
    assert r.json()["detail"]["health_unknown_nodes"] == ["n2"]
    r = request_permit(client, "req-u2", ["n3"])
    assert r.status_code == 409
    assert r.json()["detail"]["unhealthy_nodes"] == ["n3"]


def test_reject_node_occupied_by_open_permit(client):
    seed_all_healthy(client)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    r = request_permit(client, "req-2", ["n1"])
    assert r.status_code == 409
    assert r.json()["detail"]["occupied_nodes"] == [{"node": "n1", "permit": "req-1"}]


# ---------- 多数票 ----------


def test_quorum_guard_lists_affected_shards(client):
    seed_all_healthy(client)
    # n1 只占 shard-a 一票，批准后 shard-a 剩 n2,n3 = 2/3，仍过半
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    # 再重启 n2：shard-a 扣除 n1(许可占用)、n2(本批) 后只剩 n3 = 1/3，不过半
    r = request_permit(client, "req-2", ["n2"])
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["error"] == "permit_rejected"
    shards = {s["shard"]: s for s in detail["affected_shards"]}
    assert set(shards) == {"shard-a"}  # shard-b 扣 n2 后仍有 n3,n4，不受影响
    sa = shards["shard-a"]
    assert sa["voters"] == 3 and sa["required_available"] == 2 and sa["available"] == 1
    assert sa["excluded"] == {"n1": "occupied_by:req-1", "n2": "in_batch"}
    # 不冲突的申请仍可批准（n5 只占 shard-c）
    assert request_permit(client, "req-3", ["n5"]).status_code == 201


def test_quorum_counts_unhealthy_and_unknown_as_unavailable(client):
    # n3 异常、n5 从未上报；申请重启 n4 时 shard-c(n3,n4,n5) 三票全部被扣
    report(client, "n1", healthy=True, sampled_at=T0)
    report(client, "n2", healthy=True, sampled_at=T0)
    report(client, "n3", healthy=False, sampled_at=T0)
    report(client, "n4", healthy=True, sampled_at=T0)
    r = request_permit(client, "req-q", ["n4"])
    assert r.status_code == 409
    shards = {s["shard"]: s for s in r.json()["detail"]["affected_shards"]}
    assert shards["shard-c"]["excluded"] == {
        "n3": "unhealthy",
        "n4": "in_batch",
        "n5": "health_unknown",
    }
    assert shards["shard-c"]["available"] == 0


# ---------- 结束许可 ----------


def test_finish_flow(client):
    seed_all_healthy(client, boot="boot-1", sampled_at=T0)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201

    # 最新探活仍是获批前采集的 -> 拒绝
    r = client.post("/permits/req-1/finish")
    assert r.status_code == 409
    assert r.json()["detail"]["nodes"] == {"n1": "not_sampled_after_approval"}

    later = datetime.now(timezone.utc) + timedelta(minutes=10)
    # 获批后采集但启动标识未变 -> 拒绝
    report(client, "n1", healthy=True, boot="boot-1", sampled_at=later)
    r = client.post("/permits/req-1/finish")
    assert r.json()["detail"]["nodes"] == {"n1": "boot_id_unchanged"}

    # 新启动标识但不健康 -> 拒绝
    report(client, "n1", healthy=False, boot="boot-2", sampled_at=later + timedelta(minutes=1))
    r = client.post("/permits/req-1/finish")
    assert r.json()["detail"]["nodes"] == {"n1": "unhealthy"}

    # 获批后采集 + 健康 + 新启动标识 -> 结束
    report(client, "n1", healthy=True, boot="boot-2", sampled_at=later + timedelta(minutes=2))
    r = client.post("/permits/req-1/finish")
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "finished"
    assert r.json()["finished_at"]

    # 重复结束 -> 409
    assert client.post("/permits/req-1/finish").status_code == 409
    # 节点已释放，可再获许可
    assert request_permit(client, "req-2", ["n1"]).status_code == 201


def test_finish_requires_every_node_in_batch(client):
    seed_all_healthy(client, boot="boot-1", sampled_at=T0)
    # n1 在 shard-a，n4 在 shard-b/c；各分片扣除后仍过半
    assert request_permit(client, "req-1", ["n1", "n4"]).status_code == 201
    later = datetime.now(timezone.utc) + timedelta(minutes=10)
    report(client, "n1", healthy=True, boot="boot-2", sampled_at=later)
    # n4 尚未重启上报 -> 整批不能结束
    r = client.post("/permits/req-1/finish")
    assert r.status_code == 409
    assert r.json()["detail"]["nodes"] == {"n4": "not_sampled_after_approval"}
    report(client, "n4", healthy=True, boot="boot-9", sampled_at=later)
    assert client.post("/permits/req-1/finish").status_code == 200


def test_finish_unknown_permit_404(client):
    assert client.post("/permits/nope/finish").status_code == 404
    assert client.get("/permits/nope").status_code == 404


# ---------- 单节点完工（节点释放） ----------


def _reboot_report(client, node, boot, minutes=10):
    """模拟节点重启后的探活：获批后采样、健康、新启动标识。"""
    later = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    return report(client, node, healthy=True, boot=boot, sampled_at=later)


def test_single_node_completion_releases_and_frees_quota(client):
    seed_all_healthy(client, boot="boot-1", sampled_at=T0)
    # 两节点一批：n1 仅属 shard-a、n4 属 shard-b/c，各分片只扣一票仍过半
    assert request_permit(client, "req-1", ["n1", "n4"]).status_code == 201

    # 条件不满足（n1 还是获批前的探活）-> 409，许可状态不变
    r = client.post("/permits/req-1/nodes/n1/complete")
    assert r.status_code == 409
    assert r.json()["detail"] == {
        "error": "node_completion_conditions_not_met",
        "node": "n1",
        "reason": "not_sampled_after_approval",
    }
    view = client.get("/permits/req-1").json()
    assert view["status"] == "open"
    assert view["occupied_nodes"] == ["n1", "n4"]
    assert view["node_releases"]["n1"] == {"released": False, "released_at": None}

    # n1 重启后探活达标 -> 释放 n1，许可仍 open
    _reboot_report(client, "n1", "boot-2")
    r = client.post("/permits/req-1/nodes/n1/complete")
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["released"] is True and body["permit_finished"] is False
    assert body["idempotent_replay"] is False
    first_released_at = body["released_at"]

    view = client.get("/permits/req-1").json()
    assert view["status"] == "open"
    assert view["nodes"] == ["n1", "n4"]  # 原节点名单保留
    assert view["occupied_nodes"] == ["n4"]
    assert view["released_node_count"] == 1
    assert view["node_releases"]["n1"]["released"] is True
    assert view["node_releases"]["n1"]["released_at"] == first_released_at
    assert view["node_releases"]["n4"]["released"] is False

    # n1 已释放：可被新许可占用
    assert request_permit(client, "req-2", ["n1"]).status_code == 201
    # n4 仍被 req-1 占用
    r = request_permit(client, "req-3", ["n4"])
    assert r.status_code == 409
    assert r.json()["detail"]["occupied_nodes"] == [{"node": "n4", "permit": "req-1"}]


def test_single_node_completion_idempotent_even_when_reoccupied(client):
    seed_all_healthy(client, boot="boot-1", sampled_at=T0)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    _reboot_report(client, "n1", "boot-2")
    r = client.post("/permits/req-1/nodes/n1/complete")
    assert r.status_code == 201
    released_at = r.json()["released_at"]
    assert r.json()["permit_finished"] is True  # 最后一个节点释放 -> 结单
    assert client.get("/permits/req-1").json()["status"] == "finished"

    # 节点被新许可重新占用
    assert request_permit(client, "req-2", ["n1"]).status_code == 201

    # 对旧许可重复办理已释放节点 -> 200 返回原释放时间
    replay = client.post("/permits/req-1/nodes/n1/complete")
    assert replay.status_code == 200
    assert replay.json()["released_at"] == released_at
    assert replay.json()["idempotent_replay"] is True


def test_completion_errors_leave_state_unchanged(client):
    seed_all_healthy(client, boot="boot-1", sampled_at=T0)
    assert request_permit(client, "req-1", ["n1", "n4"]).status_code == 201

    # 节点不在本批内 -> 404
    r = client.post("/permits/req-1/nodes/n5/complete")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "node_not_in_permit"

    # 许可不存在 -> 404
    assert client.post("/permits/nope/nodes/n1/complete").status_code == 404

    # 不健康的最新探活 -> 409 unhealthy，未释放
    bad = datetime.now(timezone.utc) + timedelta(minutes=10)
    report(client, "n1", healthy=False, boot="boot-2", sampled_at=bad)
    r = client.post("/permits/req-1/nodes/n1/complete")
    assert r.status_code == 409 and r.json()["detail"]["reason"] == "unhealthy"

    # 启动标识未变 -> 409 boot_id_unchanged
    report(client, "n1", healthy=True, boot="boot-1", sampled_at=bad + timedelta(minutes=1))
    r = client.post("/permits/req-1/nodes/n1/complete")
    assert r.status_code == 409 and r.json()["detail"]["reason"] == "boot_id_unchanged"

    view = client.get("/permits/req-1").json()
    assert view["occupied_nodes"] == ["n1", "n4"]


def test_finish_only_checks_still_occupied_nodes(client):
    seed_all_healthy(client, boot="boot-1", sampled_at=T0)
    assert request_permit(client, "req-1", ["n1", "n4"]).status_code == 201

    # 先单节点完工释放 n1
    _reboot_report(client, "n1", "boot-2", minutes=10)
    assert client.post("/permits/req-1/nodes/n1/complete").status_code == 201
    n1_released_at = client.get("/permits/req-1").json()["node_releases"]["n1"]["released_at"]

    # 此时结单：只剩 n4 需要检查，n4 未达标 -> 409 只报 n4
    r = client.post("/permits/req-1/finish")
    assert r.status_code == 409
    assert r.json()["detail"]["nodes"] == {"n4": "not_sampled_after_approval"}

    # n4 达标后整单结单：n1 保留原释放时间，n4 用结单时间
    _reboot_report(client, "n4", "boot-3", minutes=20)
    r = client.post("/permits/req-1/finish")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "finished" and body["finished_at"]
    assert body["node_releases"]["n1"]["released_at"] == n1_released_at
    assert body["node_releases"]["n4"]["released_at"] == body["finished_at"]
    assert body["occupied_nodes"] == []

    # 结单后 finish 仍是 409（原行为不变）
    assert client.post("/permits/req-1/finish").status_code == 409
    # 结单后单节点完工走幂等分支，返回原释放时间
    replay = client.post("/permits/req-1/nodes/n4/complete")
    assert replay.status_code == 200
    assert replay.json()["released_at"] == body["node_releases"]["n4"]["released_at"]


def test_released_node_can_be_reapproved_in_another_permit(client):
    """端到端：节点释放后再次获批、再释放的完整循环。"""
    seed_all_healthy(client, boot="boot-1", sampled_at=T0)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    _reboot_report(client, "n1", "boot-2")
    assert client.post("/permits/req-1/nodes/n1/complete").status_code == 201

    # 同节点新许可获批，快照新的 boot_id
    r = request_permit(client, "req-2", ["n1"])
    assert r.status_code == 201
    assert r.json()["boot_ids_at_approval"] == {"n1": "boot-2"}

    # 最新探活 boot_id 与第二次获批快照相同 -> 不满足完工
    r = client.post("/permits/req-2/nodes/n1/complete")
    assert r.status_code == 409 and r.json()["detail"]["reason"] == "boot_id_unchanged"
    # 再次重启（boot-3）后完工
    _reboot_report(client, "n1", "boot-3", minutes=30)
    r = client.post("/permits/req-2/nodes/n1/complete")
    assert r.status_code == 201 and r.json()["permit_finished"] is True


def test_finish_after_all_nodes_completed_is_already_finished(client):
    """所有节点都经单节点完工释放后，许可已自动结单，finish 保持原 409 语义。"""
    seed_all_healthy(client, boot="boot-1", sampled_at=T0)
    assert request_permit(client, "req-1", ["n1", "n4"]).status_code == 201
    _reboot_report(client, "n1", "boot-2", minutes=10)
    _reboot_report(client, "n4", "boot-3", minutes=10)
    assert client.post("/permits/req-1/nodes/n1/complete").status_code == 201
    r = client.post("/permits/req-1/nodes/n4/complete")
    assert r.status_code == 201 and r.json()["permit_finished"] is True
    # 自动结单后再调原结单接口 -> 409（与原 permit_already_finished 行为一致）
    r = client.post("/permits/req-1/finish")
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "permit_already_finished"
    view = client.get("/permits/req-1").json()
    assert view["status"] == "finished" and view["occupied_nodes"] == []


def test_node_releases_survive_restart(make_client):
    c1 = make_client()
    seed_all_healthy(c1, boot="boot-1", sampled_at=T0)
    assert request_permit(c1, "req-1", ["n1", "n4"]).status_code == 201
    _reboot_report(c1, "n1", "boot-2")
    r = c1.post("/permits/req-1/nodes/n1/complete")
    assert r.status_code == 201
    released_at = r.json()["released_at"]
    c1.close()

    # 重启后续办：释放状态保留，n1 可再获批，n4 仍被占用
    c2 = make_client()
    view = c2.get("/permits/req-1").json()
    assert view["status"] == "open"
    assert view["occupied_nodes"] == ["n4"]
    assert view["node_releases"]["n1"]["released_at"] == released_at
    assert request_permit(c2, "req-2", ["n1"]).status_code == 201
    assert request_permit(c2, "req-3", ["n4"]).status_code == 409

    # 重启后继续把 n4 完工，整单结单
    _reboot_report(c2, "n4", "boot-3", minutes=20)
    r = c2.post("/permits/req-1/nodes/n4/complete")
    assert r.status_code == 201 and r.json()["permit_finished"] is True
    assert c2.get("/permits/req-1").json()["status"] == "finished"


def test_legacy_open_permit_treated_as_no_releases(tmp_path):
    """升级前已有的未结许可（node_releases 无记录）视为尚未释放任何节点。"""
    import sqlite3

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(MANIFEST))
    db_path = tmp_path / "legacy.db"
    approved = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    # 手工构造旧版库：只有 permits 表、没有 node_releases 表
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE liveness_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT, node_id TEXT, sampled_at TEXT,
            healthy INTEGER, boot_id TEXT, received_at TEXT);
        CREATE TABLE permits (
            request_id TEXT PRIMARY KEY, nodes TEXT, status TEXT,
            approved_at TEXT, finished_at TEXT, boot_ids_at_approval TEXT);
        """
    )
    conn.execute(
        "INSERT INTO permits VALUES (?,?,?,?,?,?)",
        (
            "old-req",
            json.dumps(["n1", "n4"]),
            "open",
            approved,
            None,
            json.dumps({"n1": "boot-1", "n4": "boot-1"}),
        ),
    )
    conn.commit()
    conn.close()

    app = create_app(db_path=str(db_path), manifest_path=str(manifest_path))
    with TestClient(app) as client:
        view = client.get("/permits/old-req").json()
        assert view["status"] == "open"
        assert view["occupied_nodes"] == ["n1", "n4"]  # 无节点被视为已释放
        assert all(v["released"] is False for v in view["node_releases"].values())
        # 占用仍生效：n1 探活健康，但因被旧许可占用而拒绝新申请
        report(client, "n1", healthy=True, boot="boot-1", sampled_at=T0)
        assert request_permit(client, "new-req", ["n1"]).status_code == 409
        assert client.get("/permits/new-req").status_code == 404  # 被拒申请不落库


def test_concurrent_completion_and_permit_request(client):
    """完工释放与新申请并发：按实际生效顺序判定，节点不会被两方同时占用。"""
    seed_all_healthy(client, boot="boot-1", sampled_at=T0)
    assert request_permit(client, "req-1", ["n1", "n4"]).status_code == 201
    _reboot_report(client, "n1", "boot-2")

    barrier = threading.Barrier(2)

    def complete_n1():
        barrier.wait()
        return client.post("/permits/req-1/nodes/n1/complete").status_code

    def grab_n1():
        barrier.wait()
        return request_permit(client, "req-2", ["n1"]).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(complete_n1)
        f2 = pool.submit(grab_n1)
        code_complete, code_grab = f1.result(), f2.result()

    assert code_complete == 201
    assert code_grab in (201, 409)  # 取决于谁先拿到写事务
    view1 = client.get("/permits/req-1").json()
    assert view1["occupied_nodes"] == ["n4"]
    # 若新申请先到，n1 仍被 req-1 占用 -> 拒绝；完工后新申请必可获批
    if code_grab == 409:
        assert request_permit(client, "req-2", ["n1"]).status_code == 201
    owners = []
    for rid in ("req-1", "req-2"):
        v = client.get(f"/permits/{rid}").json()
        if "n1" in v["occupied_nodes"]:
            owners.append(rid)
    assert owners == ["req-2"]  # 任意时刻只有一个许可占用 n1


# ---------- 持久化 ----------


def test_state_survives_restart(make_client):
    c1 = make_client()
    seed_all_healthy(c1, boot="boot-1", sampled_at=T0)
    assert request_permit(c1, "req-1", ["n1"]).status_code == 201
    c1.close()

    # 模拟服务重启：同一 db 文件新建 app
    c2 = make_client()
    p = c2.get("/permits/req-1").json()
    assert p["status"] == "open" and p["nodes"] == ["n1"]
    lv = c2.get("/liveness/n1").json()
    assert lv["healthy"] is True and lv["boot_id"] == "boot-1"
    # 占用关系仍在：n1 不能再被新申请占用
    assert request_permit(c2, "req-2", ["n1"]).status_code == 409


# ---------- 并发 ----------


def test_concurrent_requests_keep_majority(client):
    seed_all_healthy(client)
    # 8 个并发申请，交替抢 n1 / n2（同属 shard-a，3 票最多扣 1 票）
    reqs = [(f"c-{i}", ["n1" if i % 2 == 0 else "n2"]) for i in range(8)]

    def submit(req):
        rid, nodes = req
        return request_permit(client, rid, nodes).status_code

    with ThreadPoolExecutor(max_workers=8) as pool:
        codes = list(pool.map(submit, reqs))

    assert codes.count(201) == 1, codes
    assert codes.count(409) == 7, codes
    opens = client.get("/permits", params={"status": "open"}).json()["permits"]
    assert len(opens) == 1
