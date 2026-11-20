"""FastAPI 应用：分批重启许可 API。"""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException, Query, Response

from .logic import (
    evaluate_finish,
    evaluate_node_completion,
    evaluate_permit_request,
)
from .manifest import load_manifest
from .models import PermitRecord, now_utc, to_iso
from .schemas import LivenessBatchIn, PermitRequestIn
from .store import Store


def create_app(db_path: str, manifest_path: str) -> FastAPI:
    manifest = load_manifest(manifest_path)
    store = Store(db_path)

    app = FastAPI(title="restart-permit-api", version="1.1.0")
    app.state.store = store
    app.state.manifest = manifest

    def permit_view(p: PermitRecord) -> dict:
        # 保留原节点名单，并逐节点显示释放状态与释放时间
        node_status = {
            n: {
                "released": n in p.released_nodes,
                "released_at": to_iso(p.released_nodes[n])
                if n in p.released_nodes
                else None,
            }
            for n in p.nodes
        }
        return {
            "request_id": p.request_id,
            "nodes": list(p.nodes),
            "status": p.status,
            "approved_at": to_iso(p.approved_at),
            "finished_at": to_iso(p.finished_at) if p.finished_at else None,
            "boot_ids_at_approval": p.boot_ids_at_approval,
            "node_releases": node_status,
            "occupied_nodes": list(p.occupied_nodes),
            "released_node_count": len(p.released_nodes),
        }

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/manifest")
    def get_manifest():
        return {
            "shards": {
                name: {
                    "voters": list(voters),
                    "majority_needed": manifest.majority_needed(name),
                }
                for name, voters in sorted(manifest.shards.items())
            }
        }

    # ---- 探活报告 ----

    @app.post("/liveness", status_code=201)
    def post_liveness(batch: LivenessBatchIn):
        unknown = sorted({r.node_id for r in batch.reports} - manifest.nodes)
        if unknown:
            raise HTTPException(
                status_code=422,
                detail={"error": "unknown_nodes", "unknown_nodes": unknown},
            )
        store.add_reports(batch.reports)
        return {"accepted": len(batch.reports)}

    @app.get("/liveness")
    def get_all_liveness():
        latest = store.latest_reports()
        return {
            "nodes": {
                n: {
                    "sampled_at": to_iso(r.sampled_at),
                    "healthy": r.healthy,
                    "boot_id": r.boot_id,
                }
                for n, r in sorted(latest.items())
            },
            "nodes_without_reports": sorted(manifest.nodes - latest.keys()),
        }

    @app.get("/liveness/{node_id}")
    def get_node_liveness(node_id: str):
        rep = store.latest_reports().get(node_id)
        if rep is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "no_liveness_report", "node": node_id},
            )
        return {
            "node_id": node_id,
            "sampled_at": to_iso(rep.sampled_at),
            "healthy": rep.healthy,
            "boot_id": rep.boot_id,
        }

    # ---- 许可 ----

    @app.post("/permits", status_code=201)
    def request_permit(req: PermitRequestIn, response: Response):
        nodes = sorted(set(req.nodes))  # 规范化：去重排序
        unknown = [n for n in nodes if n not in manifest.nodes]
        if unknown:
            raise HTTPException(
                status_code=422,
                detail={"error": "unknown_nodes", "unknown_nodes": unknown},
            )
        with store.transaction():  # 并发申请在此串行，守住多数票
            existing = store.get_permit(req.request_id)
            if existing is not None:
                if list(existing.nodes) == nodes:
                    # 同号同批重发 -> 返回原许可
                    response.status_code = 200
                    return permit_view(existing)
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "request_id_conflict",
                        "message": "request_id 已存在且节点集合不同",
                        "existing_nodes": list(existing.nodes),
                    },
                )
            latest = store.latest_reports()
            ok, detail = evaluate_permit_request(
                manifest, nodes, latest, store.open_permits()
            )
            if not ok:
                raise HTTPException(status_code=409, detail=detail)
            record = PermitRecord(
                request_id=req.request_id,
                nodes=tuple(nodes),
                status="open",
                approved_at=now_utc(),
                finished_at=None,
                boot_ids_at_approval={n: latest[n].boot_id for n in nodes},
            )
            store.insert_permit(record)
            return permit_view(record)

    @app.get("/permits")
    def list_permits(status: str | None = Query(default=None, pattern="^(open|finished)$")):
        return {"permits": [permit_view(p) for p in store.list_permits(status)]}

    @app.get("/permits/{request_id}")
    def get_permit(request_id: str):
        p = store.get_permit(request_id)
        if p is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "permit_not_found", "request_id": request_id},
            )
        return permit_view(p)

    @app.post("/permits/{request_id}/nodes/{node_id}/complete", status_code=201)
    def complete_node(request_id: str, node_id: str, response: Response):
        """单节点完工：凭申请号和批内节点释放一个已恢复的节点。

        - 该节点最新探活须符合原结单条件（获批后采集、健康、启动标识已变），
          否则 409 且许可状态不变。
        - 重复办理已释放节点：幂等返回原释放时间（200），即使该节点已被
          新许可占用。
        - 最后一个节点释放时整单结单。
        """
        with store.transaction():
            p = store.get_permit(request_id)
            if p is None:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "permit_not_found", "request_id": request_id},
                )
            if node_id not in p.nodes:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "error": "node_not_in_permit",
                        "request_id": request_id,
                        "node": node_id,
                    },
                )
            # 幂等：已释放（含整单结单后）的节点重发，返回原释放时间
            existing_release = store.get_release(request_id, node_id)
            if existing_release is not None:
                response.status_code = 200
                return {
                    "request_id": request_id,
                    "node": node_id,
                    "released": True,
                    "released_at": to_iso(existing_release),
                    "idempotent_replay": True,
                    "permit": permit_view(store.get_permit(request_id)),
                }
            # 条件不满足：不写入任何释放记录，许可状态不变
            reason = evaluate_node_completion(p, node_id, store.latest_reports())
            if reason is not None:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "node_completion_conditions_not_met",
                        "node": node_id,
                        "reason": reason,
                    },
                )
            released_at = now_utc()
            store.insert_release(request_id, node_id, released_at)
            p = store.get_permit(request_id)  # 重新加载，带上新释放记录
            just_finished = False
            # 最后一个仍占用的节点释放 -> 整单结单
            if not p.occupied_nodes:
                store.mark_finished(request_id, released_at)
                just_finished = True
                p = store.get_permit(request_id)
            return {
                "request_id": request_id,
                "node": node_id,
                "released": True,
                "released_at": to_iso(released_at),
                "idempotent_replay": False,
                "permit_finished": just_finished,
                "permit": permit_view(p),
            }

    @app.post("/permits/{request_id}/finish")
    def finish_permit(request_id: str):
        with store.transaction():
            p = store.get_permit(request_id)
            if p is None:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "permit_not_found", "request_id": request_id},
                )
            if p.status == "finished":
                raise HTTPException(
                    status_code=409,
                    detail={"error": "permit_already_finished", "request_id": request_id},
                )
            # 只检查仍被本单占用的节点；全部符合才一起释放
            problems = evaluate_finish(p, store.latest_reports())
            if problems:
                raise HTTPException(
                    status_code=409,
                    detail={"error": "finish_conditions_not_met", "nodes": problems},
                )
            finished_at = now_utc()
            # 补齐所有仍占用节点的释放记录；已单节点完工的保留原释放时间
            remaining = {n: finished_at for n in p.occupied_nodes}
            store.insert_releases_bulk(request_id, remaining)
            store.mark_finished(request_id, finished_at)
            return permit_view(store.get_permit(request_id))

    return app


def create_app_from_env() -> FastAPI:
    return create_app(
        db_path=os.environ.get("DB_PATH", "./data/permits.db"),
        manifest_path=os.environ.get("MANIFEST_PATH", "./manifest.json"),
    )


app = create_app_from_env()
