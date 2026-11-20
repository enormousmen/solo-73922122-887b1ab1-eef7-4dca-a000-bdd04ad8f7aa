# restart-permit-api

存储集群值班员用的**分批重启许可 API**。FastAPI + SQLite，单容器即可运行。

核心规则：

- 服务启动时读取**固定的分片投票副本清单**（`manifest.json`）。
- 节点探活报告携带**采样时间、健康状态、启动标识**；每个节点以**采样时间最新**的一份为准。
- 值班员提交**申请号 + 拟重启的一批节点**，满足以下条件才批准整批：
  1. 本批每个节点最新探活均健康；
  2. 本批节点未被任何**未结束许可**占用；
  3. 扣除 *本批节点 + 未结束许可占用节点 + 健康未知/异常节点* 后，本批涉及的**每个分片**剩余可用投票副本均**过半**；否则拒绝并列出受影响分片。
- **并发申请**在写事务（`BEGIN IMMEDIATE`）中串行判定，守住多数票。
- **同号同批重发**返回原许可（HTTP 200）；同号不同批返回 409。
- 许可与探活记录落盘 SQLite，**服务重启后保留**。
- **结束许可**要求本批每个**仍被占用**节点的最新探活：在获批后采集、健康、启动标识与获批时不同；全部符合才一起释放。
- **单节点完工**：同一张许可内节点恢复有先后，可凭申请号 + 批内节点逐个释放已恢复节点。该节点最新探活须符合原结单条件，否则许可状态不变。已释放节点立即重新参与多数票计算、可被新许可占用；其余节点继续由原许可占用。最后一个节点释放时整单自动结单。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 服务监听 http://localhost:8000 ，交互文档 http://localhost:8000/docs
```

- SQLite 数据在命名卷 `permit-data`（容器内 `/data/permits.db`），重启不丢。
- 分片清单 `./manifest.json` 只读挂载进容器；修改后需 `docker compose restart` 生效。

本地开发（不用 Docker）：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
DB_PATH=./data/permits.db MANIFEST_PATH=./manifest.json \
  uvicorn app.main:app --host 0.0.0.0 --port 8000
```

运行测试：

```bash
pytest tests/ -q
```

## 分片清单格式

```json
{
  "shards": {
    "shard-a": ["n1", "n2", "n3"],
    "shard-b": ["n2", "n3", "n4"],
    "shard-c": ["n3", "n4", "n5"]
  }
}
```

每个分片列出全部投票副本节点；过半线 = `floor(票数/2)+1`（3 票需 2 可用）。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 服务健康检查 |
| GET | `/manifest` | 查看分片清单与各分片过半线 |
| POST | `/liveness` | 批量上报探活（201） |
| GET | `/liveness` | 全部节点最新探活 |
| GET | `/liveness/{node}` | 单节点最新探活 |
| POST | `/permits` | 申请许可（201 新批 / 200 幂等重发 / 409 拒绝） |
| GET | `/permits?status=open\|finished` | 许可列表 |
| GET | `/permits/{request_id}` | 单个许可（含各节点释放状态） |
| POST | `/permits/{request_id}/nodes/{node_id}/complete` | 单节点完工/释放（201 首次 / 200 幂等重发 / 409 条件不满足） |
| POST | `/permits/{request_id}/finish` | 整单结束，仅检查仍占用节点（200 / 409 条件不满足） |

## 调用示例

### 1. 上报探活

```bash
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports": [
      {"node_id":"n1","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n2","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n3","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n4","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n5","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"}
    ]}'
# => {"accepted":5}
```

### 2. 申请许可

```bash
curl -X POST http://localhost:8000/permits \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-001","nodes":["n1"]}'
# 201 =>
# {"request_id":"req-001","nodes":["n1"],"status":"open",
#  "approved_at":"...","finished_at":null,
#  "boot_ids_at_approval":{"n1":"boot-1"}}
```

同号同批重发，返回原许可（200，`approved_at` 不变）；同号不同批返回 409。

### 3. 多数票不足被拒（列出受影响分片）

`req-001` 已占用 n1 时再申请 n2（同属 shard-a，3 票最多扣 1 票）：

```bash
curl -X POST http://localhost:8000/permits \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-002","nodes":["n2"]}'
# 409 =>
# {"detail":{"ok":false,"error":"permit_rejected",
#   "affected_shards":[{"shard":"shard-a","voters":3,"required_available":2,
#     "available":1,"available_nodes":["n3"],
#     "excluded":{"n1":"occupied_by:req-001","n2":"in_batch"}}]}}
```

其它拒绝类别：`health_unknown_nodes`（健康未知）、`unhealthy_nodes`（异常）、
`occupied_nodes`（被未结束许可占用）。清单外节点一律 422。

### 4. 节点重启后上报新探活，结束许可

```bash
# 节点重启完成：采样时间晚于获批时间、健康、启动标识已变
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports":[{"node_id":"n1","sampled_at":"2026-11-19T10:20:00Z",
                "healthy":true,"boot_id":"boot-2"}]}'

curl -X POST http://localhost:8000/permits/req-001/finish
# 200 => {"request_id":"req-001",...,"status":"finished","finished_at":"..."}
```

结束条件不满足时 409 并给出逐节点原因，例如：

```json
{"detail":{"error":"finish_conditions_not_met",
           "nodes":{"n1":"boot_id_unchanged"}}}
```

可能原因：`no_liveness_report`、`not_sampled_after_approval`、`unhealthy`、`boot_id_unchanged`。
许可结束后节点被释放，可被后续申请占用。

### 5. 单节点完工（逐个释放，推荐演示脚本）

一张许可批内节点恢复有先后。值班员可先释放已恢复的节点，继续安排后续维护：

```bash
# 申请两节点一批（n1 属 shard-a，n4 属 shard-b/c，各分片只扣一票仍过半）
curl -X POST http://localhost:8000/permits \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-batch","nodes":["n1","n4"]}'

# n1 重启完成并上报新探活（采样晚于获批、健康、boot_id 已变）后，单节点完工
curl -X POST http://localhost:8000/permits/req-batch/nodes/n1/complete
# 201 =>
# {"request_id":"req-batch","node":"n1","released":true,
#  "released_at":"...","idempotent_replay":false,"permit_finished":false,
#  "permit":{"status":"open",...,"occupied_nodes":["n4"],
#            "released_node_count":1}}
```

行为要点：

- 条件不满足时返回 409，`reason` 与整单结束的逐节点原因相同
  （`no_liveness_report` / `not_sampled_after_approval` / `unhealthy` /
  `boot_id_unchanged`），**不写入释放记录，许可状态不变**。
- 节点不在本许可名单内返回 404（`node_not_in_permit`）；许可不存在返回 404。
- **重复办理已释放节点**返回 200 与**原释放时间**（`idempotent_replay=true`），
  即使该节点已被新许可占用。
- 已释放节点立即重新参与多数票计算，可被新申请占用；其余节点仍由原许可占用，
  新申请触及它们仍会以 `occupied_nodes` 拒绝。
- **最后一个仍占用节点释放时整单自动结单**（响应中 `permit_finished=true`）。
- 原 `/finish` 接口只检查仍被本单占用的节点，全部符合才一起释放；
  已单节点完工的节点保留各自的原释放时间。

许可查询在原节点名单之外新增逐节点释放状态：

```json
{
  "request_id": "req-batch",
  "nodes": ["n1", "n4"],
  "status": "open",
  "node_releases": {
    "n1": {"released": true,  "released_at": "2026-..."},
    "n4": {"released": false, "released_at": null}
  },
  "occupied_nodes": ["n4"],
  "released_node_count": 1
}
```

一键演示（含"节点释放后再次获批"的完整调用，可直接对照期望状态码）：

```bash
./scripts/demo_node_release.sh            # 默认 http://localhost:8000
./scripts/demo_node_release.sh http://host:port
```

## 设计说明

- **幂等**：`request_id` 是许可表主键。节点集合规范化（去重排序）后比较，
  同号同批重放直接返回原许可；同号不同批 409。被拒绝的申请不落库，整改后可用同号重试。
- **并发**：所有写路径（探活上报、发证、单节点完工、整单结束）走单连接 +
  可重入锁 + `BEGIN IMMEDIATE` 事务，"检查多数票 → 写入许可/释放记录"是原子的，
  并发请求按实际拿到写锁的顺序串行判定，不会超批、不会重复释放。
- **持久化**：许可、节点释放记录与全部探活报告存 SQLite（WAL 模式），
  最新探活按 `max(sampled_at)` 实时计算，服务重启后状态完整恢复、可续办。
- **结束判定**：批准时快照各节点 `boot_id`；单节点完工与整单结束共用同一判定
  ——最新报告 `sampled_at > approved_at` 且健康且 `boot_id` 不同。
- **节点释放**：新增 `node_releases` 表（`permit_request_id, node_id` 联合主键）。
  新申请判定占用时只统计各 open 许可的**未释放**节点；已释放节点重新计票。
  升级前已存在的未结许可在该表无记录，自动视为尚未释放任何节点，无需手工迁移。

## 已知限制 / 未完成项

- 单 worker 运行（SQLite 单写者）；如需多副本需换外部数据库。
- 未做认证鉴权，假定部署在受信内网；如需可前置网关或加 token。
- 探活无"过期"概念：只要最新一份健康即视为健康，不判断采样时间新旧。
- 结束许可比较的是节点上报的采样时钟与服务端获批时钟，依赖各节点时钟基本同步（NTP）。
- 许可无过期/自动回收机制；值班员需显式 finish。
