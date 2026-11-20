#!/usr/bin/env bash
# 端到端演示：单节点完工释放 -> 已释放节点再次被新许可占用。
#
# 用法：
#   ./scripts/demo_node_release.sh [BASE_URL]
# 默认 BASE_URL=http://localhost:8000（docker compose up 后的地址）。
#
# 演示流程（manifest：shard-a=[n1,n2,n3], shard-b=[n2,n3,n4], shard-c=[n3,n4,n5]）：
#   1. 全量上报健康探活（boot-1）
#   2. 申请 req-batch，一次获批重启 n1、n4（分布在不同分片，各扣一票仍过半）
#   3. n1 先重启完成（新 boot-2）-> 单节点完工释放 n1；许可仍 open，n4 继续占用
#   4. 对 n1 重复办理完工 -> 幂等返回原释放时间
#   5. n1 凭释放后的身份再次获批新许可 req-reuse（此前会被占用拒绝）
#   6. n4 重启完成后完工 -> req-batch 最后一个节点释放，整单自动结单
set -euo pipefail

BASE_URL="${1:-http://localhost:8000}"
# 重启后的探活采样时间必须严格晚于获批时间（服务端时间为微秒精度），
# 这里统一在当前时间上加偏移，避免同秒被判定为"获批前采样"。
FUTURE_TS() { date -u -d "+${1:-2} minutes" +"%Y-%m-%dT%H:%M:%SZ"; }

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
post() { curl -sS -w '\n[HTTP %{http_code}]\n' -X POST "$BASE_URL$1" -H 'Content-Type: application/json' -d "$2"; }
get()  { curl -sS -w '\n[HTTP %{http_code}]\n' "$BASE_URL$1"; }

say "0. 健康检查"
get /healthz

say "1. 上报 5 个节点的健康探活（boot-1，采样时间取 2 分钟前）"
T1="$(date -u -d '-2 minutes' +"%Y-%m-%dT%H:%M:%SZ")"
post /liveness "{
  \"reports\": [
    {\"node_id\":\"n1\",\"sampled_at\":\"$T1\",\"healthy\":true,\"boot_id\":\"boot-1\"},
    {\"node_id\":\"n2\",\"sampled_at\":\"$T1\",\"healthy\":true,\"boot_id\":\"boot-1\"},
    {\"node_id\":\"n3\",\"sampled_at\":\"$T1\",\"healthy\":true,\"boot_id\":\"boot-1\"},
    {\"node_id\":\"n4\",\"sampled_at\":\"$T1\",\"healthy\":true,\"boot_id\":\"boot-1\"},
    {\"node_id\":\"n5\",\"sampled_at\":\"$T1\",\"healthy\":true,\"boot_id\":\"boot-1\"}
  ]}"

say "2. 申请 req-batch：一批重启 n1、n4（期望 201）"
post /permits '{"request_id":"req-batch","nodes":["n1","n4"]}'

say "2b. 同号同批重发（期望 200，幂等返回原许可）"
post /permits '{"request_id":"req-batch","nodes":["n4","n1","n1"]}'

say "3. n1 尚未重启就办理完工（期望 409，许可状态不变）"
post /permits/req-batch/nodes/n1/complete '{}' || true

say "3b. n1 重启完成：上报获批后采样、健康、boot-2 的新探活"
T2="$(FUTURE_TS 2)"
post /liveness "{
  \"reports\":[
    {\"node_id\":\"n1\",\"sampled_at\":\"$T2\",\"healthy\":true,\"boot_id\":\"boot-2\"}
  ]}"

say "3c. n1 单节点完工（期望 201，permit_finished=false，n4 仍占用）"
post /permits/req-batch/nodes/n1/complete '{}'

say "4. 重复办理 n1 完工（期望 200，idempotent_replay=true，原释放时间不变）"
post /permits/req-batch/nodes/n1/complete '{}'

say "4b. 查看许可：原节点名单保留，逐节点释放状态可见"
get /permits/req-batch

say "5. n1 已释放 -> 新许可 req-reuse 再次占用 n1（期望 201）"
post /permits '{"request_id":"req-reuse","nodes":["n1"]}'

say "5b. n4 仍被 req-batch 占用 -> 新申请被拒（期望 409 occupied_nodes）"
post /permits '{"request_id":"req-blocked","nodes":["n4"]}' || true

say "6. n4 重启完成：上报新探活（boot-7）并办理单节点完工"
T3="$(FUTURE_TS 4)"
post /liveness "{
  \"reports\":[
    {\"node_id\":\"n4\",\"sampled_at\":\"$T3\",\"healthy\":true,\"boot_id\":\"boot-7\"}
  ]}"
say "6b. n4 完工 -> req-batch 最后一个节点释放，整单自动结单（permit_finished=true）"
post /permits/req-batch/nodes/n4/complete '{}'

say "7. 最终确认：req-batch 已 finished，两个许可各自的节点释放记录"
get /permits/req-batch
get /permits/req-reuse

say "演示完成。可用以下命令清理演示数据（docker 命名卷）："
echo "  docker compose down -v"
