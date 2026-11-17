# 单进程影子流量网关（FastAPI + aiohttp）

新版报价接口上线前的旁路比对网关：固定并行调用**主端**与**影子端**两个本地 GET 报价服务，
客户端只收到主端的原始状态码与原始字节；影子端的慢、失败、超时完全不影响主响应。

## 架构

```
浏览器 (web 控制台)
      │  GET /quote, /quote-big
      ▼
┌──────────────── gateway (FastAPI, 单进程, mem_limit 96m) ───────────────┐
│  名额信号量(最多4)  并行 aiohttp 调用  流式 SHA-256  比对记录(内存环形) │
└───────┬───────────────────────────────────────────┬────────────────────┘
        ▼                                           ▼
   primary:8000 (主报价服务)                  shadow:8000 (影子报价服务)
   响应原样透传给客户端                         只做摘要/状态码比对
```

- 两端使用同一镜像 `shadow-gateway:latest`，以 `SERVICE_ROLE=primary|shadow` 区分。
- 网关只代理 `GET /quote` 与 `GET /quote-big`；网页与 `/api/*` 管理接口不拦截。

## 关键语义（与需求逐条对应）

| 需求 | 实现位置 |
| --- | --- |
| 两端并行调用，客户端只收主端原状态码、原字节 | `app/gateway.py` `proxy()` / `body_iter()` 逐块透传 |
| 影子慢/失败/超时不拖住主响应 | 影子为独立后台任务；主端流结束即返回客户端，`wait_for(2s)` 只取消影子 |
| 两端流式 SHA-256，完整结束才比较 | 每块 `hash.update()` 后立即释放；`_compare()` 仅在两端 `completed` 时调用 |
| 未收完整不能报一致 | 中间态 `shadow_pending` / 断连 / 超时 / 主端不完整均不会产生 `match` |
| 最多 4 个影子调用，满则跳过并注明原因 | `ShadowLimiter`（acquire/release 覆盖整个影子通信生命周期） |
| 影子发起起 2 秒超时，只终止影子 | `asyncio.wait_for(coro, timeout=2.0)` 包住完整影子请求 |
| 客户端断连终止两端 | `request.is_disconnected()` 检测后取消影子任务并释放主端连接 |
| 通信实际退出后才归还名额 | 名额在 `run_shadow` 的 `finally` 中释放 |
| 不整份缓存/落盘，内存限 96MiB | 仅保存 64KiB 块与哈希状态；compose `mem_limit: 96m`，实测峰值 < 60MiB |

## 网页熔断保护（默认启用）

新版接口持续故障时暂停影子流量，避免反复压垮待上线服务；主请求始终照常透传，不受熔断影响。

### 状态机（`CircuitBreaker`，`app/gateway.py`）

```
 CLOSED                连续 3 次影子失败
 ●───────────────►   OPEN (停发影子, 冷却 5s, 期间请求全部跳过并注明原因)
  ▲                    │ 冷却到期
  │ 探针完整非 5xx      ▼
  └─────────────────  PROBE (仅放行 1 个真实报价请求作探针, 其余跳过)
                       │ 探针失败(网络错误/超时/完整5xx)
                       └─► OPEN 再冷却 5s
```

### 计数口径（按本轮影子调用**完成顺序**累计连续失败）

| 影子结果 | 是否计失败 |
| --- | --- |
| 网络错误（连接拒绝 / 连接重置等 `aiohttp.ClientError`） | 计 1 次失败 |
| 影子 2s 超时 | 计 1 次失败 |
| 完整收到的 5xx 响应 | 计 1 次失败 |
| 完整收到的非 5xx 响应（含 200/3xx/4xx） | 失败计数**归零** |
| 内容差异（`diff=1`，状态码或 SHA-256 不同但非 5xx） | **不算故障**，归零 |
| 客户端断连（影子随主端取消） | **不计**，中性 |

### 关键语义

- **主请求隔离**：熔断只决定影子是否发起，主端调用与逐块透传完全不受影响。
- **探针唯一**：冷却到期仅一个真实报价请求可作探针；探针在途时其余请求跳过，
  结果记 `shadow_skipped_probe_wait` 并注明原因。
- **探针占名额**：探针仍占用原来的 4 个影子名额；名额满时跳过且**不消耗探针资格**，
  名额释放后下一个请求才能成为探针。
- **断连接替**：探针遭遇客户端断连时不计失败，须等影子通信实际退出（名额/探针闸门释放）
  后，下一个请求才能接替探针。
- **轮次隔离**：每次熔断/恢复/开关切换都递增 `round`。在途调用可正常收尾，
  但熔断前发起的迟到结果因 `round` 不匹配而被忽略，不得改变探针或新轮计数。
- **网页开关**：可关闭、重新启用保护；重新启用即从零计数（CLOSED），
  切换后在途旧调用的结果不改保护状态。
- **可调参数**（环境变量）：`BREAKER_ENABLED`（默认 1 启用）、
  `BREAKER_FAIL_THRESHOLD`（默认 3）、`BREAKER_COOLDOWN_S`（默认 5）。

## 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 网页控制台:  http://localhost:8000/
# 网关健康检查: http://localhost:8000/healthz
```

### 本地直接运行（无 Docker）

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
bash scripts/run_local.sh
```

## 报价服务的可控参数（query string 会被网关原样转发）

| 参数 | 作用 |
| --- | --- |
| `delay=<秒>` | 两端首字节前共同延迟 |
| `p_delay` / `s_delay` | 仅主端 / 仅影子端延迟（验证超时隔离、熔断延迟演示） |
| `diff=1` | 仅影子端制造差异（JSON 改价 / 大响应改首字节），**不算熔断故障** |
| `status=<码>` | 自定义 HTTP 状态码（验证原状态码透传） |
| `fail=1` | 两端返回相同的 500 |
| `s_fail=1` | **仅影子端**返回 500，主端保持正常（熔断 5xx 演示） |
| `s_reset=1` | **仅影子端**发出首块后断开 TCP（模拟网络错误，熔断演示） |
| `s_status=<码>` | 仅影子端自定义状态码（配合 `diff=1`） |
| `size=<字节>` | `/quote-big` 响应大小，默认 33554432（32MiB） |
| `chunk_ms=<毫秒>` | 大响应每块间隔（验证流式与客户端断连） |

## 网页控制台

- 左上方"熔断保护"卡片：实时显示保护状态（正常 CLOSED / 熔断打开 OPEN / 探针观察 PROBE /
  已关闭）、连续失败计数、冷却剩余秒数、探针在途情况，并提供**启用/关闭开关**。
- 左侧表单可设置延迟、并发数（最多 20）、差异/失败/仅影子端故障场景并**并发发起**。
- 右上"主端响应（逐块）"按请求分 Tab，展示每块序号、字节数与内容预览及累计字节。
- 右下"比对记录"每 1.5s 自动刷新：结果药丸（一致/不一致/影子超时/影子失败/名额满跳过/
  熔断打开跳过/等待探针跳过/断连…）、探针标记、两端状态码、耗时、SHA-256 摘要与跳过原因。

## 管理 API

- `GET /healthz`：上下游地址、影子在途数、容量、超时配置与熔断器快照。
- `GET /api/records`：比对记录（最新在前，内存环形缓冲，默认保留 100 条）。
- `GET /api/state`：影子在途数、记录数、网关峰值 RSS、熔断器快照。
- `GET /api/breaker`：熔断器状态（`enabled` / `state` / `consecutive_failures` /
  `cooldown_remaining_s` / `probe_in_flight` / `round` 等）。
- `POST /api/breaker`：body `{"enabled": true|false}`，开关熔断保护；
  重新启用即从零计数。非法 body 返回 400。

## 自测

```bash
# 先按 scripts/run_local.sh 启动三服务, 然后:
python -I tests/selftest.py http://127.0.0.1:8000
python -I tests/test_breaker_unit.py                       # 熔断状态机单元测试(不起 HTTP)
python -I tests/test_breaker_e2e.py http://127.0.0.1:8000  # 熔断端到端(真实 HTTP)
python -I tests/test_late_result.py http://127.0.0.1:8000  # 轮次隔离/迟到结果
python -I tests/test_probe_disconnect.py http://127.0.0.1:8000  # 探针断连接替
```

- `selftest.py`：一致/不一致比对、主端字节与状态码原样透传、影子 2s 超时隔离、4 名额满跳过
  且注明原因、名额归还、32MiB 流式完整性与摘要、96MiB 内存约束、客户端断连双端终止。
- `test_breaker_unit.py`：三态迁移、失败计数口径、探针唯一/满额不占资格、断连中性、
  探针失败再冷却、轮次隔离、开关重置（22 项）。
- `test_breaker_e2e.py`：经真实 HTTP 验证内容差异不计故障、3 连失败熔断、冷却跳过原因、
  探针唯一与接替、5xx/网络错误/超时三类失败累计、开关行为（22 项）。
- `test_late_result.py`：熔断前在途调用的迟到失败不改探针/新轮计数。
- `test_probe_disconnect.py`：探针在途跳过、断连不计失败且等通信退出才放行接替、
  接替探针成功恢复。
