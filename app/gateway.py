"""
单进程影子流量网关 (FastAPI + aiohttp)

- 固定两个本地 GET 报价服务: 主端 PRIMARY_URL / 影子端 SHADOW_URL
- 两端并行调用, 客户端只收到主端的原始状态码和原始字节(逐块透传)
- 影子慢/失败/超时(发起起 2s)只影响影子, 不拖住主响应
- 两端各自流式计算 SHA-256, 两端都完整结束才比较状态码与摘要
- 最多 4 个影子并发, 名额满时跳过影子并注明原因, 主请求照常通过
- 客户端断连同时终止两端; 影子通信实际退出后才归还名额
- 不整份缓存/落盘响应, 每块读取后立即更新哈希并释放

网页熔断保护 (默认启用, BREAKER_ENABLED):
- 按本轮影子调用完成顺序累计连续失败: 网络错误/超时/完整 5xx 计失败,
  完整非 5xx 归零, 内容差异(match/mismatch 的非 5xx)不算故障, 客户端断连不计
- 连续 BREAKER_FAIL_THRESHOLD(默认 3) 次失败 -> 打开熔断, 停发影子
  BREAKER_COOLDOWN_S(默认 5) 秒, 主请求始终照常透传
- 冷却期内跳过影子并记录原因; 冷却到期仅放行一个真实报价请求作为探针,
  其余继续跳过; 探针完整非 5xx 才恢复(计数归零), 失败则再冷却 5 秒
- 探针仍占原 4 个名额: 满额时跳过且不消耗探针资格
- 断连后须等影子通信实际退出才允许下一个请求接替探针
- 用"轮次"(round)隔离: 在途调用可正常收尾, 但熔断前的迟到结果不得改变
  探针或新轮计数; 网页切换开关后旧结果同样不改保护状态
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import resource
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

PRIMARY_URL = os.environ.get("PRIMARY_URL", "http://primary:8001")
SHADOW_URL = os.environ.get("SHADOW_URL", "http://shadow:8002")
MAX_SHADOW_CALLS = int(os.environ.get("MAX_SHADOW_CALLS", "4"))
SHADOW_TIMEOUT_S = float(os.environ.get("SHADOW_TIMEOUT_S", "2.0"))
READ_CHUNK = 64 * 1024
MAX_RECORDS = int(os.environ.get("MAX_RECORDS", "100"))
# 熔断保护参数 (均可由环境变量覆盖)
BREAKER_ENABLED = os.environ.get("BREAKER_ENABLED", "1") not in ("0", "false", "False", "")
BREAKER_FAIL_THRESHOLD = int(os.environ.get("BREAKER_FAIL_THRESHOLD", "3"))
BREAKER_COOLDOWN_S = float(os.environ.get("BREAKER_COOLDOWN_S", "5.0"))

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
    # 长度由实际透传的字节流重新决定, 避免上游声明与透传块边界不一致
    "content-length",
}

# 已定性的终态: finalize 不得覆盖
TERMINAL_RESULTS = {
    "match", "mismatch", "shadow_timeout", "shadow_error",
    "client_disconnected", "primary_error", "primary_incomplete",
    "shadow_skipped_no_slot", "shadow_skipped_breaker_open",
    "shadow_skipped_probe_wait",
}

# 熔断三态
CLOSED, OPEN, PROBE = "closed", "open", "probe"


@dataclass
class ShadowLimiter:
    """影子名额: acquire 到 release 之间覆盖整个影子通信的生命周期。"""
    capacity: int = MAX_SHADOW_CALLS
    in_flight: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def acquire(self) -> bool:
        async with self.lock:
            if self.in_flight >= self.capacity:
                return False
            self.in_flight += 1
            return True

    async def release(self) -> None:
        async with self.lock:
            self.in_flight -= 1


@dataclass
class Dispatch:
    """熔断 + 名额联合裁决结果。"""
    start: bool
    result: Optional[str] = None      # 跳过时写入记录的终态
    detail: Optional[str] = None
    round_id: int = 0
    is_probe: bool = False
    breaker_state: str = CLOSED


class CircuitBreaker:
    """
    影子流量熔断保护 (默认启用)。

    状态:
      closed: 正常放行, 按完成顺序累计连续失败, 达阈值 -> open
      open:   停发影子; 冷却到期 -> probe
      probe:  只放行一个探针; 成功(完整非5xx) -> closed, 失败 -> open 再冷却

    轮次隔离: 每次熔断/恢复/开关切换都递增 round; 在途调用的迟到结果只在
    round 匹配时才允许改变计数与状态。断连(outcome=None)不计失败也不切换。
    """

    def __init__(self, enabled: bool, threshold: int, cooldown_s: float):
        self.enabled = enabled
        self.threshold = threshold
        self.cooldown_s = cooldown_s
        self.state = CLOSED
        self.consecutive_failures = 0
        self.probe_taken = False
        self.opened_at: Optional[float] = None
        self.cooldown_until: Optional[float] = None
        self.round_id = 0
        self.lock = asyncio.Lock()
        # 探针通信在途标记: 断连后须等通信退出才允许下一请求接替
        self.probe_in_flight = False

    def _sync_locked(self) -> None:
        """惰性状态迁移: open 冷却到期 -> probe (无探针在途时才放行)。"""
        if self.state == OPEN and time.monotonic() >= self.cooldown_until:
            self.state = PROBE
            self.probe_taken = False
            self.round_id += 1

    async def dispatch(self, limiter: ShadowLimiter) -> Dispatch:
        """请求入口裁决: 决定本次影子是发起还是跳过 (跳过原因一并返回)。"""
        async with self.lock:
            if not self.enabled:
                # 保护关闭: 不熔断, 仅受名额限制; 旧轮结果不影响保护状态
                acquired = await limiter.acquire()
                if not acquired:
                    return Dispatch(False, "shadow_skipped_no_slot",
                                    f"shadow skipped: {MAX_SHADOW_CALLS} shadow "
                                    "calls already in flight (breaker disabled)",
                                    self.round_id, False, "disabled")
                return Dispatch(True, round_id=self.round_id,
                                breaker_state="disabled")

            self._sync_locked()
            snapshot = self.state

            if self.state == OPEN:
                remain = max(0.0, self.cooldown_until - time.monotonic())
                return Dispatch(
                    False, "shadow_skipped_breaker_open",
                    f"shadow skipped: breaker OPEN after {self.consecutive_failures} "
                    f"consecutive failures, cooldown {remain:.1f}s left; "
                    "primary request still proxied",
                    breaker_state=snapshot)

            if self.state == PROBE:
                # 已有探针在途(含断连但通信未退出): 不得接替
                if self.probe_taken:
                    return Dispatch(
                        False, "shadow_skipped_probe_wait",
                        "shadow skipped: probe already in flight, waiting for it "
                        "to finish before another probe can take over",
                        breaker_state=snapshot)
                # 探针仍占原 4 名额: 满额跳过, 且不消耗探针资格
                acquired = await limiter.acquire()
                if not acquired:
                    return Dispatch(
                        False, "shadow_skipped_no_slot",
                        f"shadow skipped: {MAX_SHADOW_CALLS} shadow calls already "
                        "in flight; probe qualification NOT consumed",
                        breaker_state=snapshot)
                self.probe_taken = True
                self.probe_in_flight = True
                return Dispatch(True, round_id=self.round_id,
                                is_probe=True, breaker_state=snapshot)

            # closed: 常规放行, 受名额限制
            acquired = await limiter.acquire()
            if not acquired:
                return Dispatch(
                    False, "shadow_skipped_no_slot",
                    f"shadow skipped: {MAX_SHADOW_CALLS} shadow calls already in flight",
                    breaker_state=snapshot)
            return Dispatch(True, round_id=self.round_id, breaker_state=snapshot)

    async def on_complete(self, round_id: int, is_probe: bool,
                          outcome: Optional[str]) -> None:
        """
        影子通信实际退出后的收口 (名额已由调用方归还)。
        outcome: "failure" | "success" | None(断连, 中性不计)
        迟到结果(round 不匹配)与保护关闭期间的结果一律不改保护状态。
        """
        async with self.lock:
            if is_probe:
                # 探针通信退出, 释放接替闸门
                self.probe_in_flight = False
            if not self.enabled or round_id != self.round_id:
                return
            if outcome is None:
                # 客户端断连: 不计失败; 探针资格让后续请求可接替
                if is_probe:
                    self.probe_taken = False
                return
            if is_probe:
                if outcome == "success":
                    # 探针完整非5xx: 恢复, 从零计数
                    self.state = CLOSED
                    self.consecutive_failures = 0
                    self.probe_taken = False
                    self.round_id += 1
                else:
                    # 探针失败: 再冷却 5 秒
                    self.state = OPEN
                    self.consecutive_failures = self.threshold
                    self.probe_taken = False
                    self.opened_at = time.monotonic()
                    self.cooldown_until = self.opened_at + self.cooldown_s
                    self.round_id += 1
                return
            if self.state != CLOSED:
                return  # 防御: 非探针事件只在 closed 计数
            if outcome == "success":
                self.consecutive_failures = 0
            else:
                self.consecutive_failures += 1
                if self.consecutive_failures >= self.threshold:
                    self.state = OPEN
                    self.opened_at = time.monotonic()
                    self.cooldown_until = self.opened_at + self.cooldown_s
                    self.round_id += 1

    async def set_enabled(self, enabled: bool) -> None:
        """网页开关: 重新启用即从零计数; 切换后旧结果不改保护状态。"""
        async with self.lock:
            if enabled == self.enabled:
                return
            self.enabled = enabled
            self.state = CLOSED
            self.consecutive_failures = 0
            self.probe_taken = False
            self.probe_in_flight = False
            self.opened_at = None
            self.cooldown_until = None
            self.round_id += 1  # 让在途旧调用的结果全部过期

    async def snapshot(self) -> dict:
        async with self.lock:
            self._sync_locked()
            cooldown_remaining = None
            if self.state == OPEN:
                cooldown_remaining = max(0.0, self.cooldown_until - time.monotonic())
            return {
                "enabled": self.enabled,
                "state": self.state if self.enabled else "disabled",
                "consecutive_failures": self.consecutive_failures,
                "failure_threshold": self.threshold,
                "cooldown_s": self.cooldown_s,
                "cooldown_remaining_s": (round(cooldown_remaining, 2)
                                         if cooldown_remaining is not None else None),
                "probe_in_flight": self.probe_in_flight,
                "round": self.round_id,
            }


@dataclass
class CallContext:
    rid: str
    target: str
    started_at: float
    primary_done: bool = False
    shadow_done: bool = False
    saved: bool = False
    record: Optional[dict] = None
    shadow_task: Optional[asyncio.Task] = None
    breaker_round: Optional[int] = None
    is_probe: bool = False


state: dict = {}
# 后台影子任务的强引用集合: 防止生成器返回后 ctx 被回收导致任务被 GC 销毁,
# 任务完成时通过 done_callback 自行移除
background_tasks: set[asyncio.Task] = set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    connector = aiohttp.TCPConnector(limit=64)
    # 主端不用总超时(由客户端断连控制), 仅设连接级保护
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=5, sock_read=30)
    state["session"] = aiohttp.ClientSession(connector=connector, timeout=timeout)
    state["limiter"] = ShadowLimiter()
    state["breaker"] = CircuitBreaker(
        enabled=BREAKER_ENABLED,
        threshold=BREAKER_FAIL_THRESHOLD,
        cooldown_s=BREAKER_COOLDOWN_S,
    )
    state["records"] = []
    state["records_lock"] = asyncio.Lock()
    try:
        yield
    finally:
        await state["session"].close()


app = FastAPI(title="shadow-traffic-gateway", lifespan=lifespan)


def now_ms() -> int:
    return int(time.time() * 1000)


async def append_record(rec: dict) -> None:
    async with state["records_lock"]:
        recs = state["records"]
        recs.append(rec)
        if len(recs) > MAX_RECORDS:
            del recs[:-MAX_RECORDS]


def filtered_headers(resp: aiohttp.ClientResponse) -> dict[str, str]:
    return {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP}


async def run_shadow(ctx: CallContext) -> None:
    """
    执行一次影子调用 (从发起起整体 2s 硬超时)。
    无论成功/失败/超时/取消, 通信实际退出后才归还影子名额并通知熔断器。
    只更新记录字段, 记录统一由主端生命周期落库, 避免出现半条记录。

    熔断分类 outcome:
      "failure" 网络错误/超时/完整 5xx; "success" 完整非 5xx;
      None 客户端断连(取消), 中性不计。内容差异不影响分类。
    """
    acquired = ctx.record.get("shadow") == "started"
    rec = ctx.record
    outcome: Optional[str] = None
    try:
        session = state["session"]
        try:
            coro = session.get(SHADOW_URL + ctx.target, allow_redirects=False)
            # wait_for 从任务创建(发起)时刻起计时, 超时只取消影子请求
            async with await asyncio.wait_for(coro, timeout=SHADOW_TIMEOUT_S) as resp:
                shadow_status = resp.status
                shadow_hash = hashlib.sha256()
                async for chunk in resp.content.iter_chunked(READ_CHUNK):
                    shadow_hash.update(chunk)  # 流式哈希, 不缓存整份响应
                shadow_digest = shadow_hash.hexdigest()
        except asyncio.TimeoutError:
            outcome = "failure"
            rec["result"] = "shadow_timeout"
            rec["shadow"] = f"timeout after {SHADOW_TIMEOUT_S:g}s"
            return
        except asyncio.CancelledError:
            # 客户端断连或主端失败: 影子随主端一起终止 -> 中性, 不计失败
            outcome = None
            rec["result"] = "client_disconnected"
            rec["shadow"] = "cancelled with primary"
            raise
        except aiohttp.ClientError as exc:
            outcome = "failure"
            rec["result"] = "shadow_error"
            rec["shadow"] = f"connection error: {type(exc).__name__}"
            return
        except Exception as exc:  # 防御: 影子侧任何意外都不能拖垮主流程
            logging.getLogger("shadow").exception("shadow task failed: %r", exc)
            outcome = "failure"
            rec["result"] = "shadow_error"
            rec["shadow"] = f"unexpected error: {type(exc).__name__}: {exc}"
            return

        # 完整结束才分类: 完整 5xx 计失败, 完整非 5xx 计成功(差异不算故障)
        outcome = "failure" if shadow_status >= 500 else "success"
        rec["shadow_status"] = shadow_status
        rec["shadow_sha256"] = shadow_digest
        rec["shadow_ms"] = now_ms() - ctx.started_at
        rec["shadow"] = "completed"

        if not ctx.primary_done:
            # 主端尚未完整结束, 暂时不能比较; 最终结果由 finalize 补算
            rec["result"] = "shadow_completed_primary_incomplete"
            return

        _compare(rec)
    finally:
        ctx.shadow_done = True
        if acquired:
            # 通信实际退出(含超时/取消)后才归还名额
            await state["limiter"].release()
        # 熔断器收口: 迟到结果(round 不匹配)/关闭期间的结果不改保护状态
        await state["breaker"].on_complete(
            ctx.breaker_round, ctx.is_probe, outcome)
        if not ctx.primary_done:
            # 主端未完整(客户端断连/主端失败): 记录由主端生命周期保存
            return
        # 主端已完整: 影子在此收口, 把最终结果写回已落库的记录(原地更新)
        finalize(rec, ctx)
        if not ctx.saved:
            await save_record(ctx)


def _compare(rec: dict) -> None:
    """两端都完整结束后才调用: 比较状态码与 SHA-256 摘要。"""
    same_status = rec["shadow_status"] == rec["primary_status"]
    same_digest = rec["shadow_sha256"] == rec["primary_sha256"]
    if same_status and same_digest:
        rec["result"] = "match"
    else:
        rec["result"] = "mismatch"
        if not same_status:
            rec["detail"] = (
                f"status differs: primary={rec['primary_status']} "
                f"shadow={rec['shadow_status']}"
            )
        else:
            rec["detail"] = "same status but sha256 differs"


def finalize(rec: dict, ctx: CallContext) -> None:
    """主端完整结束后的统一收口: 补算比较结果。"""
    # 终态(断连/超时/失败等)不得被覆盖
    if rec["result"] in TERMINAL_RESULTS:
        return
    if not ctx.shadow_done:
        # 影子仍在跑(受 2s 硬超时保护): 先挂起, 等其退出时再更新,
        # 绝不能在未收完整时报一致
        rec["result"] = "shadow_pending"
        rec["shadow"] = "in flight"
        return
    # 影子已完整且主端也完整 -> 若之前挂着 incomplete 标记则补算
    if rec["result"] in ("shadow_completed_primary_incomplete", "shadow_pending"):
        _compare(rec)
    elif rec["result"] == "pending":
        # 理论上不进入(无影子任务时走 skipped 分支), 兜底
        rec["result"] = "unknown"


async def save_record(ctx: CallContext) -> None:
    """记录只落库一次: 谁先收口谁保存。"""
    if ctx.saved:
        return
    ctx.saved = True
    await append_record(ctx.record)


async def proxy(request: Request, target: str) -> StreamingResponse:
    rid = f"r{now_ms()}-{id(request) % 100000:05d}"
    started = now_ms()
    rec: dict = {
        "id": rid,
        "target": target,
        "started_at": started,
        "result": "pending",
        "primary": "pending",
        "shadow": "not_started",
    }
    ctx = CallContext(rid=rid, target=target, started_at=started, record=rec)

    # 熔断保护 + 影子名额联合裁决(非阻塞); 跳过原因写记录, 但主请求照常通过
    decision = await state["breaker"].dispatch(state["limiter"])
    rec["breaker_state"] = decision.breaker_state
    if decision.is_probe:
        rec["probe"] = True
    if decision.start:
        ctx.breaker_round = decision.round_id
        ctx.is_probe = decision.is_probe
        rec["shadow"] = "started"
        task = asyncio.create_task(run_shadow(ctx))
        ctx.shadow_task = task
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
    else:
        rec["shadow"] = "skipped"
        rec["result"] = decision.result
        rec["detail"] = decision.detail

    session = state["session"]
    try:
        primary_resp = await session.get(PRIMARY_URL + target, allow_redirects=False)
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
        rec["primary"] = f"connection error: {type(exc).__name__}"
        rec["result"] = "primary_error"
        if ctx.shadow_task is not None:
            # 主端连接失败: 终止已发起的影子; 名额由任务自身在退出时归还
            ctx.shadow_task.cancel()
            try:
                await ctx.shadow_task
            except (asyncio.CancelledError, Exception):
                pass
        await save_record(ctx)
        return JSONResponse(
            status_code=502,
            content={"detail": "primary quote service unavailable"},
        )

    async def body_iter():
        primary_hash = hashlib.sha256()

        async def settle_disconnect():
            # 客户端断连统一收口: 终止影子, 等其通信实际退出(名额/探针闸门释放)。
            # uvicorn 断连时会取消外层 ASGI 任务, 因此把清理放入独立后台任务,
            # 脱离外层 cancel scope; 外层仍在运行的路径(流式循环内检测)则直接等待。
            async def _cleanup():
                rec["primary"] = "aborted before completion"
                rec["result"] = "client_disconnected"
                primary_resp.release()
                if ctx.shadow_task is not None and not ctx.shadow_task.done():
                    ctx.shadow_task.cancel()
                if ctx.shadow_task is not None:
                    try:
                        await ctx.shadow_task  # 等影子通信实际退出(名额随之归还)
                    except BaseException:
                        pass
                await save_record(ctx)

            cleanup_task = asyncio.create_task(_cleanup())
            background_tasks.add(cleanup_task)
            cleanup_task.add_done_callback(background_tasks.discard)
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                # 外层已被 uvicorn 取消: 清理在后台继续, 这里让取消正常传播
                raise

        try:
            async for chunk in primary_resp.content.iter_chunked(READ_CHUNK):
                if await request.is_disconnected():
                    raise asyncio.CancelledError
                primary_hash.update(chunk)  # 流式哈希, 不缓存整份响应
                yield chunk
        except asyncio.CancelledError:
            await settle_disconnect()
            return
        except aiohttp.ClientError as exc:
            # 主端传输中途断开: 主响应不完整, 同样终止影子
            rec["primary"] = f"stream error: {type(exc).__name__}"
            rec["result"] = "primary_incomplete"
            primary_resp.release()
            if ctx.shadow_task is not None and not ctx.shadow_task.done():
                ctx.shadow_task.cancel()
            if ctx.shadow_task is not None:
                try:
                    await asyncio.shield(ctx.shadow_task)
                except BaseException:
                    pass
            await save_record(ctx)
            return

        # 主端完整结束: 客户端响应到此立刻收尾, 不等待影子
        primary_resp.release()
        ctx.primary_done = True
        rec["primary_status"] = primary_resp.status
        rec["primary_sha256"] = primary_hash.hexdigest()
        rec["primary_ms"] = now_ms() - started
        rec["primary"] = "completed"

        if ctx.shadow_task is None:
            # 名额满被跳过: 没有影子结果, 不产生一致性结论
            await save_record(ctx)
            return

        # 主端响应到此完整结束并立即返回客户端, 不等待影子
        finalize(rec, ctx)
        await save_record(ctx)  # 影子仍在跑时先落 shadow_pending, 其退出后原地更新

    return StreamingResponse(
        body_iter(),
        status_code=primary_resp.status,
        headers=filtered_headers(primary_resp),
        media_type=primary_resp.headers.get("Content-Type"),
    )


@app.get("/healthz")
async def healthz():
    lim: ShadowLimiter = state["limiter"]
    breaker_snap = await state["breaker"].snapshot()
    return {
        "status": "ok",
        "primary": PRIMARY_URL,
        "shadow": SHADOW_URL,
        "shadow_in_flight": lim.in_flight,
        "shadow_capacity": lim.capacity,
        "shadow_timeout_s": SHADOW_TIMEOUT_S,
        "breaker": breaker_snap,
    }


@app.get("/api/records")
async def get_records():
    async with state["records_lock"]:
        return list(reversed(state["records"]))


@app.get("/api/state")
async def get_state():
    lim: ShadowLimiter = state["limiter"]
    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        "shadow_in_flight": lim.in_flight,
        "shadow_capacity": lim.capacity,
        "records": len(state["records"]),
        "max_rss_mib": round(rss_kb / 1024, 2),
        "breaker": await state["breaker"].snapshot(),
    }


@app.get("/api/breaker")
async def get_breaker():
    return await state["breaker"].snapshot()


@app.post("/api/breaker")
async def set_breaker(request: Request):
    """网页开关熔断保护: {"enabled": true|false}; 重新启用即从零计数。"""
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(status_code=400,
                            content={"detail": "invalid JSON body"})
    if not isinstance(payload, dict) or not isinstance(
            payload.get("enabled"), bool):
        return JSONResponse(
            status_code=400,
            content={"detail": "body must be JSON with boolean 'enabled'"})
    await state["breaker"].set_enabled(payload["enabled"])
    return await state["breaker"].snapshot()


# 报价相关 GET 请求走代理; 网页与管理 API 不拦截
@app.get("/quote")
@app.get("/quote-big")
async def quote_proxy(request: Request):
    target = request.url.path
    if request.url.query:
        target += "?" + request.url.query
    return await proxy(request, target)


app.mount("/", StaticFiles(
    directory=os.path.join(os.path.dirname(__file__), "web"), html=True),
    name="web")
