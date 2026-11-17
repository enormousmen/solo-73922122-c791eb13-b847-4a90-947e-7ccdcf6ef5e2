"""
熔断状态机单元测试 (不起 HTTP 服务, 直接驱动 CircuitBreaker)。

运行: PYTHONPATH=.pydeps python -I tests/test_breaker_unit.py
"""
import asyncio
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
# -I 隔离模式忽略 PYTHONPATH: 本地无 venv 时依赖装在 .pydeps
_DEPS = os.path.join(_ROOT, ".pydeps")
if os.path.isdir(_DEPS):
    sys.path.insert(0, _DEPS)

from app.gateway import CircuitBreaker, ShadowLimiter, CLOSED, OPEN, PROBE  # noqa: E402

PASS, FAIL = [], []


def check(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


async def main():
    async def complete(breaker, limiter, outcome, *, is_probe_override=None):
        """发起->收口的测试辅助: 模拟 run_shadow 退出时归还名额并回调熔断器。"""
        d = await breaker.dispatch(limiter)
        if not d.start:
            return d
        await limiter.release()  # 生产中 run_shadow finally 归还名额
        is_probe = d.is_probe if is_probe_override is None else is_probe_override
        await breaker.on_complete(d.round_id, is_probe, outcome)
        return d

    # 1. 连续 3 次失败打开熔断, 成功归零; 中性(断连)不计
    b = CircuitBreaker(enabled=True, threshold=3, cooldown_s=0.2)
    lim = ShadowLimiter(capacity=4)
    d1 = await complete(b, lim, "failure")
    await complete(b, lim, "success")  # 归零
    snap = await b.snapshot()
    check("1 完整非5xx后失败计数归零", snap["consecutive_failures"] == 0,
          str(snap["consecutive_failures"]))
    await complete(b, lim, "failure")
    await complete(b, lim, "failure")
    check("1 两次失败仍 CLOSED", (await b.snapshot())["state"] == CLOSED)
    await complete(b, lim, "failure")  # 第 3 次
    snap = await b.snapshot()
    check("1 连续3次失败 -> OPEN", snap["state"] == OPEN, snap["state"])

    # 2. OPEN 期间全部跳过且不占名额
    in_flight_before = lim.in_flight
    d = await b.dispatch(lim)
    check("2 OPEN 期间跳过影子", not d.start and
          d.result == "shadow_skipped_breaker_open", str(d.result))
    check("2 跳过不占名额", lim.in_flight == in_flight_before)

    # 3. 冷却到期 -> PROBE, 仅一个探针; 其余跳过; 满额不占探针资格
    await asyncio.sleep(0.25)
    lim2 = ShadowLimiter(capacity=4)
    for _ in range(4):  # 占满 4 名额
        ok = await lim2.acquire()
        assert ok
    dp = await b.dispatch(lim2)
    check("3 名额满时探针跳过且不占资格",
          (not dp.start) and dp.result == "shadow_skipped_no_slot",
          str(dp.result))
    snap = await b.snapshot()
    check("3 探针资格未被消耗", snap["state"] == PROBE and not snap["probe_in_flight"])
    for _ in range(4):  # 归还名额
        await lim2.release()
    dp = await b.dispatch(lim2)
    check("3 有名额后放行探针", dp.start and dp.is_probe, str(dp))
    d_other = await b.dispatch(lim2)
    check("3 探针在途其余请求跳过",
          not d_other.start and d_other.result == "shadow_skipped_probe_wait",
          str(d_other.result))

    # 4. 探针成功 -> CLOSED, 计数从零
    await lim2.release()
    await b.on_complete(dp.round_id, True, "success")
    snap = await b.snapshot()
    check("4 探针成功恢复 CLOSED 且计数归零",
          snap["state"] == CLOSED and snap["consecutive_failures"] == 0,
          str(snap))

    # 5. 探针失败 -> 再冷却; 探针断连(中性)可被下一请求接替
    b2 = CircuitBreaker(enabled=True, threshold=3, cooldown_s=0.15)
    lim3 = ShadowLimiter(capacity=4)
    for _ in range(3):
        await complete(b2, lim3, "failure")
    check("5 已 OPEN", (await b2.snapshot())["state"] == OPEN)
    await asyncio.sleep(0.2)
    dp = await b2.dispatch(lim3)
    check("5 冷却后进入 PROBE", dp.is_probe and (await b2.snapshot())["state"] == PROBE)
    await lim3.release()
    await b2.on_complete(dp.round_id, True, None)  # 探针断连, 中性
    snap = await b2.snapshot()
    check("5 探针断连不计失败且允许接替",
          snap["state"] == PROBE and not snap["probe_in_flight"], str(snap))
    dp2 = await b2.dispatch(lim3)
    check("5 下一请求可接替探针", dp2.is_probe, str(dp2))
    await lim3.release()
    await b2.on_complete(dp2.round_id, True, "failure")
    snap = await b2.snapshot()
    check("5 探针失败重新 OPEN 冷却", snap["state"] == OPEN, str(snap))

    # 6. 轮次隔离: 熔断前在途调用的迟到结果不改新轮/探针计数
    b3 = CircuitBreaker(enabled=True, threshold=3, cooldown_s=0.1)
    lim4 = ShadowLimiter(capacity=4)
    d_inflight = await b3.dispatch(lim4)       # 在途(旧轮), 暂不收口
    for _ in range(3):
        await complete(b3, lim4, "failure")
    check("6 OPEN(旧轮调用仍在途)", (await b3.snapshot())["state"] == OPEN)
    await asyncio.sleep(0.15)
    dp = await b3.dispatch(lim4)               # 新轮探针
    check("6 冷却后探针放行", dp.is_probe)
    # 旧轮迟到失败结果: round 不匹配, 必须被忽略
    await lim4.release()
    await b3.on_complete(d_inflight.round_id, d_inflight.is_probe, "failure")
    snap = await b3.snapshot()
    check("6 熔断前迟到失败不改探针状态",
          snap["state"] == PROBE and snap["probe_in_flight"], str(snap))
    await lim4.release()
    await b3.on_complete(dp.round_id, True, "success")
    check("6 探针成功后恢复", (await b3.snapshot())["state"] == CLOSED)

    # 7. 关闭后不熔断; 重新启用从零计数且旧结果失效
    b4 = CircuitBreaker(enabled=True, threshold=3, cooldown_s=0.1)
    lim5 = ShadowLimiter(capacity=4)
    await complete(b4, lim5, "failure")
    await complete(b4, lim5, "failure")
    await b4.set_enabled(False)
    snap = await b4.snapshot()
    check("7 关闭后状态 disabled 且计数清零",
          snap["state"] == "disabled" and snap["consecutive_failures"] == 0)
    d = await b4.dispatch(lim5)
    check("7 关闭期间影子照常发起", d.start and d.breaker_state == "disabled")
    await lim5.release()
    await b4.set_enabled(True)
    await b4.on_complete(d.round_id, d.is_probe, "failure")  # 旧轮迟到
    snap = await b4.snapshot()
    check("7 重新启用从零计数, 旧结果不改状态",
          snap["state"] == CLOSED and snap["consecutive_failures"] == 0,
          str(snap))

    print(f"\n汇总: {len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("失败项:", FAIL)
        sys.exit(1)


asyncio.run(main())
