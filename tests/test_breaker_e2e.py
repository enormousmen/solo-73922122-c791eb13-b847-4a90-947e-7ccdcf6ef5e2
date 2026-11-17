"""
熔断保护端到端测试: 真实 HTTP 压测网关 + 仅影子端故障注入。

前置: 三服务已按 scripts/run_local.sh 启动。
运行: python -I tests/test_breaker_e2e.py http://127.0.0.1:8000
"""
import asyncio
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEPS = os.path.join(_ROOT, ".pydeps")
if os.path.isdir(_DEPS):
    sys.path.insert(0, _DEPS)

import aiohttp

GW = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
PASS, FAIL = [], []
TERMINAL_SKIP = {"shadow_skipped_breaker_open", "shadow_skipped_probe_wait",
                 "shadow_skipped_no_slot"}


def check(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


async def set_breaker(s, enabled):
    async with s.post(f"{GW}/api/breaker",
                      json={"enabled": enabled}) as r:
        return await r.json()


async def get_breaker(s):
    async with s.get(f"{GW}/api/breaker") as r:
        return await r.json()


async def wait_state(s, state, timeout=8.0):
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        last = await get_breaker(s)
        if last["state"] == state:
            return last
        await asyncio.sleep(0.1)
    raise AssertionError(f"breaker never reached {state}, last={last}")


async def call(s, marker, extra=""):
    qs = f"marker={marker}" + (f"&{extra}" if extra else "")
    url = f"{GW}/quote?{qs}"
    async with s.get(url) as r:
        body = await r.read()
    return r.status, body


async def find_records(s, marker, timeout=6.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        async with s.get(f"{GW}/api/records") as r:
            recs = await r.json()
        hits = [x for x in recs if marker in x["target"]]
        if hits:
            return hits
        await asyncio.sleep(0.1)
    raise AssertionError(f"record with {marker} not found")


async def main():
    run_id = f"brk{int(time.time() * 1000) % 1000000}"
    async with aiohttp.ClientSession() as s:
        # 干净起点: 重新启用 -> 从零计数
        await set_breaker(s, False)
        snap = await set_breaker(s, True)
        check("0 默认/重置后 closed 零计数",
              snap["state"] == "closed" and snap["consecutive_failures"] == 0,
              str(snap))

        # 1. 内容差异不算故障: diff=1 是完整 200, 计数保持 0
        await call(s, f"{run_id}-t1", "diff=1")
        await asyncio.sleep(0.3)
        snap = await get_breaker(s)
        check("1 内容差异(mismatch)不计失败", snap["consecutive_failures"] == 0,
              str(snap["consecutive_failures"]))

        # 2. 仅影子端 500: 主端照常 200; 连续 3 次后熔断打开
        statuses = []
        for i in range(3):
            st, _ = await call(s, f"{run_id}-t2-{i}", "s_fail=1")
            statuses.append(st)
        check("2 熔断过程中主请求始终200", all(x == 200 for x in statuses), str(statuses))
        snap = await wait_state(s, "open")
        check("2 连续3次影子500 -> OPEN", snap["consecutive_failures"] == 3, str(snap))

        # 3. OPEN 期间请求: 影子跳过并注明原因, 主端仍 200
        st, _ = await call(s, f"{run_id}-t3", "s_fail=1")
        check("3 OPEN 期主请求仍200", st == 200)
        recs = await find_records(s, f"{run_id}-t3")
        rec = recs[0]
        check("3 跳过原因=熔断打开",
              rec["result"] == "shadow_skipped_breaker_open" and "cooldown" in rec["detail"],
              rec.get("detail", ""))
        check("3 跳过时影子未发起", rec["shadow"] == "skipped")

        # 4. 冷却 5s 到期 -> probe; 仅一个探针, 并发其余被跳过
        await wait_state(s, "probe")
        # 同时发 3 个请求: 只有 1 个能成为探针
        results = await asyncio.gather(*(
            call(s, f"{run_id}-t4-{i}", "s_fail=1") for i in range(3)))
        check("4 探针窗口主请求全部200",
              all(st == 200 for st, _ in results), str(results))
        await asyncio.sleep(0.3)
        probes, waits = [], []
        for i in range(3):
            hits = await find_records(s, f"{run_id}-t4-{i}")
            rec = hits[0]
            if rec.get("probe"):
                probes.append(rec)
            if rec["result"] == "shadow_skipped_probe_wait":
                waits.append(rec)
        check("4 仅一个请求成为探针", len(probes) == 1, f"probes={len(probes)}")
        check("4 其余请求跳过并注明等待探针", len(waits) == 2, f"waits={len(waits)}")

        # 5. 探针失败(影子仍 500) -> 再冷却 5s
        snap = await wait_state(s, "open")
        check("5 探针失败重新 OPEN", snap["state"] == "open", str(snap))
        rec = probes[0]
        check("5 探针记录带探针标记且影子为500",
              rec.get("probe") is True and rec.get("shadow_status") == 500,
              str({k: rec.get(k) for k in ("probe", "shadow_status", "result")}))

        # 6. 冷却后探针成功(去掉故障参数, 影子完整 200) -> 恢复 closed
        await wait_state(s, "probe")
        st, _ = await call(s, f"{run_id}-t6")
        check("6 探针请求主端200", st == 200)
        snap = await wait_state(s, "closed")
        check("6 探针成功恢复且计数归零",
              snap["consecutive_failures"] == 0, str(snap))
        recs = await find_records(s, f"{run_id}-t6")
        check("6 探针记录为 match",
              recs[0].get("probe") is True and recs[0]["result"] == "match",
              str(recs[0].get("result")))

        # 7. 网络错误(s_reset 影子发首块后断连)也算失败并可累计到熔断
        await set_breaker(s, False)
        await set_breaker(s, True)
        for i in range(3):
            await call(s, f"{run_id}-t7-{i}", "s_reset=1")
        snap = await wait_state(s, "open")
        check("7 影子连接错误累计3次 -> OPEN", snap["state"] == "open", str(snap))
        recs = await find_records(s, f"{run_id}-t7-2")
        check("7 网络错误归类 shadow_error",
              recs[0]["result"] == "shadow_error", recs[0]["result"])

        # 8. 关闭保护: OPEN 状态下关闭后影子立即恢复发起; 再启用从零计数
        st, _ = await call(s, f"{run_id}-t8a")
        recs = await find_records(s, f"{run_id}-t8a")
        check("8a OPEN时影子被跳过",
              recs[0]["result"] == "shadow_skipped_breaker_open")
        snap = await set_breaker(s, False)
        check("8a 关闭后状态 disabled", snap["state"] == "disabled")
        await call(s, f"{run_id}-t8b", "s_fail=1")
        await asyncio.sleep(0.3)
        recs = await find_records(s, f"{run_id}-t8b")
        check("8a 关闭后影子照常发起(不受熔断)",
              recs[0]["shadow"] == "completed" and recs[0]["shadow_status"] == 500,
              str(recs[0]["shadow"]))
        snap = await set_breaker(s, True)
        check("8b 重新启用从零计数 closed",
              snap["state"] == "closed" and snap["consecutive_failures"] == 0)

        # 9. 超时也算失败: s_delay=3 触发 2s 硬超时, 三次后 OPEN
        await set_breaker(s, False)
        await set_breaker(s, True)
        for i in range(3):
            await call(s, f"{run_id}-t9-{i}", "s_delay=3")
        snap = await wait_state(s, "open", timeout=10.0)
        check("9 影子超时累计3次 -> OPEN", snap["state"] == "open", str(snap))

    print(f"\n汇总: {len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("失败项:", FAIL)
        sys.exit(1)


asyncio.run(main())
