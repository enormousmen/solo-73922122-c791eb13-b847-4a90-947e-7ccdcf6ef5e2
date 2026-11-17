"""
熔断"轮次隔离"端到端测试。

验证: 熔断打开前已在途的影子调用, 其迟到结果(失败)不得改变探针或新轮计数;
在途调用可正常收尾, 但 round 不匹配的结果被熔断器忽略。
运行: python -I tests/test_late_result.py http://127.0.0.1:8000
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
RUN = f"late{int(time.time() * 1000) % 1000000}"


async def main():
    async with aiohttp.ClientSession() as s:
        await s.post(f"{GW}/api/breaker", json={"enabled": False})
        await s.post(f"{GW}/api/breaker", json={"enabled": True})

        async def get(tag, extra=""):
            qs = f"marker={RUN}-{tag}" + (f"&{extra}" if extra else "")
            async with s.get(f"{GW}/quote?{qs}") as r:
                await r.read()
                return r.status

        # 慢失败在途 (1.5s 后影子才返回 500)
        slow = asyncio.create_task(get("slow", "s_fail=1&s_delay=1.5"))
        await asyncio.sleep(0.2)
        for i in range(3):
            await get(f"fast-{i}", "s_fail=1")
        b = await (await s.get(f"{GW}/api/breaker")).json()
        assert b["state"] == "open"
        await asyncio.sleep(5.2)
        b = await (await s.get(f"{GW}/api/breaker")).json()
        assert b["state"] == "probe" and not b["probe_in_flight"]
        await slow  # 旧轮迟到 500 结果到达
        await asyncio.sleep(0.3)
        b = await (await s.get(f"{GW}/api/breaker")).json()
        assert b["state"] == "probe" and not b["probe_in_flight"], b
        print("[OK] 熔断前迟到失败未改变探针状态:", b["state"], "round", b["round"])

        # 慢探针 (1s 成功) 以观察在途
        probe = asyncio.create_task(get("probe", "s_delay=1"))
        await asyncio.sleep(0.3)
        b = await (await s.get(f"{GW}/api/breaker")).json()
        assert b["state"] == "probe" and b["probe_in_flight"] is True, b
        print("[OK] 下一请求成为探针且在途: probe_in_flight", b["probe_in_flight"])

        # 探针在途时, 其他请求必须跳过
        await get("other")
        await asyncio.sleep(0.1)
        recs = await (await s.get(f"{GW}/api/records")).json()
        other = [r for r in recs if f"marker={RUN}-other" in r["target"]][0]
        assert other["result"] == "shadow_skipped_probe_wait", other["result"]
        print("[OK] 探针在途其余跳过:", other["result"])

        await probe
        for _ in range(20):
            b = await (await s.get(f"{GW}/api/breaker")).json()
            if b["state"] == "closed":
                break
            await asyncio.sleep(0.2)
        assert b["state"] == "closed" and b["consecutive_failures"] == 0
        print("[OK] 探针成功恢复 closed, 计数归零")
        print("ALL LATE-RESULT CHECKS PASSED")


asyncio.run(main())
