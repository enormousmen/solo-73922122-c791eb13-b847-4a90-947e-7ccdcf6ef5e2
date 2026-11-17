"""
熔断探针断连接替端到端测试。

验证: 探针在途时其他请求跳过等待; 探针遭遇客户端断连(不计失败)后,
须等影子通信实际退出才释放闸门, 由下一请求接替; 接替探针成功后恢复。
运行: python -I tests/test_probe_disconnect.py http://127.0.0.1:8000
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
RUN = f"z{int(time.time() * 1000) % 1000000}"

async def main():
    base = aiohttp.ClientSession()
    async def get(tag, extra=""):
        async with aiohttp.ClientSession() as s:
            qs = f"marker={RUN}-{tag}" + (f"&{extra}" if extra else "")
            async with s.get(f"{GW}/quote?{qs}") as r:
                await r.read(); return r.status
    async def breaker():
        async with base.get(f"{GW}/api/breaker") as r:
            return await r.json()
    await base.post(f"{GW}/api/breaker", json={"enabled": False})
    await base.post(f"{GW}/api/breaker", json={"enabled": True})
    for i in range(3):
        await get(f"fast-{i}", "s_fail=1")
    assert (await breaker())["state"] == "open"
    print("[OK] 3 连失败 -> OPEN")
    await asyncio.sleep(5.2)
    assert (await breaker())["state"] == "probe"

    # 探针: 主端慢 1.5s 才响应, 客户端在等待首块期间(连接保持)就 abort。
    # 这样从派发到断连有 ~1s 稳定在途窗口, 且影子(无延迟)随主端取消。
    async def disc_probe():
        conn = aiohttp.TCPConnector(force_close=True)
        s = aiohttp.ClientSession(connector=conn)
        try:
            url = f"{GW}/quote?marker={RUN}-probe&s_delay=2&p_delay=1.5"
            task = asyncio.create_task(s.get(url))
            await asyncio.sleep(0.4)  # 请求已发出, 主端首字节前连接保持
            await conn.close()        # 硬关闭所有底层 transport -> 服务端感知断连
            try:
                await task
            except aiohttp.ClientError:
                pass  # 硬断连的预期客户端侧异常
        finally:
            await s.close()
    t = asyncio.create_task(disc_probe())
    saw = False
    for _ in range(40):
        await asyncio.sleep(0.02)
        if (await breaker())["probe_in_flight"]:
            saw = True; break
    assert saw, "未观测到探针在途"
    print("[OK] 探针在途 probe_in_flight=True")

    await asyncio.gather(get("w1"), get("w2"))
    async with base.get(f"{GW}/api/records") as r:
        recs = await r.json()
    waits = [x for x in recs if f"marker={RUN}-w" in x["target"]]
    assert len(waits) == 2 and all(
        x["result"] == "shadow_skipped_probe_wait" for x in waits), str([x["result"] for x in waits])
    print("[OK] 探针在途期间 2 请求跳过等待")

    await t
    for _ in range(40):
        b = await breaker()
        if not b["probe_in_flight"]: break
        await asyncio.sleep(0.1)
    assert not b["probe_in_flight"]
    assert b["state"] == "probe" and b["consecutive_failures"] == 3, b
    print("[OK] 断连后等通信退出释放闸门; 仍 probe, 失败数 3 (断连不计)")

    p2 = asyncio.create_task(get("probe2", "s_delay=0.8"))
    saw = False
    for _ in range(30):
        await asyncio.sleep(0.02)
        if (await breaker())["probe_in_flight"]:
            saw = True; break
    assert saw
    print("[OK] 通信退出后下一请求接替为探针")
    await p2
    for _ in range(25):
        b = await breaker()
        if b["state"] == "closed": break
        await asyncio.sleep(0.2)
    assert b["state"] == "closed" and b["consecutive_failures"] == 0, b
    print("[OK] 接替探针成功 -> closed, 计数归零")
    async with base.get(f"{GW}/api/records") as r:
        recs = await r.json()
    disc = [x for x in recs if f"marker={RUN}-probe&" in x["target"]]
    assert disc and disc[0]["result"] == "client_disconnected" and disc[0].get("probe")
    print("[OK] 断连探针记录 client_disconnected + 探针标记")
    await base.close()
    print("PROBE DISCONNECT HANDOVER PASSED")
asyncio.run(main())
