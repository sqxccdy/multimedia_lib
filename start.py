"""在独立进程中运行本项目的 C2C 节点和 adp_operator 网关。"""

import argparse
import math
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent
NODE_MODULES = (
    "adp_operator.operators.adp_redis",
    "adp_operator.operators.c2c_http_gateway",
    "lib.proxy_node",
    "lib.m3u8Download",
    "lib.async_task_node",
    "lib.video_play",
)
WEBSOCKET_MODULE = "adp_operator.operators.c2c_websocket_gateway"


def stop_processes(processes):
    running = [process for process in processes.values() if process.poll() is None]
    for process in running:
        process.terminate()
    deadline = time.monotonic() + 5
    for process in running:
        try:
            process.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def watch_processes(processes, duration=None):
    deadline = None if duration is None else time.monotonic() + duration
    while True:
        for module, process in processes.items():
            status = process.poll()
            if status is None:
                continue
            print(f"{module} 提前退出，退出码 {status}", file=sys.stderr)
            if module == WEBSOCKET_MODULE:
                print(
                    "若 WebSocket 端口已占用，请用 --ws-port 指定空闲端口，"
                    "或用 --no-websocket 复用现有网关。",
                    file=sys.stderr,
                )
            return status if status > 0 else 1
        if deadline is None:
            time.sleep(0.25)
        else:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            time.sleep(min(0.25, remaining))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--network", help="传给所有 C2C 节点的网络配置")
    parser.add_argument(
        "--startup-delay", type=float, default=3.0,
        help="相邻进程的启动间隔（秒，默认 3）",
    )
    websocket = parser.add_mutually_exclusive_group()
    websocket.add_argument(
        "--ws-port", type=int, help="覆盖 WebSocket 网关监听端口",
    )
    websocket.add_argument(
        "--no-websocket", action="store_true", help="复用已经运行的 WebSocket 网关",
    )
    args = parser.parse_args()
    if not math.isfinite(args.startup_delay) or args.startup_delay <= 0:
        parser.error("--startup-delay 必须是大于 0 的有限秒数")
    if args.ws_port is not None and not 1 <= args.ws_port <= 65535:
        parser.error("--ws-port 必须在 1–65535 之间")
    node_args = ["--network", args.network] if args.network is not None else []
    commands = [(module, node_args) for module in NODE_MODULES]
    if not args.no_websocket:
        ws_args = ["--host", os.getenv("C2C_WS_HOST", "0.0.0.0")]
        if args.ws_port is not None:
            ws_args.extend(["--port", str(args.ws_port)])
        commands.append((WEBSOCKET_MODULE, ws_args))

    processes = {}
    try:
        for module, flags in commands:
            if processes:
                status = watch_processes(processes, args.startup_delay)
                if status is not None:
                    return status
            print(f"启动 {module}", flush=True)
            processes[module] = subprocess.Popen(
                [sys.executable, "-m", module, *flags], cwd=ROOT,
            )

        return watch_processes(processes)
    except KeyboardInterrupt:
        return 130
    finally:
        stop_processes(processes)


def exit_on_sigterm(signum, frame):
    raise SystemExit(128 + signum)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, exit_on_sigterm)
    raise SystemExit(main())
