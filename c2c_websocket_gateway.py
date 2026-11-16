"""Local, read-only WebSocket adapter for nodes/index.html and C2C TCP."""
import argparse
import asyncio
from contextlib import suppress
from http import HTTPStatus
import json
import logging
import os
from pathlib import Path
import signal
import time

from adp_operator.c2c.session import C2CSession
from adp_operator.c2c.service_discovery import find_adp_operator_service
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


class C2CReader:
    def __init__(self, host="127.0.0.1", port=60000, timeout=3):
        self.host, self.port, self.timeout = host, port, timeout

    def query(self, message):
        # Each request owns its TCP stream; concurrent browser queries cannot
        # consume each other's replies. These messages never invoke run/call.
        with C2CSession.connect(self.host, self.port) as session:
            session.connection.settimeout(self.timeout)
            session.send(message)
            return session.receive()

    def discover(self):
        try:
            response = self.query({"type": "discover"})
            if response.get("type") != "discover_ack" or not isinstance(response.get("peers"), dict):
                raise ValueError("C2C discover 未返回有效节点列表")
            return response
        except (OSError, ValueError, ConnectionError):
            if self.host not in {"127.0.0.1", "localhost"}:
                raise
            # The runtime chooses the first available TCP port. A rapid node
            # restart can leave 60000 occupied by TIME_WAIT and select 60001+.
            # Use its protocol-verified finder, restricted to this container.
            host, port, _ = find_adp_operator_service(self.host)
            self.host, self.port = host, port
            logger.info("Discovered local C2C TCP entry: %s:%s", host, port)
            response = self.query({"type": "discover"})
            if response.get("type") != "discover_ack" or not isinstance(response.get("peers"), dict):
                raise ValueError("C2C discover 未返回有效节点列表")
            return response

    def info(self, node_id):
        response = self.query({"type": "info", "identifier": node_id})
        if response.get("type") != "info_ack" or response.get("node_id") != node_id:
            raise ValueError(f"C2C 节点信息响应不匹配: {node_id}")
        return response


class WebSocketGateway:
    def __init__(self, reader, *, port=61000, poll_interval=2):
        self.reader = reader
        self.port = port
        self.poll_interval = poll_interval
        self.entry_node_id = None
        self.nodes = {}
        self.infos = {}
        self.error = None
        self.subscribers = set()
        self.updated_at = 0
        self.refresh_lock = asyncio.Lock()
        self.info_limit = asyncio.Semaphore(8)

    async def send(self, connection, message):
        try:
            await connection.send(json.dumps(message, ensure_ascii=False))
        except ConnectionClosed:
            self.subscribers.discard(connection)

    async def broadcast(self, message):
        await asyncio.gather(*(self.send(client, message) for client in tuple(self.subscribers)))

    def snapshot(self):
        return {
            "type": "subscribe_ack",
            "entry_node_id": self.entry_node_id,
            "nodes": list(self.nodes.values()),
        }

    async def get_info(self, node_id):
        async with self.info_limit:
            return await asyncio.to_thread(self.reader.info, node_id)

    @staticmethod
    def signature(node):
        return {key: value for key, value in node.items() if key != "last_seen"}

    async def refresh(self, *, force=False):
        async with self.refresh_lock:
            if not force and time.monotonic() - self.updated_at < self.poll_interval:
                return
            old_nodes, old_entry, old_error = self.nodes, self.entry_node_id, self.error
            try:
                discovered = await asyncio.to_thread(self.reader.discover)
                peers = discovered["peers"]
                results = await asyncio.gather(
                    *(self.get_info(node_id) for node_id in peers), return_exceptions=True
                )
                infos, nodes = {}, {}
                entry = discovered["node_id"]
                for (node_id, peer), info in zip(peers.items(), results):
                    if isinstance(info, BaseException):
                        logger.debug("C2C node unavailable: %s: %s", node_id, info)
                        info = None
                    operator = (info or {}).get("operator") or peer.get("operator_info") or {}
                    if info is not None:
                        infos[node_id] = info
                    nodes[node_id] = {
                        **peer,
                        "node_id": node_id,
                        "connection": "local" if node_id == entry else (
                            "indirect" if peer.get("via") else
                            "reverse" if peer.get("reverse") else "direct"
                        ),
                        "operator_base": operator.get("operator_base", "C2CNodeRuntime"),
                        "running_tasks": (info or {}).get("running_tasks", 0),
                        "websocket_port": self.port if node_id == entry else None,
                    }
                if entry not in nodes:
                    raise ConnectionError("C2C 入口尚未就绪，正在等待本地节点启动")
                self.entry_node_id, self.nodes, self.infos, self.error = entry, nodes, infos, None
            except Exception as exc:
                self.entry_node_id, self.nodes, self.infos = None, {}, {}
                self.error = f"本地 C2C TCP 入口不可用: {exc}"
            self.updated_at = time.monotonic()

            if self.error:
                if old_error != self.error or old_nodes:
                    logger.warning("%s", self.error)
                    await self.broadcast(self.snapshot())
                    await self.broadcast({"type": "websocket_error", "error": self.error})
                return
            if old_error or old_entry != self.entry_node_id:
                logger.info("Local C2C connected: %s, %s nodes", self.entry_node_id, len(self.nodes))
                await self.broadcast(self.snapshot())
                return
            for node_id in old_nodes.keys() - self.nodes.keys():
                await self.broadcast({"type": "node_offline", "node": old_nodes[node_id]})
            for node_id, node in self.nodes.items():
                previous = old_nodes.get(node_id)
                if previous is None:
                    await self.broadcast({"type": "node_discovered", "node": node})
                elif self.signature(previous) != self.signature(node):
                    await self.broadcast({"type": "node_updated", "node": node})

    async def monitor(self):
        while True:
            await self.refresh()
            await asyncio.sleep(self.poll_interval)

    async def handle_message(self, connection, message):
        if not isinstance(message, dict):
            raise ValueError("WebSocket 消息必须是 JSON 对象")
        kind = message.get("type")
        if kind not in {"subscribe", "scan", "info"}:
            raise ValueError("本地只读网关仅支持 subscribe、scan、info")
        await self.refresh()
        if kind == "subscribe":
            self.subscribers.add(connection)
            await self.send(connection, self.snapshot())
        if self.error:
            await self.send(connection, {"type": "websocket_error", "error": self.error})
            return
        if kind == "scan":
            # Advertise the real metadata of reachable, running operators;
            # don't invent a dispatch catalog or trigger any upload operation.
            operators = {}
            for info in self.infos.values():
                operator = info.get("operator") or {}
                if operator.get("name"):
                    operators[operator["name"]] = operator
            await self.send(connection, {"type": "scan_ack", "operators": list(operators.values())})
        elif kind == "info":
            node_id = message.get("node_id")
            if not isinstance(node_id, str) or node_id not in self.infos:
                raise ValueError("节点不存在或暂不可达，请刷新节点列表")
            await self.send(connection, self.infos[node_id])

    async def handle(self, connection):
        try:
            async for raw in connection:
                try:
                    if not isinstance(raw, str):
                        raise ValueError("WebSocket 消息必须是 JSON 文本")
                    await self.handle_message(connection, json.loads(raw))
                except (ValueError, TypeError) as exc:
                    await self.send(connection, {"type": "websocket_error", "error": str(exc)})
        except ConnectionClosed:
            pass
        finally:
            self.subscribers.discard(connection)

    async def http_request(self, connection, request):
        if request.path not in {"/", "/index.html", "/ws"}:
            return connection.respond(HTTPStatus.NOT_FOUND, "Not found\n")
        if request.headers.get("Upgrade", "").lower() == "websocket":
            return None
        page = Path(__file__).with_name("index.html")
        if not page.is_file():
            return connection.respond(HTTPStatus.NOT_FOUND, "nodes/index.html is not installed\n")
        response = connection.respond(HTTPStatus.OK, page.read_text(encoding="utf-8"))
        del response.headers["Content-Type"]
        response.headers["Content-Type"] = "text/html; charset=utf-8"
        response.headers["Cache-Control"] = "no-store"
        return response


async def run_gateway(host, port, tcp_host, tcp_port, poll_interval):
    gateway = WebSocketGateway(C2CReader(tcp_host, tcp_port), port=port, poll_interval=poll_interval)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stopped.set)
    try:
        async with serve(
            gateway.handle, host, port, process_request=gateway.http_request,
            max_size=65536, max_queue=16, close_timeout=3,
        ):
            logger.info("Local C2C page: http://127.0.0.1:%s", port)
            logger.info("WebSocket gateway %s:%s -> C2C TCP %s:%s", host, port, tcp_host, tcp_port)
            monitor = asyncio.create_task(gateway.monitor())
            try:
                await stopped.wait()
            finally:
                monitor.cancel()
                with suppress(asyncio.CancelledError):
                    await monitor
    finally:
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(signum)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.getenv(
        "C2C_WS_HOST", "0.0.0.0" if Path("/.dockerenv").exists() else "127.0.0.1"
    ))
    parser.add_argument("--port", type=int, default=int(os.getenv("C2C_WS_PORT", "61000")))
    parser.add_argument("--tcp-host", default=os.getenv("C2C_TCP_HOST", "127.0.0.1"))
    parser.add_argument("--tcp-port", type=int, default=int(os.getenv("C2C_TCP_PORT", "60000")))
    parser.add_argument("--poll-interval", type=float, default=2)
    args = parser.parse_args(argv)
    if not (1 <= args.port <= 65535 and 1 <= args.tcp_port <= 65535 and args.poll_interval > 0):
        parser.error("端口必须在 1–65535 之间，轮询间隔必须大于 0")
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_gateway(args.host, args.port, args.tcp_host, args.tcp_port, args.poll_interval))


def start_gateway():
    # boostrap.py passes C2C's flags to its seven node children. The gateway
    # has separate configuration and must not parse the parent's --join flags.
    main([])


if __name__ == "__main__":
    main()

