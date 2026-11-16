"""通过 C2C http_proxy channel 将 HTTP/CONNECT 请求转到本地 SOCKS5。"""

import logging
import os
import socket
import socketserver
import struct
import sys
import time
import select
from uuid import uuid4
from urllib.parse import urlsplit

from adp_operator.c2c.operator import PermanentOperator

def _relay(left, right, stop_event, idle_timeout=300):
    """非阻塞双向转发，限制缓冲区；按实际读写活动而非连接寿命超时。"""
    peers = {left: right, right: left}
    buffers = {left: bytearray(), right: bytearray()}
    readers = {left, right}
    shutdown = set()
    transferred = {left: 0, right: 0}
    last_activity = time.monotonic()
    for connection in peers:
        connection.setblocking(False)
    try:
        while readers or any(buffers.values()):
            if stop_event.is_set():
                return "节点停止", transferred[left], transferred[right]
            if time.monotonic() - last_activity >= idle_timeout:
                return "空闲超时", transferred[left], transferred[right]
            readable, writable, _ = select.select(
                [source for source in readers if len(buffers[peers[source]]) < 262144],
                [target for target in peers if buffers[target]], [], 1,
            )
            for source in readable:
                try:
                    data = source.recv(min(65536, 262144 - len(buffers[peers[source]])))
                except BlockingIOError:
                    continue
                if data:
                    buffers[peers[source]].extend(data)
                    last_activity = time.monotonic()
                else:
                    readers.remove(source)
            for target in writable:
                try:
                    sent = target.send(buffers[target])
                except BlockingIOError:
                    continue
                if not sent:
                    raise ConnectionError("转发写入返回 0")
                del buffers[target][:sent]
                transferred[peers[target]] += sent
                last_activity = time.monotonic()
            for source, target in peers.items():
                if source not in readers and not buffers[target] and target not in shutdown:
                    target.shutdown(socket.SHUT_WR)
                    shutdown.add(target)
        return "双方关闭", transferred[left], transferred[right]
    finally:
        left.close()
        right.close()


def _read_exact(connection, size):
    data = bytearray()
    while len(data) < size:
        chunk = connection.recv(size - len(data))
        if not chunk:
            raise ConnectionError("SOCKS5 握手连接关闭")
        data.extend(chunk)
    return bytes(data)


def _connect_socks(proxy_url, host, port):
    proxy = urlsplit(proxy_url)
    if proxy.scheme != "socks5" or not proxy.hostname or proxy.username:
        raise ValueError("SOCKS5_PROXY 必须是无认证 socks5://host:port")
    connection = socket.create_connection((proxy.hostname, proxy.port or 1080), timeout=15)
    try:
        connection.sendall(b"\x05\x01\x00")
        if _read_exact(connection, 2) != b"\x05\x00":
            raise ConnectionError("SOCKS5 不支持无认证连接")
        name = host.encode("idna")
        if not 0 < len(name) <= 255 or not 0 < port <= 65535:
            raise ValueError("目标地址无效")
        connection.sendall(b"\x05\x01\x00\x03" + bytes([len(name)]) + name + struct.pack("!H", port))
        version, status, reserved, address_type = _read_exact(connection, 4)
        if version != 5 or status != 0:
            raise ConnectionError(f"SOCKS5 CONNECT 失败: {status}")
        if address_type == 1:
            _read_exact(connection, 4)
        elif address_type == 4:
            _read_exact(connection, 16)
        elif address_type == 3:
            _read_exact(connection, _read_exact(connection, 1)[0])
        else:
            raise ConnectionError("SOCKS5 返回无效地址类型")
        _read_exact(connection, 2)
        connection.settimeout(None)
        return connection
    except Exception:
        connection.close()
        raise


class _ProxyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        connection_id = uuid4().hex[:8]
        started_at = time.monotonic()
        stage = "读取 HTTP 请求头"
        logging.info("[ProxyNode:%s] 连接到达 channel=http_proxy peer=%s:%s",
                     connection_id, *self.client_address)
        upstream = None
        established = False
        try:
            self.request.settimeout(15)
            header = bytearray()
            # 不预读隧道内容或请求体，后续直接双向转发。
            while not header.endswith(b"\r\n\r\n"):
                chunk = self.request.recv(1)
                if not chunk:
                    logging.info("[ProxyNode:%s] 客户端在请求头完成前断开", connection_id)
                    return
                header.extend(chunk)
                if len(header) > 65536:
                    raise ValueError("HTTP 请求头过大")
            lines = bytes(header).split(b"\r\n")
            method, target, version = lines[0].decode("ascii").split()
            if method == "CONNECT":
                address = urlsplit("//" + target)
                host, port = address.hostname, address.port or 443
            else:
                address = urlsplit(target)
                if address.scheme != "http" or not address.hostname or address.username:
                    raise ValueError("需要 HTTP 绝对 URL 或 CONNECT 请求")
                host, port = address.hostname, address.port or 80
            if not host:
                raise ValueError("缺少目标主机")
            socks = urlsplit(self.server.socks_url)
            logging.info("[ProxyNode:%s] 请求 %s target=%s:%s，经 SOCKS5 %s:%s",
                         connection_id, method, host, port,
                         socks.hostname, socks.port or 1080)
            stage = "SOCKS5 连接和目标握手"
            upstream = _connect_socks(self.server.socks_url, host, port)
            logging.info("[ProxyNode:%s] SOCKS5 握手成功，已连接 target=%s:%s",
                         connection_id, host, port)
            stage = "建立 HTTP 转发"
            if method == "CONNECT":
                self.request.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            else:
                path = address.path or "/"
                if address.query:
                    path += "?" + address.query
                excluded = {b"proxy-authorization", b"proxy-connection", b"connection", b"host"}
                headers = [line for line in lines[1:] if line and line.split(b":", 1)[0].lower() not in excluded]
                headers += [b"Host: " + address.netloc.encode("ascii"), b"Connection: close"]
                upstream.sendall(f"{method} {path} {version}\r\n".encode("ascii") + b"\r\n".join(headers) + b"\r\n\r\n")
            established = True
            stage = "双向转发"
            logging.info("[ProxyNode:%s] 开始转发%s", connection_id,
                         "，CONNECT 200 已返回" if method == "CONNECT" else "")
            reason, uploaded, downloaded = _relay(
                self.request, upstream, self.server.stop_event, self.server.idle_timeout)
            logging.info("[ProxyNode:%s] 转发结束 reason=%s 上传=%sB 下载=%sB",
                         connection_id, reason, uploaded, downloaded)
        except Exception:
            logging.exception("[ProxyNode:%s] 代理连接失败 stage=%s", connection_id, stage)
            if not established:
                try:
                    self.request.sendall(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                except OSError:
                    pass
        finally:
            if upstream is not None:
                upstream.close()
            logging.info("[ProxyNode:%s] 连接结束 established=%s stage=%s elapsed=%.2fs",
                         connection_id, established, stage, time.monotonic() - started_at)


class _ProxyServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class ProxyNode(PermanentOperator):
    proxy_port = int(os.environ.get("PROXY_NODE_PORT", "18118"))

    @classmethod
    def get_extra_info(cls):
        return {"channels": {"http_proxy": {"port": cls.proxy_port,
                                             "socket_env": "C2C_HTTP_PROXY_SOCKET"}}}

    def run(self):
        return None

    def background(self):
        with _ProxyServer(("127.0.0.1", self.proxy_port), _ProxyHandler) as server:
            server.socks_url = os.environ.get("SOCKS5_PROXY", "socks5://127.0.0.1:1080")
            server.timeout = 1
            server.stop_event = self.stop_event
            server.idle_timeout = float(os.environ.get("PROXY_IDLE_TIMEOUT", "300"))
            if server.idle_timeout <= 0:
                raise ValueError("PROXY_IDLE_TIMEOUT 必须大于 0")
            socks = urlsplit(server.socks_url)
            logging.info("[ProxyNode] channel=http_proxy listening=127.0.0.1:%s SOCKS5=%s:%s",
                         self.proxy_port, socks.hostname, socks.port or 1080)
            while not self.stop_event.is_set():
                server.handle_request()

    @staticmethod
    def get_desc(lang="zh"):
        return "通过 http_proxy channel 提供本地 SOCKS5 到 HTTP 代理转换。"


if __name__ == "__main__":
    from adp_operator import serve
    serve(ProxyNode, sys.argv)

