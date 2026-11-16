"""通过 C2C socket 通道暴露真实 Redis 服务的常驻算子。"""

import os
import errno
import logging
import socket
import sys
import threading
from pathlib import Path
from typing import Any

import redis

from adp_operator import serve
from adp_operator.c2c.operator import PermanentOperator
from adp_operator.c2c.socket_channel import relay_sockets, request_local_channel


class _RecoveringPipeline(redis.client.Pipeline):
    def execute(self, *args, **kwargs):
        try:
            return super().execute(*args, **kwargs)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            ADPRedis.reset_client(expected_pool=self.connection_pool)
            raise


class _RecoveringRedis(redis.StrictRedis):
    def execute_command(self, *args, **kwargs):
        try:
            return super().execute_command(*args, **kwargs)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            ADPRedis.reset_client(expected_pool=self.connection_pool)
            raise

    def pipeline(self, transaction=True, shard_hint=None):
        return _RecoveringPipeline(
            self.connection_pool, self.response_callbacks, transaction, shard_hint,
        )


class ADPRedis(PermanentOperator):
    proxy_port = int(os.getenv("ADP_REDIS_PORT", "49999"))
    _client = None
    _client_socket_identity = None
    _client_lock = threading.Lock()

    @staticmethod
    def get_desc(lang: str = "zh") -> str:
        if lang == "en":
            return "Expose Redis to C2C nodes through a discoverable local socket channel."
        return "通过可发现的本地 Socket 通道，为 C2C 节点提供 Redis 访问能力。"

    def __init__(self) -> None:
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.server = None

    @classmethod
    def get_extra_info(cls) -> dict[str, Any]:
        return {
            "channels": {
                "redis": {
                    "port": cls.proxy_port,
                    "socket_env": "ADP_REDIS_SOCKET",
                    "config": {
                        "REDIS_PASSWORD": os.getenv("REDIS_PASSWORD", ""),
                        "REDIS_DB": os.getenv("REDIS_DB", "0"),
                    },
                }
            }
        }

    @classmethod
    def get_client(cls):
        with cls._client_lock:
            socket_path = os.environ.get("ADP_REDIS_SOCKET")
            # HTTP calls run in separate processes: inherit neither stale channel
            # configuration nor a different local REDIS_DB from the caller.
            if os.environ.get("C2C_CHANNEL_CONTROL_SOCKET"):
                try:
                    socket_path = request_local_channel("redis")
                except (ConnectionError, OSError) as error:
                    if cls._client is not None:
                        cls._client.connection_pool.disconnect()
                    cls._client = None
                    cls._client_socket_identity = None
                    raise RuntimeError("ADPRedis 通道不可用") from error
            if not socket_path or not Path(socket_path).exists():
                if cls._client is not None:
                    cls._client.connection_pool.disconnect()
                    cls._client = None
                cls._client_socket_identity = None
                try:
                    socket_path = request_local_channel("redis")
                except (ConnectionError, OSError):
                    cls._client = None
                    raise RuntimeError("ADPRedis 节点丢失")
            if not Path(socket_path).exists():
                raise RuntimeError("ADPRedis 节点丢失")
            stat = Path(socket_path).stat()
            identity = (socket_path, stat.st_ino, stat.st_mtime_ns,
                        os.getenv("REDIS_DB", "0"), os.getenv("REDIS_PASSWORD", ""))
            if cls._client is not None and cls._client_socket_identity != identity:
                cls._client.connection_pool.disconnect()
                cls._client = None
            if cls._client is None:
                cls._client = _RecoveringRedis(
                    unix_socket_path=socket_path,
                    db=int(os.getenv("REDIS_DB", "0")),
                    password=os.getenv("REDIS_PASSWORD", ""),
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    max_connections=int(os.getenv("ADP_REDIS_MAX_CONNECTIONS", "16")),
                )
                cls._client_socket_identity = identity
            return cls._client

    @classmethod
    def reset_client(cls, expected_pool=None) -> None:
        """丢弃失效连接池，下次调用重新申请通道。"""
        with cls._client_lock:
            if expected_pool is not None and (
                cls._client is None or cls._client.connection_pool is not expected_pool
            ):
                return
            if cls._client is not None:
                cls._client.connection_pool.disconnect()
            cls._client = None
            cls._client_socket_identity = None
            os.environ.pop("ADP_REDIS_SOCKET", None)
            logging.warning("[Redis] 连接池已失效，下次调用将重新申请通道")

    def bind_client(self, client) -> None:
        self._relay_lock = threading.Lock()
        self._relay_connections = set()
        self._relay_slots = threading.BoundedSemaphore(
            int(os.getenv("ADP_REDIS_MAX_RELAY_CONNECTIONS", "128"))
        )
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(("0.0.0.0", self.proxy_port))
        self.server.listen()
        self.server.settimeout(1)
        threading.Thread(target=self._serve, args=(client.stop_event,), daemon=True).start()
        threading.Thread(
            target=self._close_on_stop,
            args=(client.stop_event,),
            daemon=True,
        ).start()

    def _close_on_stop(self, stop_event) -> None:
        stop_event.wait()
        self.server.close()
        with self._relay_lock:
            connections = tuple(self._relay_connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()

    def _relay(self, client, stop_event) -> None:
        upstream = None
        try:
            with self._relay_lock:
                if stop_event.is_set():
                    return
                self._relay_connections.add(client)
            upstream = socket.create_connection(
                (self.redis_host, self.redis_port), timeout=5,
            )
            upstream.settimeout(None)
            with self._relay_lock:
                if stop_event.is_set():
                    return
                self._relay_connections.add(upstream)
            relay_sockets(client, upstream)
        except OSError:
            logging.debug("[Redis] 转发连接结束", exc_info=True)
        finally:
            for connection in (client, upstream):
                if connection is not None:
                    connection.close()
                    with self._relay_lock:
                        self._relay_connections.discard(connection)
            self._relay_slots.release()

    def _serve(self, stop_event) -> None:
        while not stop_event.is_set():
            try:
                client, _ = self.server.accept()
            except socket.timeout:
                continue
            except OSError as error:
                if stop_event.is_set():
                    return
                if error.errno in {errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM}:
                    logging.error("[Redis] accept 资源不足，稍后重试: %s", error)
                    stop_event.wait(1)
                    continue
                logging.exception("[Redis] 代理监听失败")
                return
            if not self._relay_slots.acquire(blocking=False):
                client.close()
                continue
            try:
                threading.Thread(
                    target=self._relay, args=(client, stop_event), daemon=True,
                ).start()
            except Exception:
                client.close()
                self._relay_slots.release()
                logging.exception("[Redis] 无法启动转发线程")

    def run(self, storage: Any = None) -> None:
        del storage

if __name__ == '__main__':
    serve(ADPRedis, sys.argv)

