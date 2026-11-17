import subprocess
import sys
import shlex
import logging
import socket
import socketserver
import threading
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from adp_operator import serve
from adp_operator.c2c.operator import ADPOperator
from adp_operator.c2c.socket_channel import request_local_channel, relay_sockets


class m3u8Download(ADPOperator):
    http_routes = (
        {
            "method": "GET",
            "path": "/m3u8/files",
            "handler": "http_files",
        },
    )

    @staticmethod
    def get_desc(lang: str = "zh") -> str:
        return "Download an M3U8 URL to output.mp4 in the working directory."

    def run(
        self, storage, url: str, proxy: str = "auto", output_dir: str = "/mnt",
    ) -> dict:
        if proxy != "auto":
            return self._download(url, proxy, output_dir)
        socket_path = request_local_channel("ProxyNode")

        class ChannelHandler(socketserver.BaseRequestHandler):
            def handle(self):
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as upstream:
                    upstream.connect(socket_path)
                    relay_sockets(self.request, upstream)

        class ChannelBridge(socketserver.ThreadingTCPServer):
            daemon_threads = True

        # ffmpeg 需要 TCP HTTP 代理，将框架的 Unix channel 桥接到临时回环端口。
        with ChannelBridge(("127.0.0.1", 0), ChannelHandler) as bridge:
            worker = threading.Thread(target=bridge.serve_forever, daemon=True)
            worker.start()
            try:
                logging.info("使用 ProxyNode 的 http_proxy channel")
                return self._download(url, f"http://127.0.0.1:{bridge.server_address[1]}", output_dir)
            finally:
                bridge.shutdown()
                worker.join()

    def _download(
        self,
        url: str,
        proxy: str,
        output_dir: str = "/mnt",
    ) -> dict:
        directory = Path(output_dir).expanduser().resolve()
        directory.mkdir(parents=True, exist_ok=True)
        output_path = directory / f"{uuid4()}.mp4"
        logging.info("下载输出路径: %s", output_path)
        command = [
            "ffmpeg",
            "-rw_timeout", "300000000",
            "-tls_verify", "0",
            "-http_proxy", proxy,
            "-i", url,
            str(output_path),
        ]

        print(shlex.join(command))
        # text 模式将 ffmpeg 用于原地刷新的 \r 也转换为换行。
        with subprocess.Popen(
            command, stderr=subprocess.PIPE, text=True,
            encoding="utf-8", errors="replace",
        ) as process:
            try:
                for line in process.stderr:
                    line = line.strip()
                    if line:
                        logging.getLogger(__name__).info("%s", line)
                return_code = process.wait()
                if return_code:
                    raise subprocess.CalledProcessError(return_code, command)
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
        return {
            "status": "success",
            "file_path": str(output_path),
            "proxy": proxy,
        }

    def http_files(self, request: dict) -> dict:
        query = request.get("query") or {}
        requested_path = query.get("path", "/mnt")
        mnt_root = Path("/mnt").resolve()

        try:
            directory = Path(requested_path).resolve()
        except (OSError, RuntimeError, TypeError) as error:
            return {"status": 400, "body": {"error": str(error)}}

        if directory != mnt_root and mnt_root not in directory.parents:
            return {
                "status": 403,
                "body": {"error": "Only /mnt and its subdirectories may be listed"},
            }
        if not directory.exists():
            return {
                "status": 404,
                "body": {"error": f"Directory does not exist: {directory}"},
            }
        if not directory.is_dir():
            return {
                "status": 400,
                "body": {"error": f"Path is not a directory: {directory}"},
            }

        files = []
        for path in sorted(directory.iterdir(), key=lambda item: item.name):
            if not path.is_file():
                continue
            stat = path.stat()
            files.append({
                "name": path.name,
                "path": str(path),
                "size": stat.st_size,
                "modified_at": datetime.fromtimestamp(
                    stat.st_mtime
                ).astimezone().isoformat(),
            })
        return {
            "status": 200,
            "body": {
                "directory": str(directory),
                "count": len(files),
                "files": files,
            },
        }



if __name__ == "__main__":
    serve(m3u8Download, sys.argv)

