"""通过 C2C TCP 通道提供支持 HTTP Range 的本地 MP4 播放。"""

import hashlib
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from adp_operator.c2c.operator import PermanentOperator


class VideoPlay(PermanentOperator):
    http_port = int(os.getenv("VIDEO_PLAY_HTTP_PORT", "49998"))
    http_routes = (
        {
            "method": "GET",
            "path": "/video/file",
            "handler": "video_http",
            "channel": "video_http",
        },
        {
            "method": "HEAD",
            "path": "/video/file",
            "handler": "video_http",
            "channel": "video_http",
        },
        {
            "method": "GET",
            "path": "/video/thumbnail",
            "handler": "video_http",
            "channel": "video_http",
        },
        {
            "method": "HEAD",
            "path": "/video/thumbnail",
            "handler": "video_http",
            "channel": "video_http",
        },
    )
    _range = re.compile(r"bytes=(\d*)-(\d*)")
    _thumbnail_lock = threading.Lock()
    _thumbnail_root = Path(tempfile.gettempdir()) / "adp-video-thumbnails"

    def __init__(self) -> None:
        self.server = None

    def run(self, storage: Any = None) -> None:
        del storage

    def video_http(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        return {
            "status": 426,
            "body": {
                "error": "该接口必须由支持 C2C TCP 通道的 HTTP 网关访问"
            },
        }

    @classmethod
    def get_extra_info(cls) -> dict[str, Any]:
        return {
            "channels": {
                "video_http": {
                    "port": cls.http_port,
                }
            }
        }

    def bind_client(self, client) -> None:
        operator = self

        class VideoRequestHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:
                operator._serve(self, send_body=True)

            def do_HEAD(self) -> None:
                operator._serve(self, send_body=False)

            def log_message(self, format_string, *args) -> None:
                logging.info("[视频 HTTP] " + format_string, *args)

        self.server = ThreadingHTTPServer(
            ("127.0.0.1", self.http_port),
            VideoRequestHandler,
        )
        threading.Thread(
            target=self.server.serve_forever,
            name="video-play-http",
            daemon=True,
        ).start()
        threading.Thread(
            target=self._close_on_stop,
            args=(client.stop_event,),
            name="video-play-http-stop",
            daemon=True,
        ).start()
        logging.info(
            "[视频] MP4 Range 服务监听 127.0.0.1:%s",
            self.http_port,
        )

    def _close_on_stop(self, stop_event) -> None:
        stop_event.wait()
        self.server.shutdown()
        self.server.server_close()

    def _serve(
        self,
        handler: BaseHTTPRequestHandler,
        send_body: bool,
    ) -> None:
        path = urlsplit(handler.path).path
        if path == "/video/file":
            self._serve_file(handler, send_body)
            return
        if path == "/video/thumbnail":
            self._serve_thumbnail(handler, send_body)
            return
        self._send_error(handler, 404, "接口不存在")

    def _serve_file(
        self,
        handler: BaseHTTPRequestHandler,
        send_body: bool,
    ) -> None:
        source = self._video_path(handler)
        if source is None:
            return

        size = source.stat().st_size
        try:
            start, end, partial = self._parse_range(
                handler.headers.get("Range"),
                size,
            )
        except ValueError:
            handler.send_response(416)
            handler.send_header("Content-Range", f"bytes */{size}")
            handler.send_header("Content-Length", "0")
            handler.end_headers()
            return

        length = end - start + 1
        handler.send_response(206 if partial else 200)
        handler.send_header("Content-Type", "video/mp4")
        handler.send_header("Accept-Ranges", "bytes")
        handler.send_header("Content-Length", str(length))
        handler.send_header("Cache-Control", "no-cache")
        if partial:
            handler.send_header(
                "Content-Range",
                f"bytes {start}-{end}/{size}",
            )
        handler.end_headers()
        if not send_body:
            return

        logging.info(
            "[视频] 发送文件 %s bytes=%s-%s/%s",
            source,
            start,
            end,
            size,
        )
        try:
            with source.open("rb") as file:
                file.seek(start)
                remaining = length
                while remaining:
                    chunk = file.read(min(1024 * 1024, remaining))
                    if not chunk:
                        break
                    handler.wfile.write(chunk)
                    remaining -= len(chunk)
        except (BrokenPipeError, ConnectionResetError):
            logging.info("[视频] 客户端中断读取: %s", source)

    def _serve_thumbnail(
        self,
        handler: BaseHTTPRequestHandler,
        send_body: bool,
    ) -> None:
        source = self._video_path(handler)
        if source is None:
            return
        try:
            thumbnail = self._thumbnail(source)
        except RuntimeError as error:
            logging.exception("[视频] 生成缩略图失败: %s", source)
            self._send_error(handler, 500, str(error))
            return

        size = thumbnail.stat().st_size
        handler.send_response(200)
        handler.send_header("Content-Type", "image/jpeg")
        handler.send_header("Content-Length", str(size))
        handler.send_header("Cache-Control", "public, max-age=86400")
        handler.end_headers()
        if not send_body:
            return
        try:
            with thumbnail.open("rb") as image:
                while True:
                    chunk = image.read(64 * 1024)
                    if not chunk:
                        return
                    handler.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            logging.info("[视频] 客户端中断缩略图读取: %s", source)

    @staticmethod
    def _video_path(handler: BaseHTTPRequestHandler) -> Path | None:
        values = parse_qs(urlsplit(handler.path).query).get("path")
        if not values or not values[0]:
            VideoPlay._send_error(handler, 400, "缺少查询参数: path")
            return None
        source = Path(values[0]).expanduser().resolve()
        if not source.is_file():
            VideoPlay._send_error(
                handler,
                404,
                f"视频文件存在: {source}",
            )
            return None
        if source.suffix.lower() != ".mp4":
            VideoPlay._send_error(handler, 400, "目前仅支持 MP4 文件")
            return None
        return source

    @classmethod
    def _thumbnail(cls, source: Path) -> Path:
        cls._thumbnail_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        name = hashlib.sha256(str(source).encode("utf-8")).hexdigest()
        thumbnail = cls._thumbnail_root / f"{name}.jpg"
        source_mtime = source.stat().st_mtime_ns
        with cls._thumbnail_lock:
            if (
                thumbnail.is_file()
                and thumbnail.stat().st_mtime_ns >= source_mtime
            ):
                return thumbnail
            ffmpeg = shutil.which("ffmpeg")
            if ffmpeg is None:
                raise RuntimeError("未找到 ffmpeg，无法生成视频缩略图")
            temporary = cls._thumbnail_root / f".{name}.jpg"
            result = subprocess.run(
                [
                    ffmpeg,
                    "-nostdin",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-i",
                    str(source),
                    "-frames:v",
                    "1",
                    "-vf",
                    "scale=480:-2:force_original_aspect_ratio=decrease",
                    "-q:v",
                    "3",
                    "-y",
                    str(temporary),
                ],
                capture_output=True,
                timeout=30,
            )
            if result.returncode or not temporary.is_file():
                temporary.unlink(missing_ok=True)
                detail = result.stderr.decode("utf-8", errors="replace").strip()
                raise RuntimeError(detail or "FFmpeg 未生成缩略图")
            temporary.replace(thumbnail)
            logging.info("[视频] 已生成第一帧缩略图: %s", source)
            return thumbnail

    @classmethod
    def _parse_range(
        cls,
        value: str | None,
        size: int,
    ) -> tuple[int, int, bool]:
        if value is None:
            return 0, max(0, size - 1), False
        match = cls._range.fullmatch(value.strip())
        if match is None or size <= 0:
            raise ValueError("Range 无效")
        start_text, end_text = match.groups()
        if not start_text and not end_text:
            raise ValueError("Range 无效")
        if not start_text:
            suffix = int(end_text)
            if suffix <= 0:
                raise ValueError("Range 无效")
            start = max(0, size - suffix)
            end = size - 1
        else:
            start = int(start_text)
            end = int(end_text) if end_text else size - 1
            if start >= size or end < start:
                raise ValueError("Range 无效")
            end = min(end, size - 1)
        return start, end, True

    @staticmethod
    def _send_error(
        handler: BaseHTTPRequestHandler,
        status: int,
        message: str,
    ) -> None:
        body = message.encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "text/plain; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        if handler.command != "HEAD":
            handler.wfile.write(body)

    @staticmethod
    def get_desc(lang: str = "zh") -> str:
        return "通过 C2C TCP 通道提供支持 HTTP Range 的本地 MP4 流式播放。"


if __name__ == "__main__":
    from adp_operator import serve

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,
    )
    serve(VideoPlay, sys.argv)

