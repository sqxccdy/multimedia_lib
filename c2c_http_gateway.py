"""将 HTTP 请求转发给 C2C 网络中声明了 HTTP 路由的算子节点。"""

import asyncio
import base64
import http.client
import json
import logging
import sys
import threading
import uuid
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse

from adp_operator.c2c.operator import PermanentOperator


class C2CHttpGateway(PermanentOperator):
    http_port = 8080

    def run(self) -> None:
        return None

    def background(self) -> None:
        app = FastAPI(title="C2C HTTP Gateway")
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=[
                "Accept-Ranges",
                "Content-Length",
                "Content-Range",
            ],
        )

        @app.api_route(
            "/{path:path}",
            methods=[
                "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"
            ],
        )
        async def dispatch(request: Request, path: str):
            route_path = f"/{path}"
            try:
                target = self._find_route(request.method, route_path)
            except RuntimeError as error:
                return JSONResponse(
                    {"error": str(error)},
                    status_code=503,
                )
            if target is None:
                return JSONResponse(
                    {"error": f"未找到路由: {request.method} {route_path}"},
                    status_code=404,
                )

            if target[3] is not None:
                try:
                    response = await asyncio.to_thread(
                        self._open_channel_response,
                        target,
                        request,
                        route_path,
                    )
                    headers = self._response_headers(response)
                    if request.method == "HEAD":
                        self._close_channel_response(response)
                        return Response(
                            b"",
                            status_code=response.status,
                            headers=headers,
                        )
                    return StreamingResponse(
                        self._stream_response(response),
                        status_code=response.status,
                        headers=headers,
                    )
                except Exception as error:
                    logging.exception(
                        "[HTTP] 通道转发失败: %s %s",
                        request.method,
                        route_path,
                    )
                    return JSONResponse({"error": str(error)}, status_code=502)

            body = await request.body()
            json_body = None
            if body:
                try:
                    json_body = json.loads(body)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    pass
            request_data = {
                "method": request.method,
                "path": route_path,
                "query": dict(request.query_params),
                "headers": dict(request.headers),
                "body": base64.b64encode(body).decode("ascii"),
                "text": body.decode("utf-8", errors="replace"),
                "json": json_body,
                "client": {
                    "host": request.client.host,
                    "port": request.client.port,
                } if request.client is not None else None,
            }
            try:
                result = await asyncio.to_thread(
                    self._call_route,
                    target,
                    request_data,
                )
                return self._make_response(result)
            except Exception as error:
                logging.exception(
                    "[HTTP] 转发失败: %s %s", request.method, route_path
                )
                return JSONResponse({"error": str(error)}, status_code=502)

        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=self.http_port,
            log_level="info",
        )
        server = uvicorn.Server(config)

        def stop_server() -> None:
            self.stop_event.wait()
            server.should_exit = True

        threading.Thread(
            target=stop_server,
            name="c2c-http-gateway-stop",
            daemon=True,
        ).start()
        logging.info("[HTTP] C2C HTTP 网关监听 0.0.0.0:%s", self.http_port)
        server.run()

    def _find_route(
        self,
        method: str,
        path: str,
    ) -> tuple[str, str, str | None, str | None] | None:
        matches = []
        for peer_id, peer in self.c2c.client.registry.active().items():
            operator_info = peer.get("operator_info") or {}
            operator_name = operator_info.get("name")
            if not isinstance(operator_name, str) or not operator_name:
                continue
            for route in operator_info.get("http_routes") or ():
                if (
                    isinstance(route, dict)
                    and route.get("method") == method
                    and route.get("path") == path
                ):
                    handler = route.get("handler")
                    channel = route.get("channel")
                    channels = peer.get("channels") or {}
                    if (
                        channel is None
                        and isinstance(handler, str)
                        and isinstance(channels, dict)
                        and handler in channels
                    ):
                        channel = handler
                    matches.append((
                        peer_id,
                        operator_name,
                        handler,
                        channel,
                    ))
        matches = list(dict.fromkeys(matches))
        if len(matches) > 1:
            targets = ", ".join(
                f"{operator_name}.{handler or channel}"
                for _, operator_name, handler, channel in matches
            )
            raise RuntimeError(
                f"路由冲突: {method} {path} 同时由 {targets} 提供"
            )
        return matches[0] if matches else None

    def _call_route(
        self,
        target: tuple[str, str, str | None, str | None],
        request_data: dict[str, Any],
    ) -> Any:
        _, operator_name, handler, _ = target
        if handler is None:
            raise ValueError(f"节点 {operator_name} 未声明 HTTP 处理方法")
        with self.c2c.execution(uuid.uuid4().hex) as c2c:
            return c2c.call(operator_name, handler, request_data)

    def _open_channel_response(
        self,
        target: tuple[str, str, str | None, str | None],
        request: Request,
        route_path: str,
    ) -> http.client.HTTPResponse:
        peer_id, _, _, channel_name = target
        if channel_name is None:
            raise ValueError("HTTP 路由未声明通道")
        channel = self.c2c.client.registry.open_channel(
            peer_id,
            channel_name,
        )
        try:
            query = request.scope.get("query_string", b"")
            request_target = route_path.encode("ascii")
            if query:
                request_target += b"?" + query
            lines = [
                request.method.encode("ascii")
                + b" "
                + request_target
                + b" HTTP/1.1",
                b"Host: localhost",
                b"Connection: close",
            ]
            excluded = {b"host", b"connection", b"proxy-connection"}
            lines.extend(
                name + b": " + value
                for name, value in request.headers.raw
                if name.lower() not in excluded
            )
            channel.sendall(b"\r\n".join(lines) + b"\r\n\r\n")
            response = http.client.HTTPResponse(channel)
            response.begin()
            channel.settimeout(None)
            response._c2c_channel = channel
            return response
        except Exception:
            channel.close()
            raise

    @staticmethod
    def _response_headers(response: http.client.HTTPResponse) -> dict[str, str]:
        excluded = {
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade",
        }
        return {
            name: value
            for name, value in response.getheaders()
            if name.lower() not in excluded
        }

    @staticmethod
    def _stream_response(response: http.client.HTTPResponse):
        try:
            while True:
                chunk = response.read(64 * 1024)
                if not chunk:
                    return
                yield chunk
        finally:
            C2CHttpGateway._close_channel_response(response)

    @staticmethod
    def _close_channel_response(response: http.client.HTTPResponse) -> None:
        response.close()
        channel = getattr(response, "_c2c_channel", None)
        if channel is not None:
            channel.close()

    @staticmethod
    def _make_response(result: Any) -> Response:
        if not isinstance(result, dict):
            return JSONResponse(result)
        status_code = result.get("status", 200)
        headers = result.get("headers") or {}
        if not isinstance(status_code, int) or not 100 <= status_code <= 599:
            raise ValueError("HTTP 响应 status 必须是 100~599 的整数")
        if not isinstance(headers, dict):
            raise ValueError("HTTP 响应 headers 必须是对象")
        if "body_base64" in result:
            body = base64.b64decode(result["body_base64"], validate=True)
            return Response(body, status_code=status_code, headers=headers)
        body = result.get("body")
        if isinstance(body, (dict, list)) or body is None:
            return JSONResponse(body, status_code=status_code, headers=headers)
        if not isinstance(body, str):
            raise ValueError("HTTP 响应 body 必须是对象、数组、字符串或 null")
        return Response(body, status_code=status_code, headers=headers)

    @staticmethod
    def get_desc(lang: str = "zh") -> str:
        return "监听 8080 端口，并将 HTTP 请求转发给 C2C 网络中声明路由的节点。"


if __name__ == "__main__":
    from adp_operator import serve

    serve(C2CHttpGateway, sys.argv)
