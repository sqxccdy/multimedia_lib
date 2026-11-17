"""HTTP 同步转异步节点；继承后可覆盖 run 实现自己的 pipeline。"""

import copy
import json
import threading
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

from adp_operator.c2c.operator import PermanentOperator


class AsyncTaskNode(PermanentOperator):
    http_routes = (
        {"method": "POST", "path": "/tasks", "handler": "submit_task"},
        {"method": "GET", "path": "/tasks", "handler": "get_task"},
    )

    def __init__(self):
        self._tasks = {}
        self._task_expirations = {}
        self._lock = threading.Lock()

    def bind_c2c(self, c2c):
        super().bind_c2c(c2c)
        # 默认 call 会启动临时子进程；任务接口必须在常驻实例上执行。
        c2c.client.executor.call = self._handle_call

    def _handle_call(self, message, send_event, connection=None):
        method = message.get("method")
        try:
            if method not in {"submit_task", "get_task"}:
                raise ValueError(f"不支持调用方法: {method}")
            args = message.get("args", [])
            kwargs = message.get("kwargs", {})
            if not isinstance(args, list) or not isinstance(kwargs, dict):
                raise ValueError("args 必须是数组，kwargs 必须是对象")
            send_event({"type": "call_ack", "status": "running", "method": method})
            result = getattr(self, method)(*args, **kwargs)
        except Exception as error:
            send_event({"type": "call_error", "status": "error", "error": str(error)})
            return
        send_event({"type": "call_result", "status": "ok", "method": method,
                    "result": result})

    def background(self):
        """即使没有查询请求，也定期释放已完成任务的结果。"""
        while not self.stop_event.wait(1):
            with self._lock:
                self._cleanup_tasks()

    def _cleanup_tasks(self):
        """调用者必须持有 self._lock。使用单调时钟避免系统时间调整影响。"""
        now = time.monotonic()
        expired = [task_id for task_id, deadline in self._task_expirations.items()
                   if deadline <= now]
        for task_id in expired:
            self._tasks.pop(task_id, None)
            del self._task_expirations[task_id]

    @staticmethod
    def _now():
        return datetime.now(timezone.utc).isoformat()

    def submit_task(self, request):
        payload = request.get("json")
        if not isinstance(payload, dict):
            return {"status": 400, "body": {"error": "请求体必须为 JSON 对象"}}
        payload = copy.deepcopy(payload)
        task_id = uuid4().hex
        with self._lock:
            self._tasks[task_id] = {
                "task_id": task_id, "pipeline_id": task_id,
                "status": "queued", "result": None, "error": None,
                "created_at": self._now(), "started_at": None,
                "finished_at": None, "latest_log": None,
            }
        try:
            threading.Thread(target=self._execute, args=(task_id, payload),
                             name=f"task-{task_id}", daemon=True).start()
        except Exception as error:
            self._update(task_id, status="failed", error=str(error),
                         finished_at=self._now())
            return {"status": 503, "body": self._snapshot(task_id)}
        return {"status": 202, "body": {"task_id": task_id,
                                         "pipeline_id": task_id}}

    def _snapshot(self, task_id):
        with self._lock:
            self._cleanup_tasks()
            return copy.deepcopy(self._tasks.get(task_id))

    def _update(self, task_id, **fields):
        with self._lock:
            self._tasks[task_id].update(fields)
            if fields.get("status") in ("succeeded", "failed"):
                self._task_expirations[task_id] = time.monotonic() + 60

    def get_task(self, request):
        task_id = (request.get("query") or {}).get("task_id")
        if not isinstance(task_id, str) or not task_id:
            return {"status": 400, "body": {"error": "缺少 task_id"}}
        task = self._snapshot(task_id)
        if task is None:
            return {"status": 404, "body": {"error": "任务不存在"}}
        return {"status": 200, "body": task}

    def _execute(self, task_id, payload):
        self._update(task_id, status="running", started_at=self._now())
        try:
            with self.c2c.execution(task_id):
                result = self.run(payload, task_id)
                # 在完成前检查结果是否能通过 HTTP 返回。
                result = json.loads(json.dumps(result, allow_nan=False))
            self._update(task_id, status="succeeded", result=result,
                         finished_at=self._now())
        except Exception as error:
            self._update(task_id, status="failed", error=str(error),
                         finished_at=self._now())

    def _record_log(self, task_id, event):
        message = event.get("message")
        if isinstance(message, str) and message.strip():
            with self._lock:
                task = self._tasks.get(task_id)
                if task is not None:
                    task["latest_log"] = message[-8192:]

    def run(self, payload, pipeline_id):
        """默认执行指定算子；子类可以覆盖此方法编排自己的任务。"""
        operator_name = payload.get("operator_name")
        if not isinstance(operator_name, str) or not operator_name:
            raise ValueError("缺少 operator_name")
        request = {
            "type": "run", "operator_name": operator_name,
            "pipeline_id": pipeline_id,
            "config": payload.get("config", {"init": {}, "run": {}}),
        }
        # 直接消费原有运行事件流：日志已由下载节点通过 C2C 发回。
        # C2CCommands.run 会跳过 log，所以在这里一起读取日志和结果。
        result_event = None
        for event in self.c2c._request(
            operator_name, request, {"run_end", "run_error"}
        ):
            event_type = event.get("type")
            if event_type == "log":
                self._record_log(pipeline_id, event)
            elif event_type == "run_error":
                raise RuntimeError(event.get("error", "运行失败"))
            elif event_type == "run_result":
                result_event = event
            elif event_type == "run_end":
                if result_event is None:
                    raise RuntimeError("未收到执行结果")
                return result_event.get("result")
        raise RuntimeError("运行事件流异常结束")

    @staticmethod
    def get_desc(lang="zh"):
        return "提交任务立即返回 ID，后台执行 pipeline，按 ID 查询状态和结果。"


if __name__ == "__main__":
    from adp_operator import serve
    serve(AsyncTaskNode, sys.argv)
