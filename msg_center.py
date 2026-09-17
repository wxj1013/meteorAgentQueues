import socket
import pickle
import uuid
from typing import Optional
from task import Task

class MessageCenter:
    def __init__(self, host="localhost", port=6666):
        self.host = host
        self.port = port

    def _send(self, cmd, args=None):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))
            sock.sendall(pickle.dumps((cmd, args)))
            resp = sock.recv(8192)
            return pickle.loads(resp)

    def publish_task(self, queue: str, content: str) -> str:
        resp = self._send("publish", (queue, content))
        if resp.get("ok"):
            return resp["task_id"]
        raise Exception(resp.get("error", "unknown error"))

    def fetch_task(self, queue: str) -> Optional[Task]:
        resp = self._send("fetch", queue)
        if resp.get("ok"):
            return pickle.loads(resp["data"])
        return None

    def report_result(self, task_id: str, result: str):
        self._send("report", (task_id, result))

    def fetch_result(self, task_id: str):
        resp = self._send("result", task_id)
        return resp["data"]
