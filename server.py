import socketserver
import pickle
import queue
import uuid
from task import Task

# 五个任务队列
QUEUES = {
    "planner": queue.Queue(),
    "coder": queue.Queue(),
    "analyst": queue.Queue(),
    "engineer": queue.Queue(),
    "tester": queue.Queue(),
}

class QueueHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(8192)
        if not data:
            return
        try:
            cmd, args = pickle.loads(data)
        except:
            self.request.sendall(pickle.dumps({"error": "bad request"}))
            return

        if cmd == "publish":
            queue_name, content = args
            if queue_name not in QUEUES:
                self.request.sendall(pickle.dumps({"error": f"queue {queue_name} not exists"}))
                return
            task_id = str(uuid.uuid4())
            task = Task(task_id=task_id, content=content)
            QUEUES[queue_name].put(pickle.dumps(task))
            self.request.sendall(pickle.dumps({"ok": True, "task_id": task_id}))

        elif cmd == "fetch":
            queue_name = args
            if queue_name not in QUEUES:
                self.request.sendall(pickle.dumps({"error": "no such queue"}))
                return
            data = QUEUES[queue_name].get(5)
            self.request.sendall(pickle.dumps({"ok": True, "data": data}))

        elif cmd == "report":
            task_id, result = args
            task = Task(task_id=task_id, result=result)
            QUEUES["planner"].put(pickle.dumps(task))
            self.request.sendall(pickle.dumps({"ok": True}))

        elif cmd == "clear":
            for q in QUEUES.values():
                while not q.empty():
                    try:
                        q.get_nowait()
                    except:
                        break
            self.request.sendall(pickle.dumps({"ok": True}))
        else:
            self.request.sendall(pickle.dumps({"error": "unknown command"}))

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
