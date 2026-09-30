# server.py
import socketserver
import pickle
import queue
import uuid
from task import Task
import threading

# 五个任务队列
QUEUES = {
    "planner": queue.Queue(),
    "coder": queue.Queue(),
    "analyst": queue.Queue(),
    "engineer": queue.Queue(),
    "tester": queue.Queue(),
}

# 任务进展和回报列表
results_lock = threading.Lock()
results = {}

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

        # 发布一个任务
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
                self.request.sendall(pickle.dumps({"error": f"queue {queue_name} not exists"}))
                return
            try:
                task = QUEUES[queue_name].get_nowait()
                self.request.sendall(pickle.dumps({"ok": True, "data": task}))
            except queue.Empty:
                self.request.sendall(pickle.dumps({"ok": False, "error": "queue empty"}))

        elif cmd == "report":
            task_id, result = args
            with results_lock:
                results[task_id] = result
            self.request.sendall(pickle.dumps({"ok": True}))

        elif cmd == "result":
            task_id = args
            with results_lock:
                result = results.get(task_id)
            if result is None:
                self.request.sendall(pickle.dumps({"ok": False, "error": "not found"}))
            else:
                self.request.sendall(pickle.dumps({"ok": True, "data": result}))

        elif cmd == "peek":
            queue_name = args
            if queue_name not in QUEUES:
                self.request.sendall(pickle.dumps({"error": f"queue {queue_name} not exists"}))
                return
            q = QUEUES[queue_name]
            # 使用队列内部的 mutex 保证线程安全地读取
            with q.mutex:
                all_items = list(q.queue)  # q.queue 是 collections.deque
                count = len(all_items)
                peek_items = all_items[:10]  # 取前10个（仍是 bytes）
            self.request.sendall(pickle.dumps({
                "ok": True, 
                "count": count, 
                "data": peek_items
            }))

        elif cmd == "clear_queue":
            queue_name = args
            if queue_name not in QUEUES:
                self.request.sendall(pickle.dumps({"error": f"queue {queue_name} not exists"}))
                return
            q = QUEUES[queue_name]
            with q.mutex:
                q.queue.clear()  # 直接清空底层 deque
            self.request.sendall(pickle.dumps({"ok": True, "cleared": queue_name}))

        elif cmd == "clear_all":
            # args 在这里是 None
            for qname, q in QUEUES.items():
                with q.mutex:
                    q.queue.clear()
            self.request.sendall(pickle.dumps({"ok": True, "cleared": "all"}))

        else:
            self.request.sendall(pickle.dumps({"error": "unknown command"}))

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
