# Task类
class Task:
    def __init__(self, task_id=None, content=None):
        self.task_id = task_id
        self.content = content
        self.sub_tasks = []
