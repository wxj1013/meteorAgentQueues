from server import QueueHandler
import yaml
import socketserver  # 新增导入

# 假设配置文件名为 config.yaml，请根据实际路径调整
with open("config.yaml", "r") as f:
    cfg = yaml.safe_load(f)["message_center"]

HOST, PORT = cfg["host"], cfg["port"]
with socketserver.ThreadingTCPServer((HOST, PORT), QueueHandler) as server:
    print(f"Queue server running on {HOST}:{PORT}")
    server.serve_forever()