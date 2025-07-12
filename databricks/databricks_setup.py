# setup_databricks_config.py

import os
import json
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")
cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")

if not all([host, token, cluster_id]):
    raise ValueError("Missing one or more environment variables in .env")

# 1. ~/.databrickscfg for auth
cfg_path = os.path.expanduser("~/.databrickscfg")
with open(cfg_path, "w") as f:
    f.write(f"[DEFAULT]\nhost = {host}\ntoken = {token}\ncluster_id = {cluster_id}")

"""# 2. ~/.databricks-connect for cluster config
connect_path = os.path.expanduser("~/.databricks-connect")
with open(connect_path, "w") as f:
    json.dump({
        "host": host,
        "token": token,
        "cluster_id": cluster_id
    }, f, indent=2)
"""
print("Databricks config setup complete:")
print(f" - Auth:       {cfg_path}")

