import json
import os
import sys
import urllib.request

port = os.getenv("AGENT_HEALTH_PORT", "8091")
url = f"http://127.0.0.1:{port}/health"

try:
    with urllib.request.urlopen(url, timeout=3) as response:
        payload = json.load(response)
        print(json.dumps(payload, ensure_ascii=True))
        sys.exit(0 if response.status == 200 and payload.get("status") == "ok" else 1)
except Exception as exc:
    print(json.dumps({"status": "unhealthy", "error": str(exc)}, ensure_ascii=True))
    sys.exit(1)
