import uvicorn

from threading import Thread
from main import app

import consumer


def run_server():
    config = uvicorn.Config(app, host="0.0.0.0", port=8001)
    server = uvicorn.Server(config)
    server.run()

Thread(target=run_server, daemon=True).start()
consumer.listen()
