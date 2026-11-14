"""Run the local API using environment configuration, with no CLI surface."""

import os

from .api import ReferenceServer


def serve(database=None, host=None, port=None):
    database = database or os.environ.get("CN_FUTURES_REF_DATABASE_URL")
    if not database:
        raise RuntimeError("CN_FUTURES_REF_DATABASE_URL is required")
    host = host or os.environ.get("CN_FUTURES_REF_HOST", "0.0.0.0")
    port = int(port or os.environ.get("CN_FUTURES_REF_PORT", "8765"))
    server = ReferenceServer((host, port), database)
    print("Cn Futures Reference API listening on http://%s:%d" % (host, port))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    serve()
