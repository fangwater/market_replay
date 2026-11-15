import os

from .api import ReferenceHandler, ReferenceServer


def serve(host: str | None = None, port: int | None = None) -> None:
    host = host or os.environ.get("CME_REFERENCE_HOST", "0.0.0.0")
    port = int(port or os.environ.get("CME_REFERENCE_PORT", "8770"))
    server = ReferenceServer((host, port), ReferenceHandler)
    print(f"CME Reference listening on http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    serve()
