import os

import uvicorn

from app import create_app

app = create_app()


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=os.getenv("GW_HOST", "127.0.0.1"),
        port=int(os.getenv("GW_PORT", "0")),
    )
