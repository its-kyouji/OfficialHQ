import asyncio

from config import BUF_SIZE

# === BIDIRECTIONAL RELAY ===

async def relay_stream(src, dst):
    try:
        while True:
            data = await src.read(BUF_SIZE)

            if not data:
                break

            dst.write(data)
            await dst.drain()

    except (
        ConnectionError,
        asyncio.CancelledError
    ):
        pass

    except Exception:
        pass
