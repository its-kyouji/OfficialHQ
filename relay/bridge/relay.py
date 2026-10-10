import asyncio

from config import BUF_SIZE, IDLE_TIMEOUT
# ====================
# BIDIRECTIONAL RELAY
# ====================
async def relay(reader, writer, label=""):
    try:
        while True:
            data = await asyncio.wait_for(
                reader.read(BUF_SIZE),
                timeout=IDLE_TIMEOUT
            )

            if not data:
                break

            if writer.is_closing():
                break

            writer.write(data)

            await asyncio.wait_for(
                writer.drain(),
                timeout=IDLE_TIMEOUT
            )

    except asyncio.TimeoutError:
        print(f"[BRIDGE:{label}] Idle timeout", flush=True)

    except Exception as e:
        print(
            f"[BRIDGE:{label}] Error: "
            f"{type(e).__name__}: {e}",
            flush=True
        )

    finally:
        try:
            writer.write_eof()
        except (OSError, AttributeError):
            writer.close()
