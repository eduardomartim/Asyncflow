"""AsyncFlow API main entry point."""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.v1.endpoints import router
from app.core.config import settings
from app.core.exceptions import AsyncFlowError
from app.db.base import (
    assert_schema_is_current,
    dispose_engine,
    get_session_factory_singleton,
    init_db,
)
from app.mq.connection import RabbitMQConnection
from app.outbox.relay import OutboxRelay

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("asyncflow.api")

mq = RabbitMQConnection()
_relay: OutboxRelay | None = None
_relay_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _relay, _relay_task

    init_db(settings)
    # The schema is owned by Alembic (`alembic upgrade head`, see README);
    # refuse to start against an unmigrated/outdated database.
    await assert_schema_is_current()

    try:
        await mq.connect()
        mq_available = True
    except Exception:
        logger.exception("rabbitmq_connect_failed_api_will_serve_degraded")
        mq_available = False

    if mq_available:
        _relay = OutboxRelay(session_factory=get_session_factory_singleton(), mq=mq)
        _relay_task = asyncio.create_task(_relay.run())

    logger.info("asyncflow_api_started", extra={"rabbitmq_available": mq_available})
    try:
        yield
    finally:
        if _relay is not None:
            _relay.stop()
        if _relay_task is not None:
            await asyncio.wait([_relay_task], timeout=10)
        await mq.close()
        await dispose_engine()
        logger.info("asyncflow_api_stopped")


app = FastAPI(
    title="AsyncFlow",
    description="Async task processing system with FastAPI, RabbitMQ, and PostgreSQL",
    version="0.1.0",
    lifespan=lifespan,
)


@app.exception_handler(AsyncFlowError)
async def asyncflow_error_handler(_request: Request, exc: AsyncFlowError) -> JSONResponse:
    """Map our domain errors to clean, consistent JSON (no internals leaked)."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": exc.code, "message": exc.message, "details": exc.details},
    )


@app.exception_handler(Exception)
async def unhandled_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    """Last-resort handler: log the full traceback server-side, return a
    generic message to the client. Never echo str(exc) back to the caller --
    it can contain internal details (DSNs, SQL, file paths)."""
    logger.exception("unhandled_exception", exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={"code": "internal_error", "message": "An unexpected error occurred"},
    )


@app.get("/health", tags=["health"])
async def health() -> dict:
    """Liveness/readiness probe: reports DB and RabbitMQ connectivity."""
    db_ok = True
    try:
        session_factory = get_session_factory_singleton()
        async with session_factory() as session:
            from sqlalchemy import text

            await session.execute(text("SELECT 1"))
    except Exception:
        db_ok = False

    return {
        "status": "ok" if db_ok and mq.is_connected else "degraded",
        "database": "ok" if db_ok else "unavailable",
        "rabbitmq": "ok" if mq.is_connected else "unavailable",
    }


app.include_router(router, tags=["v1 - Tasks"])


def main() -> None:
    """Run the API server (console-script entry point)."""
    import uvicorn

    uvicorn.run(app, host=settings.api_host, port=8000)


if __name__ == "__main__":
    main()
