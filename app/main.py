import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.db_config import init_db, close_db
from app.consumer import start_consumer
from app.routes.health import router as health_router
from app.routes.ready import router as ready_router

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db()
    consumer_task = asyncio.create_task(start_consumer())
    logger.info("Media processing service started")
    yield
    # Shutdown
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    await close_db()
    logger.info("Media processing service stopped")


app = FastAPI(
    title="Media Processing Service",
    description="Consumes video.queued events from Kafka and persists processing jobs",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(ready_router)
