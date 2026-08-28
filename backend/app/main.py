import dramatiq
from dramatiq.brokers.redis import RedisBroker
from fastapi import FastAPI

from app.api.routes import dlq as dlq_routes
from app.api.routes import pipelines as pipeline_routes
from app.config import settings
from app.observability.logging import configure_logging

configure_logging()

broker = RedisBroker(url=settings.redis_url)
dramatiq.set_broker(broker)

app = FastAPI(title='Cantata Transcription', version='0.1.0')
app.include_router(pipeline_routes.router)
app.include_router(dlq_routes.router)


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok'}
