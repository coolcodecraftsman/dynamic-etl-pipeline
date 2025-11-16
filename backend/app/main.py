from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core.database import engine
from app.models import Base

from app.api.v1.routes_upload import router as upload_router
from app.api.v1.routes_schema import router as schema_router
from app.api.v1.routes_files import router as files_router
from app.api.v1.routes_health import router as health_router


def get_application() -> FastAPI:
    app = FastAPI(
        title=settings.PROJECT_NAME,
        version="0.1.0",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.on_event("startup")
    async def startup_event():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    @app.get("/health")
    async def health_check_root():
        return {"status": "ok"}

    app.include_router(health_router, prefix="/api/v1")
    app.include_router(upload_router, prefix=settings.API_V1_PREFIX)
    app.include_router(schema_router, prefix=settings.API_V1_PREFIX)
    app.include_router(files_router, prefix=settings.API_V1_PREFIX)

    return app


app = get_application()
