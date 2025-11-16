import asyncio
from app.core.database import engine
from app.models import Base

async def init():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)   # (optional) clean
        await conn.run_sync(Base.metadata.create_all) # create tables

    print("Tables created successfully.")

if __name__ == "__main__":
    asyncio.run(init())
