import motor.motor_asyncio
from core.config import Config as cfg

client = motor.motor_asyncio.AsyncIOMotorClient(cfg.MONGO_DB)

db = client.testovoe
sample_collection = db.sample_collection
