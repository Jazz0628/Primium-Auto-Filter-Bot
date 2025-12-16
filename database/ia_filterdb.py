import motor.motor_asyncio
from umongo import Instance, Document, fields
# --- THE NECESSARY CHANGE IS HERE ---
from umongo.frameworks.motor_asyncio import MotorAsyncIO 
# ------------------------------------
import datetime
from info import DATABASE_NAME, COLLECTION_NAME
import logging

# Set up logging for this file
logger = logging.getLogger(__name__)


class DummyClient:
    """Placeholder client before initialization."""
    def __init__(self):
        self.db = None 

# The placeholder object is created
client = DummyClient()

def get_db_instance():
    """Returns the currently active umongo Instance."""
    if client.db is None:
        logger.error("Attempted to access database before initialization!")
        raise RuntimeError("Database not initialized.")
    return client.db
    
async def initialize_umongo(motor_client: motor.motor_asyncio.AsyncIOMotorClient):
    """Initializes the umongo framework with the motor client and sets the database."""
    
    client.db = motor_client[DATABASE_NAME] 
    
    global umongo_instance
    umongo_instance = Instance(MotorAsyncIO(client.db))
    
    logger.info(f"Umongo Instance initialized for DB: {DATABASE_NAME}")


# We need a placeholder for umongo_instance until initialize_umongo runs
class _PlaceholderInstance:
    def register(self, doc):
        return doc
umongo_instance = _PlaceholderInstance()


@umongo_instance.register
class Media(Document):
    file_id = fields.StrField(attribute='_id')
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)
    media_group_id = fields.IntField(allow_none=True)
    is_group = fields.BooleanField(default=False)
    
    class Meta:
        collection_name = COLLECTION_NAME
        indexes = [
            {"key": "file_name", "unique": False},
            {"key": [("file_name", 1), ("caption", 1)], "default_language": "english"},
        ]

    @classmethod
    async def ensure_indexes(cls):
        await cls.collection.create_indexes(cls.Meta.indexes)


class Media(Media):
    class Meta:
        # This line will use the get_db_instance function to find the collection
        collection = get_db_instance()[COLLECTION_NAME] 
        pass
