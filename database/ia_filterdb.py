import motor.motor_asyncio
from umongo import Instance, Document, fields
from umongo.frameworks import MotorAsyncIO
import datetime
from info import DATABASE_NAME, COLLECTION_NAME
import logging

# Set up logging for this file
logger = logging.getLogger(__name__)

# --- FIX START ---

class DummyClient:
    """Placeholder client before initialization."""
    def __init__(self):
        # Use a standard attribute (.db) instead of dictionary access ([])
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
    
    # 1. Select the correct database and assign it to the client's attribute
    client.db = motor_client[DATABASE_NAME] 
    
    # 2. Initialize umongo instance using the selected database
    global umongo_instance
    umongo_instance = Instance(MotorAsyncIO(client.db))
    
    logger.info(f"Umongo Instance initialized for DB: {DATABASE_NAME}")

# --- FIX END ---


# The rest of the Document definitions remains the same
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
        # Use the collection name from config
        collection_name = COLLECTION_NAME
        # Ensure indexes are created for faster searching
        indexes = [
            # For exact file_name search
            {"key": "file_name", "unique": False},
            # For text search queries
            {"key": [("file_name", 1), ("caption", 1)], "default_language": "english"},
        ]

    # Helper method for umongo to ensure indexes exist
    @classmethod
    async def ensure_indexes(cls):
        await cls.collection.create_indexes(cls.Meta.indexes)


# We redefine Media as a MotorAsyncIO document using the dynamically acquired db instance
class Media(Media):
    class Meta:
        collection = get_db_instance()[COLLECTION_NAME] 
        # Re-initialize the instance for Umongo to use the real collection
        umongo_instance = Instance(MotorAsyncIO(get_db_instance()))
