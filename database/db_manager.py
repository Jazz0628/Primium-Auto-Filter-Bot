import os
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from info import DATABASE_NAME # Use the name from your original info.py

logger = logging.getLogger(__name__)

# --- Configuration ---
SIZE_LIMIT_MB = 315
# 315 MB converted to bytes
SIZE_LIMIT_BYTES = SIZE_LIMIT_MB * 1024 * 1024 

# Collect all potential database URIs from environment variables (DB_URI_1 to DB_URI_10)
DATABASE_URIS = [
    os.environ.get(f"DB_URI_{i}") for i in range(1, 11)
]
# Filter out any None or empty strings from the list
DATABASE_URIS = [uri for uri in DATABASE_URIS if uri]

# Cache the active client and URI
active_client = None
active_db_uri = None

async def check_db_size(uri: str) -> int:
    """Connects to a single MongoDB URI and returns its dataSize in bytes."""
    try:
        # Use a short timeout to quickly check connection status
        client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=5000)
        # Force a connection check
        await client.admin.command('ping') 
        
        db = client[DATABASE_NAME]
        
        # 'dbstats' returns dataSize in bytes
        stats = await db.command("dbstats")
        size_bytes = stats.get("dataSize", 0)
        
        # Close connection immediately after checking
        client.close()
        return size_bytes
        
    except Exception as e:
        # If connection fails (e.g., DB is down), treat it as 'full' 
        # so the bot attempts the next URI.
        logger.error(f"Failed to check DB size at {uri[:20]}...: {e}")
        return float('inf') 

async def get_active_db_uri() -> str:
    """Finds the first available (not full) database URI."""
    global active_db_uri
    
    # 1. Check current active URI first for efficiency
    if active_db_uri:
        current_size = await check_db_size(active_db_uri)
        if current_size < SIZE_LIMIT_BYTES:
            return active_db_uri
        else:
            logger.warning(
                f"Active DB is full ({current_size / 1024 / 1024:.2f} MB). Switching..."
            )

    # 2. Iterate through all URIs to find the first available one
    for uri in DATABASE_URIS:
        size = await check_db_size(uri)
        
        if size < SIZE_LIMIT_BYTES:
            active_db_uri = uri
            logger.info(
                f"Switched to new active DB: {active_db_uri[:20]}... (Size: {size / 1024 / 1024:.2f} MB)"
            )
            return active_db_uri
            
    # 3. Handle case where all databases are full
    if DATABASE_URIS:
        # Revert to the last URI even though it's full (as a final attempt)
        active_db_uri = DATABASE_URIS[-1]
        logger.error("ALL CONFIGRURED DATABASES ARE FULL! Using the last one.")
        return active_db_uri
    
    # 4. Handle case where no URIs are configured
    raise EnvironmentError("No MongoDB URIs found in environment variables (DB_URI_1 to DB_URI_10).")

async def get_db_client() -> AsyncIOMotorClient:
    """Returns the single, active Motor client instance."""
    global active_client, active_db_uri
    
    new_uri = await get_active_db_uri()
    
    # If the URI is the same, return the existing client connection
    if active_client and active_db_uri == new_uri:
        return active_client
        
    # If a new URI was selected or it's the first connection
    if active_client:
        active_client.close() # Close the old client connection
        
    active_db_uri = new_uri
    # Create the new client connection
    active_client = AsyncIOMotorClient(active_db_uri)
    return active_client

# This is what other modules will call to get the current connection
async def get_current_db():
    client = await get_db_client()
    return client[DATABASE_NAME]
