from struct import pack
import re
import base64
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
# REMOVED: from motor.motor_asyncio import AsyncIOMotorClient # No longer needed here
from marshmallow.exceptions import ValidationError
# REMOVED: FILES_DATABASE, DATABASE_URI # No longer needed here
from info import DATABASE_NAME, COLLECTION_NAME, MAX_BTN

# ----------------- NEW DYNAMIC DB IMPORTS -----------------
from database.db_manager import get_current_db, active_db_uri, check_db_size
# ----------------------------------------------------------

# We use a placeholder client and instance that will be updated later
# We only need the instance to register the Document
# The actual connection will be created and switched by get_db_instance()
class DummyClient:
    def __init__(self):
        self.is_connected = False
        self.is_closed = True
        self[DATABASE_NAME] = None

client = DummyClient() 
mydb = client[DATABASE_NAME]
instance = Instance.from_db(mydb)


# Function to ensure the database instance uses the currently active connection
async def get_db_instance():
    """Gets the currently active database object and updates the umongo instance."""
    mydb = await get_current_db()
    # This is the crucial step for umongo: setting the active database
    instance.set_db(mydb) 
    return mydb

@instance.register
class Media(Document):
    file_id = fields.StrField(attribute="_id")
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)
    file_type = fields.StrField(allow_none=True)

    class Meta:
        indexes = ("$file_name",)
        collection_name = COLLECTION_NAME

# The core functions now rely on the dynamic connection:

async def get_files_db_size():
    """Get the size of the currently active database."""
    # Note: This uses the db_manager's check_db_size for the current URI
    if not active_db_uri:
        # Fallback if somehow not initialized
        await get_db_instance() 
    return await check_db_size(active_db_uri)


async def save_file(media):
    """Save file in database"""
    # Ensure umongo instance is using the latest active DB before saving
    await get_db_instance() 

    # TODO: Find better way to get same file_id for same media to avoid duplicates
    file_id, file_ref = unpack_new_file_id(media.file_id)
    file_name = re.sub(r"(_|\-|\.|\+)", " ", str(media.file_name))
    try:
        file = Media(
            file_id=file_id,
            file_ref=file_ref,
            file_name=file_name,
            file_size=media.file_size,
            mime_type=media.mime_type,
            caption=media.caption.html if media.caption else None,
            file_type=media.mime_type.split("/")[0],
        )
    except ValidationError:
        print("Error occurred while saving file in database")
        return "err"
    else:
        try:
            await file.commit()
        except DuplicateKeyError:
            print(
                f'{getattr(media, "file_name", "NO_FILE")} is already saved in database'
            )
            return "dup"
        else:
            print(f'{getattr(media, "file_name", "NO_FILE")} is saved to database')
            return "suc"


async def get_search_results(query, max_results=MAX_BTN, offset=0, lang=None):
    # Ensure umongo instance is using the latest active DB before querying
    await get_db_instance() 
    
    query = query.strip()
    if not query:
        raw_pattern = "."
    elif " " not in query:
        raw_pattern = r"(\b|[\.\+\-_])" + query + r"(\b|[\.\+\-_])"
    else:
        raw_pattern = query.replace(" ", r".*[\s\.\+\-_]")
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except:
        regex = query
    filter = {"file_name": regex}
    cursor = Media.find(filter)
    cursor.sort("$natural", -1)
    if lang:
        lang_files = [file async for file in cursor if lang in file.file_name.lower()]
        files = lang_files[offset:][:max_results]
        total_results = len(lang_files)
        next_offset = offset + max_results
        if next_offset >= total_results:
            next_offset = ""
        return files, next_offset, total_results
    cursor.skip(offset).limit(max_results)
    files = await cursor.to_list(length=max_results)
    total_results = await Media.count_documents(filter)
    next_offset = offset + max_results
    if next_offset >= total_results:
        next_offset = ""
    return files, next_offset, total_results


async def get_bad_files(query, file_type=None, offset=0, filter=False):
    # Ensure umongo instance is using the latest active DB before querying
    await get_db_instance() 

    query = query.strip()
    if not query:
        raw_pattern = "."
    elif " " not in query:
        raw_pattern = r"(\b|[\.\+\-_])" + query + r"(\b|[\.\+\-_])"
    else:
        raw_pattern = query.replace(" ", r".*[\s\.\+\-_]")
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except:
        return []
    filter = {"file_name": regex}
    if file_type:
        filter["file_type"] = file_type
    total_results = await Media.count_documents(filter)
    cursor = Media.find(filter)
    cursor.sort("$natural", -1)
    files = await cursor.to_list(length=total_results)
    return files, total_results


async def get_file_details(query):
    # Ensure umongo instance is using the latest active DB before querying
    await get_db_instance() 

    filter = {"file_id": query}
    cursor = Media.find(filter)
    filedetails = await cursor.to_list(length=1)
    return filedetails


def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")


def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")


def unpack_new_file_id(new_file_id):
    """Return file_id, file_ref"""
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash,
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref
