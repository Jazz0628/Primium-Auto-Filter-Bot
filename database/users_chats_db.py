import datetime
import pytz
# REMOVED: from motor.motor_asyncio import AsyncIOMotorClient # Not needed
# from info import SETTINGS, ... # Removed the commented-out old imports
from info import *

# ----------------- NEW DYNAMIC DB IMPORTS -----------------
from database.db_manager import get_current_db, get_db_client, check_db_size
# ----------------------------------------------------------

# REMOVED: client = AsyncIOMotorClient(DATABASE_URI)
# REMOVED: mydb = client[DATABASE_NAME]


class Database:
    def __init__(self):
        # Collections are None until initialize() is called
        self.col = None
        self.grp = None
        self.misc = None
        self.verify_id = None
        self.users = None
        self.req = None
        self.mGrp = None
        self.pmMode = None
        self.jisshu_ads_link = None
        self.movies_update_channel = None
        self.botcol = None
        
    async def initialize(self):
        """Initializes the database connection and collections dynamically."""
        client = await get_db_client() # Get the currently active client
        mydb = client[DATABASE_NAME] # Get the database object
        
        self.col = mydb.users
        self.grp = mydb.groups
        self.misc = mydb.misc
        self.verify_id = mydb.verify_id
        self.users = mydb.uersz
        self.req = mydb.requests
        self.mGrp = mydb.mGrp
        self.pmMode = mydb.pmMode
        self.jisshu_ads_link = mydb.jisshu_ads_link
        self.movies_update_channel = mydb.movies_update_channel
        self.botcol = mydb.botcol

    default = {
        "spell_check": SPELL_CHECK,
        "auto_filter": AUTO_FILTER,
        "file_secure": PROTECT_CONTENT,
        "auto_delete": AUTO_DELETE,
        "template": IMDB_TEMPLATE,
        "caption": FILE_CAPTION,
        "tutorial": TUTORIAL,
        "tutorial_2": TUTORIAL_2,
        "tutorial_3": TUTORIAL_3,
        "shortner": SHORTENER_WEBSITE,
        "api": SHORTENER_API,
        "shortner_two": SHORTENER_WEBSITE2,
        "api_two": SHORTENER_API2,
        "log": LOG_VR_CHANNEL,
        "imdb": IMDB,
        "fsub_id": AUTH_CHANNEL,
        "link": LINK_MODE,
        "is_verify": IS_VERIFY,
        "verify_time": TWO_VERIFY_GAP,
        "shortner_three": SHORTENER_WEBSITE3,
        "api_three": SHORTENER_API3,
        "third_verify_time": THREE_VERIFY_GAP,
    }

    def new_user(self, id, name):
        return dict(
            id=id, name=name, point=0, ban_status=dict(is_banned=False, ban_reason="")
        )

    async def get_settings(self, group_id):
        # All functions will now use the currently set connection
        await self.ensure_collections()
        chat = await self.grp.find_one({"id": int(group_id)})
        if chat and "settings" in chat:
            return chat["settings"]
        else:
            return self.default.copy()

    async def find_join_req(self, id):
        await self.ensure_collections()
        return bool(await self.req.find_one({"id": id}))

    async def add_join_req(self, id):
        await self.ensure_collections()
        await self.req.insert_one({"id": id})

    async def del_join_req(self):
        await self.ensure_collections()
        await self.req.drop()

    def new_group(self, id, title):
        return dict(id=id, title=title, chat_status=dict(is_disabled=False, reason=""))

    async def add_user(self, id, name):
        await self.ensure_collections()
        user = self.new_user(id, name)
        await self.col.insert_one(user)

    async def update_point(self, id):
        await self.ensure_collections()
        await self.col.update_one({"id": id}, {"$inc": {"point": 100}})
        point = (await self.col.find_one({"id": id}))["point"]
        if point >= PREMIUM_POINT:
            seconds = REF_PREMIUM * 24 * 60 * 60
            oldEx = await self.users.find_one({"id": id})
            if oldEx:
                expiry_time = oldEx["expiry_time"] + datetime.timedelta(seconds=seconds)
            else:
                expiry_time = datetime.datetime.now() + datetime.timedelta(
                    seconds=seconds
                )
            user_data = {"id": id, "expiry_time": expiry_time}
            await db.update_user(user_data)
            await self.col.update_one({"id": id}, {"$set": {"point": 0}})

    async def get_point(self, id):
        await self.ensure_collections()
        newPoint = await self.col.find_one({"id": id})
        return newPoint["point"] if newPoint else None

    async def is_user_exist(self, id):
        await self.ensure_collections()
        user = await self.col.find_one({"id": int(id)})
        return bool(user)

    async def total_users_count(self):
        await self.ensure_collections()
        count = await self.col.count_documents({})
        return count

    async def get_all_users(self):
        await self.ensure_collections()
        return self.col.find({})

    async def delete_user(self, user_id):
        await self.ensure_collections()
        await self.col.delete_many({"id": int(user_id)})

    async def delete_chat(self, id):
        await self.ensure_collections()
        await self.grp.delete_many({"id": int(id)})

    async def get_banned(self):
        await self.ensure_collections()
        users = self.col.find({"ban_status.is_banned": True})
        chats = self.grp.find({"chat_status.is_disabled": True})
        b_chats = [chat["id"] async for chat in chats]
        b_users = [user["id"] async for user in users]
        return b_users, b_chats

    async def add_chat(self, chat, title):
        await self.ensure_collections()
        chat = self.new_group(chat, title)
        await self.grp.insert_one(chat)

    async def get_chat(self, chat):
        await self.ensure_collections()
        chat = await self.grp.find_one({"id": int(chat)})
        return False if not chat else chat.get("chat_status")

    async def update_settings(self, id, settings):
        await self.ensure_collections()
        await self.grp.update_one({"id": int(id)}, {"$set": {"settings": settings}})

    async def total_chat_count(self):
        await self.ensure_collections()
        count = await self.grp.count_documents({})
        return count

    async def get_all_chats(self):
        await self.ensure_collections()
        return self.grp.find({})

    async def get_db_size(self):
        """Returns the size of the currently active database."""
        await self.ensure_collections()
        from database.db_manager import active_db_uri
        if not active_db_uri:
            # Fallback if somehow not initialized
            await self.initialize() 
            
        return await check_db_size(active_db_uri)

    async def get_notcopy_user(self, user_id):
        await self.ensure_collections()
        user_id = int(user_id)
        user = await self.misc.find_one({"user_id": user_id})
        ist_timezone = pytz.timezone("Asia/Kolkata")
        if not user:
            res = {
                "user_id": user_id,
                "last_verified": datetime.datetime(
                    2020, 5, 17, 0, 0, 0, tzinfo=ist_timezone
                ),
                "second_time_verified": datetime.datetime(
                    2019, 5, 17, 0, 0, 0, tzinfo=ist_timezone
                ),
            }
            await self.misc.insert_one(res)
            # Fetch the inserted document to return it
            user = await self.misc.find_one({"user_id": user_id}) 
        return user

    async def update_notcopy_user(self, user_id, value: dict):
        await self.ensure_collections()
        user_id = int(user_id)
        myquery = {"user_id": user_id}
        newvalues = {"$set": value}
        return await self.misc.update_one(myquery, newvalues)

    async def is_user_verified(self, user_id):
        await self.ensure_collections()
        user = await self.get_notcopy_user(user_id)
        try:
            pastDate = user["last_verified"]
        except Exception:
            user = await self.get_notcopy_user(user_id)
            pastDate = user["last_verified"]
        ist_timezone = pytz.timezone("Asia/Kolkata")
        pastDate = pastDate.astimezone(ist_timezone)
        current_time = datetime.datetime.now(tz=ist_timezone)
        seconds_since_midnight = (
            current_time
            - datetime.datetime(
                current_time.year,
                current_time.month,
                current_time.day,
                0,
                0,
                0,
                tzinfo=ist_timezone,
            )
        ).total_seconds()
        time_diff = current_time - pastDate
        total_seconds = time_diff.total_seconds()
        return total_seconds <= seconds_since_midnight

    async def user_verified(self, user_id):
        await self.ensure_collections()
        user = await self.get_notcopy_user(user_id)
        try:
            pastDate = user["second_time_verified"]
        except Exception:
            user = await self.get_notcopy_user(user_id)
            pastDate = user["second_time_verified"]
        ist_timezone = pytz.timezone("Asia/Kolkata")
        pastDate = pastDate.astimezone(ist_timezone)
        current_time = datetime.datetime.now(tz=ist_timezone)
        seconds_since_midnight = (
            current_time
            - datetime.datetime(
                current_time.year,
                current_time.month,
                current_time.day,
                0,
                0,
                0,
                tzinfo=ist_timezone,
            )
        ).total_seconds()
        time_diff = current_time - pastDate
        total_seconds = time_diff.total_seconds()
        return total_seconds <= seconds_since_midnight

    async def use_second_shortener(self, user_id, time):
        await self.ensure_collections()
        user = await self.get_notcopy_user(user_id)
        if not user.get("second_time_verified"):
            ist_timezone = pytz.timezone("Asia/Kolkata")
            await self.update_notcopy_user(
                user_id,
                {
                    "second_time_verified": datetime.datetime(
                        2019, 5, 17, 0, 0, 0, tzinfo=ist_timezone
                    )
                },
            )
            user = await self.get_notcopy_user(user_id)
        if await self.is_user_verified(user_id):
            try:
                pastDate = user["last_verified"]
            except Exception:
                user = await self.get_notcopy_user(user_id)
                pastDate = user["last_verified"]
            ist_timezone = pytz.timezone("Asia/Kolkata")
            pastDate = pastDate.astimezone(ist_timezone)
            current_time = datetime.datetime.now(tz=ist_timezone)
            time_difference = current_time - pastDate
            if time_difference > datetime.timedelta(seconds=time):
                pastDate = user["last_verified"].astimezone(ist_timezone)
                second_time = user["second_time_verified"].astimezone(ist_timezone)
                return second_time < pastDate
        return False

    async def use_third_shortener(self, user_id, time):
        await self.ensure_collections()
        user = await self.get_notcopy_user(user_id)
        if not user.get("third_time_verified"):
            ist_timezone = pytz.timezone("Asia/Kolkata")
            await self.update_notcopy_user(
                user_id,
                {
                    "third_time_verified": datetime.datetime(
                        2018, 5, 17, 0, 0, 0, tzinfo=ist_timezone
                    )
                },
            )
            user = await self.get_notcopy_user(user_id)
        if await self.user_verified(user_id):
            try:
                pastDate = user["second_time_verified"]
            except Exception:
                user = await self.get_notcopy_user(user_id)
                pastDate = user["second_time_verified"]
            ist_timezone = pytz.timezone("Asia/Kolkata")
            pastDate = pastDate.astimezone(ist_timezone)
            current_time = datetime.datetime.now(tz=ist_timezone)
            time_difference = current_time - pastDate
            if time_difference > datetime.timedelta(seconds=time):
                pastDate = user["second_time_verified"].astimezone(ist_timezone)
                second_time = user["third_time_verified"].astimezone(ist_timezone)
                return second_time < pastDate
        return False

    async def create_verify_id(self, user_id: int, hash):
        await self.ensure_collections()
        res = {"user_id": user_id, "hash": hash, "verified": False}
        return await self.verify_id.insert_one(res)

    async def get_verify_id_info(self, user_id: int, hash):
        await self.ensure_collections()
        return await self.verify_id.find_one({"user_id": user_id, "hash": hash})

    async def update_verify_id_info(self, user_id, hash, value: dict):
        await self.ensure_collections()
        myquery = {"user_id": user_id, "hash": hash}
        newvalues = {"$set": value}
        return await self.verify_id.update_one(myquery, newvalues)

    async def get_user(self, user_id):
        await self.ensure_collections()
        user_data = await self.users.find_one({"id": user_id})
        return user_data

    async def remove_ban(self, id):
        await self.ensure_collections()
        ban_status = dict(is_banned=False, ban_reason="")
        await self.col.update_one({"id": id}, {"$set": {"ban_status": ban_status}})

    async def ban_user(self, user_id, ban_reason="No Reason"):
        await self.ensure_collections()
        ban_status = dict(is_banned=True, ban_reason=ban_reason)
        await self.col.update_one({"id": user_id}, {"$set": {"ban_status": ban_status}})

    async def get_ban_status(self, id):
        await self.ensure_collections()
        default = dict(is_banned=False, ban_reason="")
        user = await self.col.find_one({"id": int(id)})
        if not user:
            return default
        return user.get("ban_status", default)

    async def update_user(self, user_data):
        await self.ensure_collections()
        await self.users.update_one(
            {"id": user_data["id"]}, {"$set": user_data}, upsert=True
        )

    async def get_expired(self, current_time):
        await self.ensure_collections()
        expired_users = []
        if data := self.users.find({"expiry_time": {"$lt": current_time}}):
            async for user in data:
                expired_users.append(user)
        return expired_users

    async def has_premium_access(self, user_id):
        await self.ensure_collections()
        user_data = await self.get_user(user_id)
        if user_data:
            expiry_time = user_data.get("expiry_time")
            if expiry_time is None:
                # User previously used the free trial, but it has ended.
                return False
            elif (
                isinstance(expiry_time, datetime.datetime)
                and datetime.datetime.now() <= expiry_time
            ):
                return True
            else:
                await self.users.update_one(
                    {"id": user_id}, {"$set": {"expiry_time": None}}
                )
        return False

    async def check_remaining_uasge(self, user_id):
        await self.ensure_collections()
        user_id = user_id
        user_data = await self.get_user(user_id)
        expiry_time = user_data.get("expiry_time")
        # Calculate remaining time
        remaining_time = expiry_time - datetime.datetime.now()
        return remaining_time

    async def all_premium_users(self):
        await self.ensure_collections()
        count = await self.users.count_documents(
            {"expiry_time": {"$gt": datetime.datetime.now()}}
        )
        return count

    async def update_one(self, filter_query, update_data):
        await self.ensure_collections()
        try:
            # Assuming self.client and self.users are set up properly
            result = await self.users.update_one(filter_query, update_data)
            return result.matched_count == 1
        except Exception as e:
            print(f"Error updating document: {e}")
            return False

    async def remove_premium_access(self, user_id):
        await self.ensure_collections()
        return await self.update_one({"id": user_id}, {"$set": {"expiry_time": None}})

    async def check_trial_status(self, user_id):
        await self.ensure_collections()
        user_data = await self.get_user(user_id)
        if user_data:
            return user_data.get("has_free_trial", False)
        return False

    # Free Trail Remove Logic
    async def reset_free_trial(self, user_id=None):
        await self.ensure_collections()
        if user_id is None:
            # Reset for all users
            update_data = {"$set": {"has_free_trial": False}}
            result = await self.users.update_many(
                {}, update_data
            )  # Empty query to match all users
            return result.modified_count
        else:
            # Reset for a specific user
            update_data = {"$set": {"has_free_trial": False}}
            result = await self.users.update_one({"id": user_id}, update_data)
            return (
                1 if result.modified_count > 0 else 0
            )  # Return 1 if updated, 0 if not

    async def give_free_trial(self, user_id):
        await self.ensure_collections()
        # await set_free_trial_status(user_id)
        user_id = user_id
        seconds = 5 * 60
        expiry_time = datetime.datetime.now() + datetime.timedelta(seconds=seconds)
        user_data = {"id": user_id, "expiry_time": expiry_time, "has_free_trial": True}
        await self.users.update_one({"id": user_id}, {"$set": user_data}, upsert=True)

    # JISSHU BOTS
    async def jisshu_set_ads_link(self, link):
        await self.ensure_collections()
        await self.jisshu_ads_link.update_one({}, {"$set": {"link": link}}, upsert=True)

    async def jisshu_get_ads_link(self):
        await self.ensure_collections()
        link = await self.jisshu_ads_link.find_one({})
        if link is not None:
            return link.get("link")
        else:
            return None

    async def jisshu_del_ads_link(self):
        await self.ensure_collections()
        try:
            isDeleted = await self.jisshu_ads_link.delete_one({})
            if isDeleted.deleted_count > 0:
                return True
            else:
                return False
        except Exception as e:
            print(f"Got err in db set : {e}")
            return False

    async def get_send_movie_update_status(self, bot_id):
        await self.ensure_collections()
        bot = await self.botcol.find_one({"id": bot_id})
        if bot and bot.get("movie_update_feature"):
            return bot["movie_update_feature"]
        else:
            return IS_SEND_MOVIE_UPDATE

    async def update_send_movie_update_status(self, bot_id, enable):
        await self.ensure_collections()
        bot = await self.botcol.find_one({"id": int(bot_id)})
        if bot:
            await self.botcol.update_one(
                {"id": int(bot_id)}, {"$set": {"movie_update_feature": enable}}
            )
        else:
            await self.botcol.insert_one(
                {"id": int(bot_id), "movie_update_feature": enable}
            )

    async def get_pm_search_status(self, bot_id):
        await self.ensure_collections()
        bot = await self.botcol.find_one({"id": bot_id})
        if bot and bot.get("bot_pm_search"):
            return bot["bot_pm_search"]
        else:
            return IS_PM_SEARCH

    async def update_pm_search_status(self, bot_id, enable):
        await self.ensure_collections()
        bot = await self.botcol.find_one({"id": int(bot_id)})
        if bot:
            await self.botcol.update_one(
                {"id": int(bot_id)}, {"$set": {"bot_pm_search": enable}}
            )
        else:
            await self.botcol.insert_one({"id": int(bot_id), "bot_pm_search": enable})

    async def movies_update_channel_id(self, id=None):
        await self.ensure_collections()
        if id is None:
            myLinks = await self.movies_update_channel.find_one({})
            if myLinks is not None:
                return myLinks.get("id")
            else:
                return None
        return await self.movies_update_channel.update_one(
            {}, {"$set": {"id": id}}, upsert=True
        )

    async def reset_group_settings(self, id):
        await self.ensure_collections()
        await self.grp.update_one({"id": int(id)}, {"$set": {"settings": self.default}})

    async def ensure_collections(self):
        """Helper to ensure collections are initialized if called before db.initialize()"""
        if self.col is None:
            await self.initialize()


db = Database()
