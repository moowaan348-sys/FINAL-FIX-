import os
from motor.motor_asyncio import AsyncIOMotorClient

mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
_db_name = os.environ.get('DB_NAME', 'dataline_store')

client = AsyncIOMotorClient(mongo_url)
db = client[_db_name]

# Collections
users_col = db['users']
lines_col = db['lines']
topups_col = db['topups']
orders_col = db['orders']
bases_col = db['bases']
admins_col = db['admin_users']
settings_col = db['settings']
notifications_col = db['notifications']
bin_cache_col = db['bin_cache']
bin_search_log_col = db['bin_search_log']


async def ensure_indexes():
    await users_col.create_index('telegram_user_id', unique=True)
    await lines_col.create_index('id', unique=True)
    await lines_col.create_index([('status', 1), ('base_name', 1)])
    await lines_col.create_index('bin')
    await lines_col.create_index('country')
    await lines_col.create_index('dedupe_key', unique=True, sparse=True)
    await topups_col.create_index('id', unique=True)
    await topups_col.create_index([('status', 1), ('crypto_type', 1)])
    await topups_col.create_index('telegram_user_id')
    await orders_col.create_index('id', unique=True)
    await orders_col.create_index('telegram_user_id')
    await orders_col.create_index([('check_status', 1), ('scheduled_check_at', 1)])
    await admins_col.create_index('username', unique=True)
    await notifications_col.create_index([('created_at', 1)])
    await notifications_col.create_index([('event_type', 1)])
    await bin_search_log_col.create_index([('searched_at', -1)])
    await bin_search_log_col.create_index('bin')
