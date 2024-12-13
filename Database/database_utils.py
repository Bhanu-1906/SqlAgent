from  mysql.connector import connect
from config.settings import AppSettings
def get_db_connection():
    return connect(
        host=AppSettings.DB_HOST,
        user=AppSettings.DB_USER,
        password=AppSettings.DB_PASSWORD,
        database=AppSettings.DB_NAME
    )