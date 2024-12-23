from  mysql.connector import connect
from config.settings import AppSettings
Appsettings = AppSettings()
def get_db_connection():
    return connect(
        host=Appsettings.DB_HOST,
        user=Appsettings.DB_USER,
        password=Appsettings.DB_PASSWORD,
        database=Appsettings.DB_NAME_CHATHISTORY
    )
if __name__ == '__main__':
    get_db_connection()