from pydantic_settings import BaseSettings, SettingsConfigDict
 
class AppSettings(BaseSettings):
    GEMINI_API_KEY: str
    DB_HOST: str
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME_CHATHISTORY: str
    DB_DIALECT: str
    DB_NAME: str
    DB_PORT_NUMBER:str
   
    model_config = SettingsConfigDict(
        env_file=".env",  
        extra="ignore"  
    )
 
# Initialize settings
Appsettings = AppSettings()
 
