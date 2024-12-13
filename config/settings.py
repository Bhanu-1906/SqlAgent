from pydantic_settings import BaseSettings, SettingsConfigDict
 
class AppSettings(BaseSettings):
    GEMINI_API_KEY: str
    DB_HOST: str
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str
    DB_PORT_NUMBER:str
    
    model_config = SettingsConfigDict(
        env_file=".env",    # Path to the `.env` file.
        extra="ignore"      # Ignore extra environment variables not listed here.
    )
 
# Initialize settings
Appsettings = AppSettings()