from langchain_core.tools import Tool
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.runnables import Runnable
from config.settings import Appsettings
 
class LLMManger:
    def __init__(self):
        self.llm = ChatGoogleGenerativeAI(
            model="models/gemini-1.5-flash",
            temperature=1,
            api_key=Appsettings.GEMINI_API_KEY
        )
 
    def get_llm(self):
        return self.llm
 
    def bind_tools(self, tools: list) -> Runnable:
        try:
            return self.llm.bind(tools=tools)
        except Exception as e:
            print(f"Error binding tools: {e}")
            raise
    def invoke(self,message):
        return self.llm.invoke(message)