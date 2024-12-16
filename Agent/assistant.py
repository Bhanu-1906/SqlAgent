from core.llm_manager import LLMManger
from core.state_models import State
from Agent.chat_history import ChatHistory
from Tools.query_executor import query_executor
from Tools.query_generator_tool import query_generator
from Prompt.prompt_loader import PromptLoader
from langchain_core.messages import HumanMessage
from Database.database_utils import AppSettings
 
llm = LLMManger()
 
def assistant(state: State):
    user_id=1234
    chat = ChatHistory(host="localhost", user="root", password="Alliswell#1906", database="events_for_redis")
    previous_conversations = chat.fetch_previous_conversations(user_id)    
    user_details = previous_conversations[0] if isinstance(previous_conversations[0], dict) else {}
    previous_messages = previous_conversations[1] if isinstance(previous_conversations[1], list) else []
 
    if not user_details or user_details.get("name") == "N/A":
        user_info = "User details are not available."
    else:
        user_info = (
            f"User name is {user_details.get('name')}, "
            f"user likes {user_details.get('likes')}, "
            f"user dislikes {user_details.get('dislikes')}, "
            f"user age is {user_details.get('age')}."
        )
 
    state['messages'] = [{'role': 'user', 'content': user_info}]  + state.get('messages', [])
    tools = [query_executor,query_generator]
    llm_with_tool = llm.bind_tools(tools= tools)
    llm_with_tool = llm.bind_tools(tools=tools)
 
    if state['messages'] and isinstance(state['messages'][-1], HumanMessage):
        input_message = state['messages'][-1].content
    elif state['messages'] and isinstance(state['messages'][-1], dict):
        input_message = state['messages'][-1].get('content', '')
    else:
        input_message = ''
 
    system_message = PromptLoader().get_prompt("core_agent_prompt")
 
    try:
        final_response = llm_with_tool.invoke([system_message]  + state['messages'])
    except Exception as e:
        raise RuntimeError("Error invoking LLM:") from e
 
    chat.insert(user_id, input_message, final_response.content, state)
 
    return {'messages':[llm_with_tool.invoke([system_message] + state['messages'])]}
 
 