
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, START, END,MessagesState
from Tools.query_generator_tool import query_generator
from Tools.query_executor import query_executor
from core.state_models import State
from Agent.assistant import assistant
from langgraph.prebuilt import ToolNode,tools_condition

tools = [query_generator,query_executor]

memory = MemorySaver()
builder = StateGraph(State)

builder.add_node('assistant', assistant)
builder.add_node('tools', ToolNode(tools=tools))

builder.add_edge(START, 'assistant')
builder.add_conditional_edges('assistant', tools_condition)
builder.add_edge('tools', 'assistant')

graph = builder.compile()