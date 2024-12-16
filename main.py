from Agent.state_graph import graph
from langchain_core.messages import HumanMessage
 
def get_reply():
    print("Chat with the assistant! Type 'q' to quit.\n")
   
    while True:
        user_message = input("You: ")
        if user_message.strip().lower() == 'q':
            print("Goodbye!")
            break
       
        state = {"messages": [HumanMessage(content=user_message)]}
       
        try:
            output_message = graph.invoke(state)
            response = output_message["messages"][-1].content
            print(f"Assistant: {response}")
        except Exception as e:
            response_html = f"An error occurred: {e}"
            print(f"Error: {response_html}")
 
if __name__ == '__main__':
    get_reply()
 
 
