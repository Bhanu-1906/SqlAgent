import json
from .validate_refine import validate_and_refine_queries
from langchain_core.tools import tool
from Database import DatabaseConnect
from core.llm_manager import LLMManger
from Prompt.prompt_loader import PromptLoader


conn = DatabaseConnect.DatabaseConnection("root", "Alliswell#1906", "127.0.0.1", "3306", "mysql")
res = conn.get_all_databases_metadata()
llm = LLMManger()



@tool
def query_generator(user_query: str):
    """
    Generates an SQL query based on the user's natural language input.
 
    - Dynamically fetches database schema (`table_info`) to ensure query accuracy.
    - Supports SELECT, INSERT, UPDATE, and DELETE queries tailored to the user's request.
    - Automatically validates the generated query for correctness and relevance.
    - Outputs the query and the target database in a structured format for execution.
 
    Args:
        user_query (str): A natural language description of the SQL operation.
 
    Returns:
        dict:
            - `query` (str): The generated SQL query.
            - `database_name` (str): The best-matching database for the query.
            - `status` (str, optional): Status or validation feedback if the query cannot be executed.
            - For errors: A dictionary containing the error message.
    Note:
        This tool will automatically fetch the database schema (`table_info`) dynamically without requiring additional input.
    """
    # sql_prompt = (
    #     "<database>mysql\n"
    #     "<table_info>\n"
    #     "Based on the following database schema, find the best-matching database and generate an SQL query:\n"
    # )
 
    # sql_prompt += (str(fetch()))
    print(res)
    # sql_prompt = (res)
 
    # sql_prompt += (
    #     f"<user_query>\n{user_query}\n"      
    # )
    
    # sql_prompt+=(PromptLoader().get_prompt("query_generator_prompt"))
    sql_prompt = (
        f"<database_schema>\n{res}\n</database_schema>\n"
        f"<user_query>\n{user_query}\n</user_query>\n"
        + PromptLoader().get_prompt("query_generator_prompt")
    )

 
    try:
        llm_response = llm.invoke(sql_prompt).content
        llm_response=llm_response.replace("```json","").replace("```","")
        print(llm_response)
        output = json.loads(llm_response)
 
        if isinstance(output, dict) and "database" in output and "query" in output:
            validation_result = validate_and_refine_queries(
                queries=[output.get("query")],
                user_query=user_query
            )
            if validation_result["action"] == "yes":
                return {
                    "query": output["query"],
                    "database_name": output["database"],
                }
            else:
                return {
                    "status": validation_result["status"]
                }
        else:
            raise ValueError("LLM response did not conform to the required format.")
 
    except json.JSONDecodeError as json_error:
        return {"error": f"JSON parsing error: {str(json_error)}", "user_query": user_query, "messages": [str(json_error)]}
    except Exception as e:
        return {"error": str(e), "user_query": user_query, "messages": [str(e)]}
 
