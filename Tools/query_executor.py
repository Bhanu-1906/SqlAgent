from langchain_core.tools import tool
from Database import DatabaseConnect


@tool
def query_executor(query: str):
    """
    Executes a given SQL query on the database and handles both SELECT and non-SELECT operations.

    - For SELECT queries: Fetches and returns the result set with column names.
    - For non-SELECT queries (e.g., INSERT, UPDATE, DELETE): Commits the changes to the database.
    - Implements error handling to roll back transactions in case of failures.

    Args:
        query (str): The SQL query to be executed.

    Returns:
        dict:
            - For SELECT queries: A dictionary with the query, success message, and fetched results.
            - For non-SELECT queries: A dictionary with the query and success message.
            - For errors: A dictionary containing the error message.
    """
    try:
        d = DatabaseConnect.DatabaseConnection("root", "Alliswell#1906", "127.0.0.1", "3306", "mysql")
        query = query.replace('\\', '')
 
        result = d.execute_query(query=query, database_name="employees")
 
        if result.returns_rows:
            rows = result.fetchall()
            return {
                'query': query,
                'results': rows
            }
        else:
            return {
                'query': query,
                'message': 'Query executed successfully.'
            }
    except Exception as e:
        return {
            'query': query,
            'error': f"An error occurred: {str(e)}"
        }