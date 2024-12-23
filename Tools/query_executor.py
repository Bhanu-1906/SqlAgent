from langchain_core.tools import tool
from Database import DatabaseConnect
from config.settings import AppSettings
AppSettings = AppSettings()

@tool
def query_executor(query: str,database:str):
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
        d = DatabaseConnect.DatabaseConnection(AppSettings.DB_USER, AppSettings.DB_PASSWORD,AppSettings.DB_HOST,AppSettings.DB_PORT_NUMBER, AppSettings.DB_DIALECT,AppSettings.DB_NAME)
        query = query.replace('\\', '')
 
        result = d.execute_query(query=query, database_name=database)
 
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
    
if __name__ == '__main__':
    print(query_executor('SELECT COUNT(*) AS Number_of_Departments FROM departments;','employees'))