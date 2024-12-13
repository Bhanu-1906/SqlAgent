def validate_and_refine_queries(queries: list, user_query: str):
    
    allowed_keywords = ["SELECT","INSERT", "UPDATE", "DELETE", "ALTER"]
    restricted_keywords = ["CREATE TABLE", "DROP TABLE", "DROP", "TRUNCATE"]
 
    for query in queries:
        if any(restricted_keyword in query.upper() for restricted_keyword in restricted_keywords):
            return {
                "status": f"The operation requested ('{user_query}') cannot be performed.",
                "action": "no"
            }
 
        if any(keyword in query.upper() for keyword in allowed_keywords):
            return {
                "status": f"The operation requested ('{user_query}') is allowed.",
                "action": "yes"
            }
 
    return {
        "status": f"The operation requested ('{user_query}') could not be validated.",
        "action": "no"
    }