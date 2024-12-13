import typing
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

class DatabaseConnection:
    """
    A comprehensive database connection management class supporting multiple database dialects.
    
    Supports connection and metadata retrieval for MySQL, PostgreSQL, and Oracle databases.
    """
    
    # Use a class-level constant for supported dialects
    SUPPORTED_DIALECTS = {
        "mysql": "pymysql",
        "postgresql": "psycopg2",
        "oracle": "cx_oracle"
    }
    
    def __init__(
        self, 
        username: str, 
        password: str, 
        hostname: str, 
        port: int, 
        dialect: str
    ):
        """
        Initialize database connection parameters.
        
        Args:
            username (str): Database user name
            password (str): Database user password
            hostname (str): Database server hostname
            port (int): Database server port
            dialect (str): Database dialect (mysql, postgresql, oracle)
        """
        # Validate dialect
        if dialect not in self.SUPPORTED_DIALECTS:
            raise ValueError("Unsupported dialect. Supported dialects: {}".format(list(self.SUPPORTED_DIALECTS.keys())))
        
        self._username = username
        self._password = password
        self._hostname = hostname
        self._port = port
        self._dialect = dialect
        
        # Construct database URL using string formatting
        self._database_url = "{0}+{1}://{2}:{3}@{4}:{5}".format(
            self._dialect, 
            self.SUPPORTED_DIALECTS[self._dialect],
            self._username, 
            self._password, 
            self._hostname, 
            self._port
        )
        
        # Initialize engine and session factory
        self._engine: typing.Optional[Engine] = None
        self._session_factory = None
    
    def create_engine(self, database_name: str = "") -> Engine:
        """
        Create and return a SQLAlchemy engine.
        
        Args:
            database_name (str, optional): Name of the database to connect to
        
        Returns:
            Engine: SQLAlchemy database engine
        """
        # Construct full URL with database name
        full_url = "{0}/{1}".format(self._database_url, database_name) if database_name else self._database_url
        
        self._engine = create_engine(full_url, echo=False,pool_pre_ping=True)
        self._session_factory = sessionmaker(bind=self._engine)
        return self._engine
    
    def get_database_url(self, mask_password: bool = True) -> str:
        """
        Generate database connection URL.
        
        Args:
            mask_password (bool, optional): Whether to mask the password. Defaults to True.
        
        Returns:
            str: Database connection URL
        """
        # Use string formatting instead of f-strings
        password = "****" if mask_password else self._password
        return "{0}+{1}://{2}:{3}@{4}:{5}".format(
            self._dialect, 
            self.SUPPORTED_DIALECTS[self._dialect],
            self._username, 
            password, 
            self._hostname, 
            self._port
        )
    
    def get_all_databases(self) -> typing.List[str]:
        """
        Retrieve all databases from the server.
        
        Returns:
            List[str]: List of database names
        """
        try:
            with self.create_engine().connect() as connection:
                # Use a dictionary for dialect-specific queries to improve maintainability
                dialect_queries = {
                    "mysql": "SHOW DATABASES;",
                    "postgresql": "SELECT datname FROM pg_database WHERE datistemplate = false;",
                    "oracle": "SELECT name FROM v$database;"
                }
                
                query = dialect_queries.get(self._dialect)
                if not query:
                    raise ValueError("Unsupported dialect.")
                
                result = connection.execute(text(query))
                return [row[0] for row in result.fetchall()]
        
        except Exception as e:
            print(f"Database")
            
    
    def execute_query(
        self, 
        query: str, 
        database_name: str, 
        params: typing.Optional[dict] = None
    ) -> typing.Any:
        """
        Execute a SQL query on a specific database.
        
        Args:
            query (str): SQL query to execute
            database_name (str): Target database name
            params (dict, optional): Query parameters
        
        Returns:
            Query execution result
        """
        try:
            with self.create_engine(database_name).begin() as conn:
                return conn.execute(text(query), params or {})
        except Exception as e:
            print(f"Failed to execute query: {e}")
            return None
    
    def get_database_metadata(self, database_name: str = "") -> typing.Optional[typing.List[dict]]:
        """
        Retrieve metadata for tables in a database.
        
        Args:
            database_name (str, optional): Target database name
        
        Returns:
            Optional[List[dict]]: List of table metadata or None
        """
        try:
            metadata = MetaData()
            engine = self.create_engine(database_name)
            metadata.reflect(bind=engine)
            
            tables_metadata = []
            for table_name, table in metadata.tables.items():
                table_metadata = {
                    "name": table_name,
                    "columns": [column.name for column in table.columns],
                    "foreign_keys": []
                }
                
                # Collect foreign key relationships
                for column in table.columns:
                    for fk in column.foreign_keys:
                        table_metadata["foreign_keys"].append({
                            "column": column.name,
                            "references": {
                                "table": fk.column.table.name,
                                "column": fk.column.name
                            }
                        })
                
                tables_metadata.append(table_metadata)
            
            return tables_metadata
        
        except Exception as e:
            print(f"Failed to retrieve metadata: {e}")
            return None
    
    def generate_schema_report(
        self, 
        database_name: str, 
        tables_metadata: typing.List[dict]
    ) -> str:
        """
        Generate a formatted schema report.
        
        Args:
            database_name (str): Name of the database
            tables_metadata (List[dict]): Metadata of tables
        
        Returns:
            str: Formatted schema report
        """
        if not tables_metadata:
            return "No metadata found for database: {0}".format(database_name)
        
        report_lines = [
            "{0:<20} {1:<30} {2:<60}".format("Database Name", "Tables", "Columns"),
            "-" * 110
        ]
        
        for table in tables_metadata:
            table_name = table["name"]
            columns = ", ".join(table["columns"])
            
            # Generate relationships string
            relationships = ""
            if table.get("foreign_keys"):
                relationships = (
                    "\n" + " " * 51 + "Relationships: " +
                    ", ".join(
                        "{0} -> {1}.{2}".format(
                            rel['column'], 
                            rel['references']['table'], 
                            rel['references']['column']
                        )
                        for rel in table["foreign_keys"]
                    )
                )
            
            report_lines.append(
                "{0:<20} {1:<30} {2:<60}{3}".format(
                    database_name, table_name, columns, relationships
                )
            )
        
        return "\n".join(report_lines)
    
    def get_all_databases_metadata(self) -> str:
        """
        Fetch and format metadata for all databases.
        
        Returns:
            str: Formatted metadata report for all databases
        """
        databases = self.get_all_databases()
        if not databases:
            return "No databases found or failed to fetch databases."
        
        metadata_reports = []
        for db in databases:
            tables_metadata = self.get_database_metadata(database_name=db)
            if tables_metadata:
                metadata_reports.append(self.generate_schema_report(db, tables_metadata))
            else:
                metadata_reports.append("Failed to retrieve metadata for database: {0}".format(db))
        
        return "\n\n".join(metadata_reports)

if __name__ == "__main__":
    try:
        db = DatabaseConnection(
            username='avnadmin', 
            password='AVNS_hcHLxiEmLVsprkffH_5', 
            hostname='postg-1-harikrishna-7c5f.i.aivencloud.com', 
            port='21911', 
            dialect='postgresql'
        )
        
        print("\nDatabase Metadata:")
        print(db.get_all_databases_metadata())
    
    except Exception as e:
        print(f"An error occurred: {e}")

