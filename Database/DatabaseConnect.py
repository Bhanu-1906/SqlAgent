import typing
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
 
class DatabaseConnection:
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
        dialect: str,
        database_name: typing.Union[str, typing.List[str]]
    ):
        if dialect not in self.SUPPORTED_DIALECTS:
            raise ValueError("Unsupported dialect. Supported dialects: {}".format(list(self.SUPPORTED_DIALECTS.keys())))
       
        self._username = username
        self._password = password
        self._hostname = hostname
        self._port = port
        self._dialect = dialect
       
        if isinstance(database_name, str):
            self._database_name = [database_name] if database_name else []
        elif isinstance(database_name, list):
            self._database_name = database_name
        else:
            raise ValueError("database_name must be a string or a list of strings.")
       
        self._database_url = "{0}+{1}://{2}:{3}@{4}:{5}".format(
            self._dialect,
            self.SUPPORTED_DIALECTS[self._dialect],
            self._username,
            self._password,
            self._hostname,
            self._port
        )
       
        self._engine: typing.Optional[Engine] = None
        self._session_factory = None
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
 
    def create_engine(self, database_name: str = "") -> Engine:
        full_url = "{0}/{1}".format(self._database_url, database_name) if database_name else self._database_url
        self._engine = create_engine(full_url, echo=False, pool_pre_ping=True)
        self._session_factory = sessionmaker(bind=self._engine)
        return self._engine
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
   
 
    def get_all_databases(self) -> typing.List[str]:
        try:
            with self.create_engine().connect() as connection:
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
            print(f"Error fetching databases: {e}")
            return []
   
    def get_database_metadata(self, database_name: str = "") -> typing.Optional[typing.List[dict]]:
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
            print(f"Error fetching metadata for {database_name}: {e}")
            return None
   
    def generate_schema_report(self, database_name: str, tables_metadata: typing.List[dict]) -> str:
        if not tables_metadata:
            return "No metadata found for database: {0}".format(database_name)
       
        report_lines = [
            "{0:<20} {1:<30} {2:<60}".format("Database Name", "Tables", "Columns"),
            "-" * 110
        ]
       
        for table in tables_metadata:
            table_name = table["name"]
            columns = ", ".join(table["columns"])
           
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
        if self._database_name:  # If specific database names are provided
            metadata_reports = []
            for db in self._database_name:
                tables_metadata = self.get_database_metadata(database_name=db)
                if tables_metadata:
                    metadata_reports.append(self.generate_schema_report(db, tables_metadata))
            return "\n\n".join(metadata_reports) if metadata_reports else "No metadata found for the specified databases."
       
        else:  # If no specific databases are provided, fetch all
            databases = self.get_all_databases()
            if not databases:
                return "No databases found or failed to fetch databases."
           
            metadata_reports = []
            for db in databases:
                tables_metadata = self.get_database_metadata(database_name=db)
                if tables_metadata:
                    metadata_reports.append(self.generate_schema_report(db, tables_metadata))
           
            return "\n\n".join(metadata_reports)
 
if __name__ == "__main__":
    try:
 
        db = DatabaseConnection(
            username='root',
            password='1234',
            hostname='127.0.0.1',
            port=3306,
            dialect='mysql',
            database_name=['world','employees']
        )
        print("\nDatabase Metadata:")
        print(db.get_all_databases_metadata())
   
    except Exception as e:
        print(f"An error occurred: {e}")
 
 
