from config import config
from google.cloud.sql.connector import Connector
from pg8000 import Connection


class CloudSQLConnection:
    connector = Connector()
    connection: Connection = None

    def _connection_string(self) -> str:
        return config.PROJECT_ID + ":" + config.SQL_DATABASE_REGION + ":" + config.SQL_DATABASE_INSTANCE

    def connect(self) -> None:
        if self.connection != None:
            return

        self.connection: Connection = self.connector.connect(
            self._connection_string(),
            "pg8000",
            user=config.SQL_DATABASE_USERNAME,
            password=config.SQL_DATABASE_PASSWORD,
            db=config.SQL_DATABASE_NAME,
        )

# Create global instances
try:
    sql_connection = CloudSQLConnection()

    sql_connection.connect()
except Exception as e:
    print(f"Error initializing cloud services: {e}") 