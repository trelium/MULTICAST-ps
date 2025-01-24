import os
import logging
import sys
from dotenv import load_dotenv
from multicastps.data.queries_mc import TABLES
from multicastps.utils.logging_setup import setup_logging
from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy.engine import URL
from sqlalchemy.exc import IntegrityError, DataError

load_dotenv()
if __name__ == '__main__':
    modulename = str(__file__).split('/')[-1][:-3]
    logger = setup_logging(modulename, 'INFO')
else: 
    logger = logging.getLogger(__name__)


class MulticastDB:
    def __init__(self) -> None:
        try: 
            self.engine = create_engine(URL.create(
                            drivername = "mysql+mysqlconnector",
                            username=os.environ.get('DB_USER'),
                            password=os.environ.get('DB_PW'),  
                            host=os.environ.get('DB_HOST'),
                            database=os.environ.get('DB_NAME'),
                            ))
            logger.info(f"Connection to DB established")
        except:
            logger.exception(f"Connection to DB failed")
    
        def table_exists(engine, table_name):
            inspector = inspect(engine)
            return table_name in inspector.get_table_names()

        with self.engine.connect() as connection:
            for table_name, query in TABLES.items():
                if not table_exists(self.engine, table_name):
                    try:
                        connection.execute(text(query))
                    except Exception as e:
                        logger.exception(f"Error creating table {table_name}: {e}")

        self.metadata = MetaData(bind = self.engine)
        self.metadata.reflect()

    def query(self, query):
        """
        Execute a SQL query 
        """
        with self.engine.connect() as connection:
            try:
                res = connection.execute(text(query))
                return res.fetchall()
            except:
                logger.exception(f"Error executing query")

    def insert_pd(self,df,table):
        """
        Upload to database a pandas dataframe to specified table, appending to existing data
        """
        df.to_sql(name = table,
                con = self.engine,
                if_exists = "append",
                index = False
                )

    def drop_tables(self, table_name=None):
        """
        Drop all tables from the database if 'all' is passed, or drop a specific table.
        :param table_name: Name of the table to drop, or 'all' to drop all tables.
        """
        
        if table_name == 'all':
            self.metadata.drop_all()
            logger.info("All tables dropped successfully.")
        elif table_name:
            if table_name in self.metadata.tables:
                Table(table_name, self.metadata).drop(self.engine)
            else:
                raise ValueError(f"Table '{table_name}' does not exist in the database.")
        else:
            raise ValueError("No table name provided. Specify 'all' to drop all tables or provide a table name.")
