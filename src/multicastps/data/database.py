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
        self.metadata.reflect() #self.metadata.tables , info on table/column structure, must be refreshed with reflect()

    def get_table_names(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
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
        
    def table_to_csv(self,table):
        #https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql_query.html
        #df = pd.read_sql_query(query, engine)
        pass

    def get_sensing_timespan(self, table = None, exact = True):
        """Returns the start and end date of sensing for each participant in the database, across all tables present 
        If exact is set to true, return all daily time windows for which there is passive sensing data.
        Returns list of tuples 
        """
        all_tables = set(TABLES.keys())
        all_tables.discard('DEVICE_INFO')
        qry = str()
        if table: 
            if exact == False:#get first and last timestamp for which data is available, disregarding gaps in between
                qry = f"""SELECT USER_ID, min(TIMESTAMP) min, max(TIMESTAMP) max
                        FROM `{table}` GROUP BY USER_ID """
            else: #get days in between which sensing was continuous
                qry = f"""WITH t AS (
                            WITH tt AS (
                                SELECT USER_ID, DATE(TIMESTAMP) AS dd
                                FROM {table}
                                GROUP BY USER_ID, DATE(TIMESTAMP)
                                ORDER BY 1, 2
                            )
                            SELECT USER_ID, dd AS d, ROW_NUMBER() OVER (ORDER BY USER_ID, dd) AS i,
                                DATE_SUB(dd, INTERVAL ROW_NUMBER() OVER (ORDER BY USER_ID, dd) DAY) AS ii
                            FROM tt
                            GROUP BY USER_ID, dd
                        )
                        SELECT USER_ID, MIN(d), MAX(d)
                        FROM t
                        GROUP BY USER_ID, ii;
                        """
        else: #get first and last timestamp for which data is available, across all tables  
            for i in all_tables:
                if i == 'STEPS_IOS' or i == 'STEPS':
                   qry += f"""SELECT USER_ID, min(START_TIME) min , max(START_TIME) max
                            FROM `{i}` GROUP BY USER_ID UNION """ 
                else:
                    qry += f"""SELECT USER_ID, min(TIMESTAMP) min , max(TIMESTAMP) max
                            FROM `{i}` GROUP BY USER_ID UNION """
            qry = qry[:-6]
            qry = """WITH t AS ( """ + qry + """ ) SELECT USER_ID, min, max from t GROUP BY USER_ID"""
        
        return self.query(qry)
    
    def get_ema_timespan(self, exact = True):
        qry = str()
        table = 'EMA'
        if exact == False:#get first and last timestamp for which data is available, disregarding gaps in between
            qry = f"""SELECT participantCode, min(submitdate) min, max(submitdate) max
                    FROM `{table}` GROUP BY participantCode """
        else: #get days in between which EMA was continuous
            qry = f"""WITH t AS (
                        WITH tt AS (
                            SELECT participantCode, DATE(submitdate) AS dd
                            FROM {table}
                            GROUP BY participantCode, DATE(submitdate)
                            ORDER BY 1, 2
                        )
                        SELECT participantCode, dd AS d, ROW_NUMBER() OVER (ORDER BY participantCode, dd) AS i,
                            DATE_SUB(dd, INTERVAL ROW_NUMBER() OVER (ORDER BY participantCode, dd) DAY) AS ii
                        FROM tt
                        GROUP BY participantCode, dd
                    )
                    SELECT participantCode, MIN(d), MAX(d)
                    FROM t
                    GROUP BY participantCode, ii;
                    """
        return self.query(qry)
    
    def update_overview(self):
        # make table with part_code, part_code_harmonized, user_id, is_participant
        with self.engine.connect() as connection:
            qry = """CREATE TABLE IF NOT EXISTS PartOverview (
                        part_code VARCHAR(15),
                        part_code_harm VARCHAR(10),
                        user_id VARCHAR(40),
                        is_participant BOOLEAN
                    );"""
            res = connection.execute(text(qry))

            qry = """INSERT INTO PartOverview (part_code, part_code_harm, user_id, is_participant)
            SELECT 
                nickname AS part_code,
                REGEXP_SUBSTR(nickname, '^(MC_|BMC_)[0-9]{4}') AS part_code_harm,
                _id AS user_id,
                nickname REGEXP '^(MC_|BMC_)[0-9]{4}.*' AS is_participant
            FROM Participant;
            """
            res = connection.execute(text(qry))
         

    def make_participant_report(self): #you care here only about the study participants         part_code_harmonized,  sensing_windows, ema_window, missed_emas, missed_emas_pct, days_no_ps, days_no_ps_pct, hr_no_ps, hr_no_ps_pct, missing_gps_pings   #between 8 and 18 
        pass    
