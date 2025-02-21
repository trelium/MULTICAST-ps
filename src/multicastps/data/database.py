import os
import logging
import pandas as pd
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
        
    def table_to_csv(self,table,**kwargs):
        query = f"""SELECT * FROM {table};""" 
        return pd.read_sql_query(query, self.engine,**kwargs)

    def get_sensing_timespan(self, table = None, exact = True):
        """Returns the start and end date of sensing for each participant in the database, across all tables present 
        If exact is set to true, return all daily time windows for which there is passive sensing data.
        Returns list of tuples 
        """
        all_tables = set(TABLES.keys())
        all_tables.discard('DEVICE_INFO')
        qry = str()
        if table == 'STEPS_IOS' or table == 'STEPS':
            timecol = 'START_TIME'
        else:
            timecol = 'TIMESTAMP'
        if table: 
            if exact == False:#get first and last timestamp for which data is available, disregarding gaps in between
                qry = f"""SELECT USER_ID, min({timecol}) min, max({timecol}) max
                        FROM `{table}` GROUP BY USER_ID """
            else: #get days in between which sensing was continuous
                qry = f"""WITH t AS (
                            WITH tt AS (
                                SELECT USER_ID, DATE({timecol}) AS dd
                                FROM {table}
                                GROUP BY USER_ID, DATE({timecol})
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
        self.drop_tables('PartOverview')
        # make table with part_code, part_code_harmonized, user_id, is_participant
        # note that some device_ids that are present in the ps tables from a time before study start
        # might not be present here as the database was cleaned before study start 
        with self.engine.connect() as connection:
            qry = """CREATE TABLE IF NOT EXISTS PartOverview (
                        part_code VARCHAR(15),
                        part_code_harm VARCHAR(10),
                        user_id VARCHAR(40),
                        is_participant BOOLEAN
                    );"""
            res = connection.execute(text(qry))
            #note: there are duplicate values in Participant
            qry = """INSERT INTO PartOverview (part_code, part_code_harm, user_id, is_participant) 
                    SELECT 
                        nickname AS part_code,
                        NULLIF(REGEXP_SUBSTR(nickname, '^(MC_|BMC_)[0-9]{4}'), '') AS part_code_harm,
                        _id AS user_id,
                        nickname REGEXP '^(MC_|BMC_)[0-9]{4}.*' AS is_participant
                    FROM Participant;
                    """
            res = connection.execute(text(qry))

            qry = """UPDATE PartOverview
                    SET user_id = REPLACE(REPLACE(user_id, 'ObjectId(', ''), ')', '');
                    """
            res = connection.execute(text(qry))
        self.metadata.reflect()
         

    def make_participant_report(self): #only study participants         part_code_harmonized,  ps_window, ema_window, all_ps_windows, all_ema_windows, missed_emas, missed_emas_pct, days_no_ps, days_no_ps_pct, hr_no_ps, hr_no_ps_pct, missing_gps_pings   #between 8 and 18 
        self.update_overview() #update participant codes mappings
        ow = self.table_to_csv('PartOverview')
        ow = ow.drop_duplicates() #for safety 
        ps_win = pd.DataFrame(self.get_sensing_timespan(exact=False))
        ps_win.columns = ['USER_ID', 'oldest_ps_rec', 'latest_ps_rec']
        ps_win = ps_win.merge(ow[['user_id','part_code_harm']], left_on='USER_ID', right_on='user_id', how='left')
        ema_win = pd.DataFrame(self.get_ema_timespan(exact=False))
        ema_win.columns = ['participantCode', 'oldest_ema_rec', 'latest_ema_rec']
        ema_win = ema_win.merge(ow[['part_code','part_code_harm']], left_on='participantCode', right_on='part_code', how='left')
        
        #get all sensing windows 
        tot_psws = pd.DataFrame()
        all_tables = set(TABLES.keys())
        all_tables.discard('DEVICE_INFO')
        for table in all_tables:
            psws = pd.DataFrame(self.get_sensing_timespan(table = table, exact=True))
            psws['table'] = table
            tot_psws = pd.concat([psws, tot_psws])
        #change from user_id to participant code harmonized
        tot_psws = tot_psws.merge(ow, left_on='USER_ID', right_on='user_id', how='left')
        #add EMA windows 
        emaws = pd.DataFrame(self.get_ema_timespan(exact=True))
        emaws['table'] = 'EMA'
        emaws = emaws.merge(ow, left_on='participantCode', right_on='part_code', how='left') 
        tot_psws = pd.concat([emaws, tot_psws],join = 'inner')
        # Group tot_psws by harmonized part code and create a dictionary with min and max values per participant
        ps_ema_ws = tot_psws.groupby('part_code_harm').apply(lambda g: {
            table: {
                "min": g.loc[g["table"] == table, "MIN(d)"].tolist(),
                "max": g.loc[g["table"] == table, "MAX(d)"].tolist()
            } for table in g["table"].unique()
        }).reset_index(name='all_windows') 

        #missed_emas participantCode submitdate there will be one record per sent survey 
        emas = self.table_to_csv('EMA')
        emas['submitdate'] = pd.to_datetime(emas['submitdate'])
        emas = emas.merge(ow, left_on='participantCode', right_on='part_code', how='left')
        emas_count = emas.groupby('part_code_harm')['submitdate'].count().reset_index()
        emas_count.columns = ['part_code_harm', 'ema_count']        # Rename column for clarity
        emas_count['ema_missed'] = 70 - emas_count['ema_count'] #70 total prompts
        emas_count['ema_missed_pct'] = emas_count['ema_missed'] * 100 /70
        emas_count['ema_missed_pct'] = emas_count['ema_missed_pct'].round(2)
        #ps_missing_days
        emas_st = self.table_to_csv('StartTimes') #USER_ID    ema_start
        emas_st = emas_st.merge(ow,left_on='USER_ID', right_on='user_id', how='left')
        #hr_no_ps
        ret = emas_count.merge(ps_ema_ws,left_on='part_code_harm', right_on='part_code_harm', how='left')
        ret = ret.merge(ema_win,left_on='part_code_harm', right_on='part_code_harm', how='left')
        ret = ret.merge(ps_win,left_on='part_code_harm', right_on='part_code_harm', how='left')
        ret = ret.merge(emas_st,left_on='part_code_harm', right_on='part_code_harm', how='left')

        #cleanup
        ret.columns = ret.columns.str.lower()  # Convert all column names to lowercase
        columns_to_drop = [col.lower() for col in ['participantCode', 'part_code_x', 'USER_ID_x', 
                                                'user_id_x', 'USER_ID_y', 'part_code_y', 
                                                'user_id_y', 'is_participant']]
        ret.drop(columns=[col for col in columns_to_drop if col in ret.columns], inplace=True)
        return ret
