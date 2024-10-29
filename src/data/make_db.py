import logging 
import pandas as pd
from pathlib import Path
from glob import glob 
import os
from tqdm import tqdm 
import sqlite3
import argparse
import pickle
import sys
import json
from parsing import parse_and_df, parse_ios_df
from database import SensingDB
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

def load_df():
    return


def build_file_index(path, already_processed):
    """
    Build index of files to be parsed an uploaded, excluding files already uploaded. 
    """
    if already_processed:
        processed_dbs = pickle.load(open(already_processed,'rb'))
    else:
        processed_dbs = set()
    iosSenses = set(glob((path)+'/*/*.db'))
    andSenses = set(glob((path)+'/*/*.dbr'))
    to_analyze = iosSenses.union(andSenses) - processed_dbs # exclude paths that have been uploaded already 
    return to_analyze

def setup_logging(log_file, log_level):
    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_file)

    # Set logging levels
    level = getattr(logging, log_level.upper())
    console_handler.setLevel(level)  # Logs desired level of information to the terminal
    file_handler.setLevel(logging.DEBUG)    # Logs everything to the file

    # Create formatters and add them to the handlers
    console_format = logging.Formatter('%(levelname)s - %(message)s')
    file_format = logging.Formatter('%(levelname)s - %(asctime)s - %(name)s - %(message)s')
    console_handler.setFormatter(console_format)
    file_handler.setFormatter(file_format)

    logger = logging.getLogger('make_db')
    logger.setLevel(logging.DEBUG)  # Set to the lowest level to capture all messages
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

def main():
    def dir_path(path):
    # utility to Check if the provided path is a valid directory
        if not os.path.isdir(path):
            raise argparse.ArgumentTypeError(f"The directory '{path}' does not exist.")
        return path

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        help="Directory containing files to be processed. Make sure you have read privileges to it. Defaults to current wd.",
        type=dir_path,  
        default=os.getcwd(),  # Default to the current working directory
        required=False   
    )
    parser.add_argument(
        "--pickle",
        help="Path to binarized set containing paths to db files already successfully uploaded.",
        required=False,
        default=None
    )    
    parser.add_argument(
        "--log-level",
        help="Set the logging level. Options are: DEBUG, INFO, WARNING, ERROR, CRITICAL.",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO"  
    )
    args = parser.parse_args()
    logger = setup_logging("make_db.log", args.log_level)
    paths = build_file_index(args.path, args.pickle)

    logger.info(f'Found {len(paths)} files to process')

    load_dotenv() 
    engine = create_engine(URL.create(
                                drivername = "mysql+mysqlconnector",
                                username=os.environ.get('DB_USER'),
                                password=os.environ.get('DB_PW'),  
                                host=os.environ.get('DB_HOST'),
                                database=os.environ.get('DB_NAME'),
                                ))

    #------------
    for dbloc in paths:
        conn = sqlite3.connect(dbloc)
        try:
            dft = pd.read_sql(sql="SELECT * FROM events", con=conn)
        except pd.errors.DatabaseError:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            if str(ex_value) == """Execution failed on sql 'SELECT * FROM events': no such table: events""":
                logger.info(f"At location {dbloc} Execution failed on sql 'SELECT * FROM events': no such table: events")
            else:
                logger.error(f"Can't read database file: {dbloc} --- error: {ex_value}")
            continue
    
        try:
            if dbloc[-3:] == 'dbr':
                dft = dft[['timestamp','data_type','data']]
                dft.rename(columns={
                                    'data_type' : 'event_id'
                                                    }, inplace = True )

                df_dict = parse_and_df(dft, dbloc)
                
            elif dbloc[-3:] == '.db':
                dft = dft[['event_time','event_id','event_data','device_id', 'user_id']]
                dft.rename(columns={
                                    'event_data' : 'data',
                                    'event_time' : 'timestamp'
                                                    }, inplace = True )
                df_dict = parse_ios_df(dft, dbloc)
        except:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            logger.error(f"Parsing failed, no data extracted at location: {dbloc} --- error: {ex_value}")

        ##return rows as list of tuples        records = df.to_records(index=False)   return list(records)
        #columns must be same 
        
        for df_name in df_dict.keys():
            try: 
                df_dict[df_name].to_sql(name = df_name,
                    con = engine,
                    if_exists = "append",
                    index = False
                    )
            except IntegrityError:
                logger.error(f"Data violates schema constraints, no data extracted for table {df_name} at location: {dbloc}")


    logger.info(f'Execution complete for path: {args.path}')


if __name__ == '__main__':
    print("""Execution started. """)
    main()