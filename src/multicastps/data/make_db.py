import pandas as pd
from pathlib import Path
from glob import glob 
import os
from tqdm import tqdm 
import sqlite3
import argparse
import pickle
import sys
from sqlalchemy.exc import IntegrityError, DataError
from multicastps.data.parsing import parse_and_df, parse_ios_df, parse_part_vars
from multicastps.data.database import MulticastDB

from dotenv import load_dotenv
from multicastps.data.queries_mc import TABLES
import sys
from multicastps.utils.logging_setup import setup_logging

#------------UTILS
def build_file_index(path, already_processed):
    #Build index of passive sensing files to be parsed an uploaded, excluding files already uploaded. 
    iosSenses = set(glob(os.path.join(path, "phone",'*','*.db')))
    andSenses = set(glob(os.path.join(path, "phone",'*','*.dbr')))
    to_analyze = iosSenses.union(andSenses) - already_processed # exclude paths that have been uploaded already 
    
    #retrieve csvs for ema 
    csv_files_ema = glob((os.path.join(path, "ema", '*.csv')))
    if len(csv_files_ema) != 2:
        raise ValueError(f"Expected 2 CSV files in the folder, but found {len(csv_files_ema)}.")
    
    #retrieve csvs for mc
    csv_files_mc = glob((os.path.join(path, "mongo_export", '*.csv')))
    if len(csv_files_mc) != 6:
        raise ValueError(f"Expected 6 CSV files in the folder, but found {len(csv_files_mc)}.")
    
    return to_analyze, csv_files_ema, csv_files_mc

def dir_path(path):
    # Utility to check if the provided path is a valid directory and contains required subdirectories
    required_subdirs = ['ema', 'mongo_export', 'phone']

    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError(f"The directory '{path}' does not exist.")

    # Check for required subdirectories
    missing_subdirs = [subdir for subdir in required_subdirs if not os.path.isdir(os.path.join(path, subdir))]
    if missing_subdirs:
        raise argparse.ArgumentTypeError(
            f"The directory '{path}' is missing the following required subdirectories: {', '.join(missing_subdirs)}"
        )

    return path

#------------ARGUMENTS
parser = argparse.ArgumentParser()
parser.add_argument(
    "--path",
    help="Directory containing sub-directories ema, mongo_export and phone. Make sure you have read privileges to it. Defaults to current wd.",
    type=dir_path,  
    default=os.path.join(os.getcwd(),'data','raw'),  # Default to the data folder in current working directory
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
    help="Set the logging level for shell. Options are: DEBUG, INFO, WARNING, ERROR, CRITICAL.",
    type=str,
    choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    default="INFO"  
)
parser.add_argument(
    "--streams",
    help="The sensor streams to import. If only a subset of streams are selected, this will not reflect in the logs",
    type=str,
    choices=list(TABLES.keys()),
    default=list(TABLES.keys()),
    nargs='+'
)
parser.add_argument(
    "--sources",
    help="Which sources to pull data from",
    type=str,
    default='ALL',
    choices=["ALL", "PM", "PR"]
)

args = parser.parse_args()

load_dotenv() 
mod_name = str(__file__).split('/')[-1][:-3]
logger = setup_logging(mod_name, args.log_level)
if args.pickle:
    processed_dbs = pickle.load(open(args.pickle,'rb'))
else:
    processed_dbs = set()

logger.info('Execution started')
paths, paths_ema, paths_mc = build_file_index(args.path, processed_dbs)
not_copied = dict()

db = MulticastDB()

#------------EMA DATA UPLOAD 
file1, file2 = paths_ema
df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)
# Merge the two DataFrames, keeping common and non-common columns
merged_df = pd.concat([df1, df2], ignore_index=True)

db.insert_pd(merged_df,'EMA')
logger.info('EMA uploaded')

#------------MOBILECOACH DATA UPLOAD
for file in paths_mc:
    if os.path.splitext(os.path.basename(file))[0] == "ParticipantVariableWithValue":
        with pd.read_csv(file, chunksize=1000,) as reader:
            for chunk in reader:
                df_dict = parse_part_vars(chunk)
                for stream_name in df_dict.keys():
                    db.insert_pd(df_dict[stream_name], stream_name)
    df = pd.read_csv(file)
    db.insert_pd(df, os.path.splitext(os.path.basename(file))[0])
logger.info('Mobilecoach data uploaded')
    
#------------PASSIVE SENSING UPLOAD
logger.info(f'Found {len(paths)} phone sensing files to process ({len(processed_dbs)} already processed)')

for dbloc in tqdm(paths):
    failed_skipped_streams = set()
    conn = sqlite3.connect(dbloc)
    try:
        dft = pd.read_sql(sql="SELECT * FROM events", con=conn)
    except pd.errors.DatabaseError:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        if str(ex_value) == """Execution failed on sql 'SELECT * FROM events': no such table: events""":
            logger.info(f"At location {dbloc} Execution failed on sql 'SELECT * FROM events': no such table: events")
        else:
            logger.error(f"Can't read database file: {dbloc} --- error: {ex_value}")
            not_copied[dbloc] = 'all' #the whole file contents could not be copied
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
        not_copied[dbloc] = 'all' #the whole file contents could not be copied
        continue
    
    #Upload to database
    for stream_name in df_dict.keys():
        try: 
            if stream_name in args.streams :
                db.insert_pd(df_dict[stream_name], stream_name)
            else:
                failed_skipped_streams.add(stream_name)
        except IntegrityError:
            logger.error(f"Data violates schema constraints, no data extracted for table {stream_name} at location: {dbloc}")
            failed_skipped_streams.add(stream_name)
        except:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            logger.error(f"Execution failed: {dbloc} \n {ex_value}")
            failed_skipped_streams.add(stream_name)
    
    #save outcomes of processing operations 
    if len(failed_skipped_streams)!= 0:
        not_copied[dbloc] = failed_skipped_streams
    processed_dbs.add(dbloc)

logger.info(f'Execution complete for path: {args.path}')
# Save to a pickle file
with open(os.path.join(os.environ.get('LOG_DIR'), 'processed_dbs.pkl'), "wb") as file:
    pickle.dump(processed_dbs, file)
