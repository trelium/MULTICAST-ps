# MULTICAST study data management utilities
This repository contains utilities to:
- Clean and copy the collected smartphone data form the MULTICAST study to a SQL database 
- Read/write programmatically the data in the SQL database containng study data 
- Obtain reports on data coverage based on the data present in the SQL database 

## Installation

After cloning this repo, create a `.env` file in the root folder of the project with the following values to ingest and access the data:

```env
DB_USER=""        # username of SQL database where the data is stored
DB_PW=""          # password of SQL database where the data is stored
DB_HOST=""        # IP address of SQL database where the data is stored
DB_PORT=""        # port of SQL database where the data is stored
DB_NAME=""        # name of SQL database where the data is stored
LOG_DIR=""        # directory where to store logs of executions
PM_PW=""          # password to access pathmate API
PM_USER=""        # username to access pathmate API
````

Install the environment using the following command:

```bash
conda env create -f environment.yml
```

Activate the environment `ps-multi`:

```bash
conda activate ps-multi
```

Create the following folder structure in the project root folder:

```
data
└── raw
    ├── ema
    ├── mongo_export
    ├── phone
    └── pm
```

## Usage

### Copying data to the SQL database

The code provides functionality to clean and copy data collected by the CORA app and stored in the CORA servers to a SQL database where it is consolidated.

#### Copying data collected by the legacy CORA app

In the legacy version of the app used for data collection (CORA app), study data was uploaded and stored in three different formats. Follow these preliminary steps before importing the data to the SQL database for analysis:

1. **EMA survey answers in Limesurvey SQL database**:\
  This data needs to be exported manually in `.csv` format from the Limesurvey web interface (see study protocol document for instructions) and copied to `data/raw/ema`.\
  Note that there are two survey results to export, corresponding to an old and a new version of the EMA questionnaires. Both survey answers should be exported and stored in the `/ema` folder.

2. **MobileCoach data in MongoDB**:\
  This data needs to be copied from the CORA server (MobileCoach) where it is stored in MongoDB. Log into the CORA server and run:

  ```bash
  /opt/data_exports_mongo/check_and_extract.sh
  ```
  This will create a series of .csv files with the data in the current directory.

3. **Passive sensing data in SQLite files**:\
  Copy data from the CORA (MobileCoach) server, where it is stored as a collection of folders in:

  ```
  /opt/mobilecoach-server/sensing/uploads/phone
  ```

Copy the files obtained above in the previously made folder so that your folder structure looks like:

```
└── data
    └── raw
        ├── ema
        │   ├── multicast_ema_new_15012025.csv
        │   └── multicast_ema_old_21082024.csv
        ├── mongo_export
        │   ├── DialogMessage.csv
        │   ├── DialogOption.csv
        │   ├── Intervention.csv
        │   ├── InterventionVariableWithValue.csv
        │   ├── Participant.csv
        │   └── ParticipantVariableWithValue.csv
        ├── phone
        │   ├── 6445fc379026c6000edaf4d9
        │   │   ├── 1682369311586_6445fc379026c6000edaf4d9.dbr
        │   │   ├── 1682419344044_6445fc379026c6000edaf4d9.dbr
        │   │   └── ...
        │   ├── 644604239026c6000edaf51e
        │   │   ├── 644604239026c6000edaf51e_1682310214_mca.db
        │   │   ├── 644604239026c6000edaf51e_1682312374_mca.db
        │   │   └── ...
        │   └── ...
        └── pm
            ├── basic-participant-information-table.csv
            ├── dashboard-messages-list.csv
            ├── dialog-messages-list.csv
            ├── json-objects-table.csv
            ├── json-sensor-data-list.csv
            ├── variables-history-list.csv
            └── variables-table.csv
```

**Copy the data to the SQL database**:

To copy data in the SQL database after following the steps above, run:

```bash
python src/multicastps/data/make_db.py --path /path/to/raw/data/folder --pickle /path/to/pkl/processed_dbs.pkl
```

The `--pickle` argument is optional and can be provided if a file with a list of already copied SQLite files is available.


#### Copying data collected by the Pathmate CORA app

1. **Downloading phone data from the Pathmate CORA server**:\
Run this script with the `--path` argument pointing to the raw data folder where you want to store the data:

```bash
python src/multicastps/data/pm_utils.py download_pm_data --path data/raw/pm
```

After running the script, the contents of the folder `pm` will look like:

```
└── data
    └── raw
        └── pm
            ├── basic-participant-information-table.csv
            ├── dashboard-messages-list.csv
            ├── dialog-messages-list.csv
            ├── json-objects-table.csv
            ├── json-sensor-data-list.csv
            ├── variables-history-list.csv
            └── variables-table.csv
```


### Read/write data on the SQL database

The module `MULTICAST-ps/src/multicastps/data/database.py` contains a class to interact with the database following the object-relational mapping paradigm. Particulary, the `insert_pd()` method can be used to insert a pandas dataframe to a specified table, and `drop_tables()` can be used to delete tables in the database.

### Obtaining reports on data coverage 

A participant report can be obtained based on the data present in the database. It can be obtained via the `make_participant_report()` method in `database.py`. This report consists of a table containg the following information: 
| Column Name         | Description                                                                                                         |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| **part_code_harm**  | Study code associated to a participant. "harm" stands for harmonized: multiple codes e.g. MC_1001_a, MC_1001_b are transformed into a univocal one that identifies a participant (MC_1001)       |
| **ema_count**       | Total number of EMAs records observed.                |
| **ema_missed**      | Number of EMAs that were expected but not completed/submitted.                                                  |
| **ema_missed_pct**  | Percentage of missed EMAs relative to the total expected EMAs.                                                               |
| **all_windows**     | The set of daily time windows during which there is at least one EMA / passive sensing recording, divided per sensor stream.              |
| **oldest_ema_rec**  | Timestamp of the earliest EMA record available for a participant.                                                      |
| **latest_ema_rec**  | Timestamp of the most recent EMA record available for a participant.                                                                   |
| **oldest_ps_rec**   | Timestamp of the earliest passive sensing record across all sensor streams.  |
| **latest_ps_rec**   | Timestamp of the most recent passive sensing record across all sensor streams.                                                                        |
| **dates_no_ps**     | List of dates on which no passive sensing data was recorded, considering the period start_date - en_date and selecting together location, activity, wifi, bluetooth and screen activation recordings.                                                                      |
| **days_no_ps**      | Count of days without any passive sensing  data, considering the period start_date - en_date and selecting together location, activity, wifi, bluetooth and screen activation recordings.                                                                                      |
| **dates_no_ps_idx** | Day index numbers counted from start_date representing the dates without passive sensing data.                                             |
| **tot_days_ps**     | Total number of days with available passive sensing data, considering also days outside study participation.                                                                               |
| **start_date**      | Beginning date of study participation, corresponding to the second oldest EMA recording. date.                                                         |
| **end_date**        | Ending date of study participation, corresponding to start_date + 28 days.                                                           |
