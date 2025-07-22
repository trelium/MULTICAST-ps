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

## Get started

### Data ingestion

To use the data for downstream tasks, different data sources need to be copied into a SQL database.

#### Legacy CORA app

In the legacy version of the app used for data collection (CORA app), study data was uploaded and stored in three different formats. Follow these preliminary steps before importing the data to the SQL database for analysis:

- **EMA survey answers in Limesurvey SQL database**:\
  This data needs to be exported manually in `.csv` format from the Limesurvey web interface (see study protocol document for instructions) and copied to `data/raw/ema`.\
  Note that there are two survey results to export, corresponding to an old and a new version of the EMA questionnaires. Both survey answers should be exported and stored in the `/ema` folder.

- **MobileCoach data in MongoDB**:\
  This data needs to be copied from the MobileCoach server, where it can be exported from the MongoDB instance with the script:

  ```bash
  /opt/data_exports_mongo/check_and_extract.sh
  ```

- **Passive sensing data in SQLite files**:\
  Copied from the MobileCoach server, where it is stored as a collection of folders in:

  ```
  /opt/mobilecoach-server/sensing/uploads/phone
  ```

After completing these preliminary steps, your folder structure should look something like:

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

#### Pathmate CORA app

To import and save data from the Pathmate server, run the `download_pm_data` function in:

```
src/multicastps/data/pm_utils.py
```

Run the script with the `--path` argument pointing to the raw data folder:

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

#### Populate the SQL database

To populate the SQL database, run:

```bash
python src/multicastps/data/make_db.py --path /path/to/raw/data/folder --pickle /path/to/pkl/processed_dbs.pkl
```

The `--pickle` argument is optional and can be provided if a file with a list of already copied SQLite files is available.
