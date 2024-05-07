TABLES = {'Accelerometer': """CREATE TABLE `PHONE_ACCELEROMETER` (
                                    `TIMESTAMP` char(13) NOT NULL, 
                                    `DEVICE_ID` varchar(40) NOT NULL,
                                    `DOUBLE_VALUES_0` double,
                                    `DOUBLE_VALUES_1` double,
                                    `DOUBLE_VALUES_2` double
                                    ) ENGINE=InnoDB;""",
        'Bluetooth' : """CREATE TABLE `PHONE_BLUETOOTH` (
                                    `TIMESTAMP` char(13) NOT NULL,
                                    `DEVICE_ID` varchar(40) NOT NULL,
                                    `BT_ADDRESS` varchar(150) ,
                                    `BT_NAME` varchar(150) ,
                                    `BT_RSSI` mediumint
                                    ) ENGINE=InnoDB;""",                                    
        'InstalledApps' : """CREATE TABLE `PHONE_APPLICATIONS` (
                                    `TIMESTAMP` char(13) NOT NULL,
                                    `DEVICE_ID` varchar(40) NOT NULL,
                                    `PACKAGE_NAME` varchar(250),
                                    `APPLICATION_NAME` varchar(150),
                                    `IS_SYSTEM_APP` boolean ,
                                    `VERSION_NAME` varchar(100) ,
                                    `FIRST_INSTALL_TIME` char(13) ,
                                    `LAST_UPDATE_TIME` char(13) ,
                                    `IS_INACTIVE` boolean ,
                                    `IS_RUNNING` boolean ,
                                    `IS_PERSISTENT` boolean 
                                    ) ENGINE=InnoDB;""",
        'AppUsageStats' : """CREATE TABLE `APP_USAGE_STATS` (
                                    `TIMESTAMP` char(13) NOT NULL,
                                    `DEVICE_ID` varchar(40) NOT NULL,
                                    `STATS` json
                                    ) ENGINE=InnoDB;""",  
        'Battery' : """CREATE TABLE `PHONE_BATTERY` (
                            `TIMESTAMP` char(13) NOT NULL,
                            `DEVICE_ID` varchar(40) NOT NULL,
                            `BATTERY_STATUS` varchar(150), 
                            `BATTERY_LEVEL` tinyint,
                            `BATTERY_SCALE` tinyint
                            ) ENGINE=InnoDB;""",  
        'PhoneRadio' : """CREATE TABLE `PHONE_RADIO` (
                            `TIMESTAMP` char(13) NOT NULL,
                            `DEVICE_ID` varchar(40) NOT NULL,
                            `RADIO_RESULTS` JSON 
                            ) ENGINE=InnoDB;""",  
        'WiFi' : """CREATE TABLE `PHONE_WIFI_VISIBLE` (
                            `TIMESTAMP` char(200) NOT NULL,
                            `DEVICE_ID` varchar(40) NOT NULL,
                            `SSID` varchar(250) ,
                            `BSSID` varchar(250) ,
                            `SECURITY` varchar(200) ,
                            `FREQUENCY` mediumint ,
                            `RSSI` mediumint 
                            ) ENGINE=InnoDB;""",            
        'Calendar' : """CREATE TABLE `PHONE_CALENDAR` (
                            `TIMESTAMP` char(13) NOT NULL,
                            `DEVICE_ID` varchar(40) NOT NULL,
                            `EVENTS` JSON
                            ) ENGINE=InnoDB;""",  
        'Light' : """CREATE TABLE `PHONE_LIGHT` (
                            `TIMESTAMP` char(13) NOT NULL,
                            `DEVICE_ID` varchar(40) NOT NULL,
                            `DOUBLE_LIGHT_LUX` float ,
                            `ACCURACY` smallint ,
                            `MAX_RANGE` mediumint
                            ) ENGINE=InnoDB;""",       
        'Location' : """CREATE TABLE `PHONE_LOCATIONS` (
                            `TIMESTAMP` char(13) NOT NULL,
                            `DEVICE_ID` varchar(40) NOT NULL,
                            `DOUBLE_LATITUDE` decimal(17,15) ,
                            `DOUBLE_LONGITUDE` decimal(17,15) ,
                            `DOUBLE_BEARING` decimal(25,15) ,
                            `DOUBLE_SPEED` float ,
                            `DOUBLE_ALTITUDE` float ,
                            `PROVIDER` varchar(50) ,
                            `ACCURACY` varchar(100) 
                            ) ENGINE=InnoDB;""",  
        'Screen' : """CREATE TABLE `PHONE_SCREEN` (
                            `TIMESTAMP` char(13) NOT NULL,
                            `DEVICE_ID` varchar(40) NOT NULL,
                            `SCREEN_STATUS` varchar(50) 
                            ) ENGINE=InnoDB;""", 
        'Proximity' : """CREATE TABLE `PHONE_PROXIMITY` (
                            `TIMESTAMP` char(13) NOT NULL,
                            `DEVICE_ID` varchar(40) NOT NULL,
                            `DISTANCE` float ,
                            `MAX_RANGE` float 
                            ) ENGINE=InnoDB;""",    
        'PhysicalActivity' : """CREATE TABLE `PHONE_ACTIVITY_RECOGNITION` (
                            `TIMESTAMP` char(13) NOT NULL,
                            `DEVICE_ID` varchar(40) NOT NULL,
                            `ACTIVITY_NAME` varchar(20) ,
                            `ACTIVITY_TYPE` tinyint ,
                            `CONFIDENCE` tinyint 
                            ) ENGINE=InnoDB;""",                                
        }