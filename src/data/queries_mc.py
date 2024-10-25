TABLES = {
        'LOCATION' : """CREATE TABLE `LOCATION` (
                    `TIMESTAMP` TIMESTAMP NOT NULL,
                    `USER_ID` varchar(40) NOT NULL,
                    `LATITUDE` decimal(9,6) ,
                    `LONGITUDE` decimal(9,6) ,
                    `ACCURACY` float ,  
                    `ALTITUDE` float ,
                    `SOURCE` varchar(50) 
                    ) ENGINE=InnoDB;""",  
        'LOCATION_MORE': """CREATE TABLE `LOCATION_MORE` (
                        `TIMESTAMP` TIMESTAMP NOT NULL,
                        `USER_ID` varchar(40) NOT NULL,
                        `SATELLITES` int,
                        `SPEED` float,
                        `NEWWORKLOCATIONSOURCE` varchar(255) ,
                        `BEARING` float ,
                        `HASBEARING` boolean ,
                        `NEWWORKLOCATIONTYPE` varchar(255) ,
                        `HASSPEED` boolean ,
                        `TRAVELSTATE` varchar(255)
                        ) ENGINE=InnoDB;""",
        'LOCATION_PING' : """CREATE TABLE `LOCATION_PING` (
                        `TIMESTAMP` TIMESTAMP NOT NULL,
                        `USER_ID` varchar(40) NOT NULL,
                        `PING` boolean
                         ) ENGINE=InnoDB;""",   
        'WIFI_CONNECTED' : """CREATE TABLE `WIFI_CONNECTED` (
                        `TIMESTAMP` TIMESTAMP NOT NULL,
                        `USER_ID` varchar(40) NOT NULL,
                        `BSSID` varchar(250) ,
                        `SSID` varchar(250) 
                        ) ENGINE=InnoDB;""",                
        'WIFI_STATE' : """CREATE TABLE `WIFI_CONNECTED` (
                        `TIMESTAMP` TIMESTAMP NOT NULL,
                        `USER_ID` varchar(40) NOT NULL,
                        `WIFI_CONNECTED` boolean ,
                        `WIFI_ENABLED` boolean
                        ) ENGINE=InnoDB;""",     
        'WIFI_SCANNED' : """CREATE TABLE `WIFI_SCANNED` (
                        `TIMESTAMP` TIMESTAMP NOT NULL,
                        `USER_ID` varchar(40) NOT NULL,
                        `BSSID` varchar(250) ,
                        `SSID` varchar(250) ,
                        `FREQUENCY` float ,
                        `CAPABILITY` varchar(250) ,
                        `LEVEL` float 
                        ) ENGINE=InnoDB;""",           
        'BLUETOOTH' : """CREATE TABLE `BLUETOOTH` (
                                    `TIMESTAMP` TIMESTAMP NOT NULL,
                                    `USER_ID` varchar(40) NOT NULL,
                                    `BT_ADDRESS` varchar(150),
                                    `BT_RSSI` mediumint,
                                    `BT_NAME` varchar(150) 
                                    ) ENGINE=InnoDB;""",                                    
        'STEPS_IOS': """CREATE TABLE `STEPS_IOS` (
                                    `USER_ID` varchar(40) NOT NULL,
                                    `START_TIME` TIMESTAMP NOT NULL, 
                                    `END_TIME` TIMESTAMP NOT NULL, 
                                    `STEP_COUNT` mediumint,
                                    `EST_DISTANCE` float,
                                    `FLOORS_ASCENDED` float,
                                    `FLOORS_DESCENDED` float
                                    ) ENGINE=InnoDB;""",
        'STEPS' : """CREATE TABLE `STEPS` (
                                    `START_TIME` TIMESTAMP NOT NULL, 
                                    `END_TIME` TIMESTAMP NOT NULL, 
                                    `USER_ID` varchar(40) NOT NULL,
                                    `STEPS` mediumint
                                    ) ENGINE=InnoDB;""",
        'DEVICE_INFO' : """CREATE TABLE `DEVICE_INFO` (
                                        `USER_ID` varchar(40) NOT NULL,
                                        `DEVICE_ID` varchar(40) NOT NULL,
                                        `OS` varchar(10),
                                        `NAME` varchar(255),
                                        `BUNDLE` varchar(255),
                                        `VERSION` varchar(100),
                                        `PRODUCTTYPE` varchar(100),
                                        `OPERATINGSYSTEMVERSION` varchar(50),
                                        `START_TIME` TIMESTAMP
                                    ) ENGINE=InnoDB;""",  
        'CALL_LOG' : """CREATE TABLE `CALL_LOG` (
                            `TIMESTAMP` TIMESTAMP NOT NULL,
                            `USER_ID` varchar(40) NOT NULL,
                            `CALLID` varchar(150), 
                            `CALLTYPE`  varchar(30),
                            `DURATION` float
                            ) ENGINE=InnoDB;""",  
        'ACTIVITY' : """CREATE TABLE `ACTIVITY` (
                            `TIMESTAMP` TIMESTAMP NOT NULL,
                            `USER_ID` varchar(40) NOT NULL,
                            `ACTIVITY` varchar(20) ,
                            `CONFIDENCE` varchar(20) 
                            ) ENGINE=InnoDB;""",    
        'BRIGHTNESS' : """CREATE TABLE `BRIGHTNESS` (
                            `TIMESTAMP` TIMESTAMP NOT NULL,
                            `USER_ID` varchar(40) NOT NULL,
                            `BRIGHTNESS` float
                            ) ENGINE=InnoDB;""",       
        'SCREEN' : """CREATE TABLE `SCREEN` (
                            `TIMESTAMP` TIMESTAMP NOT NULL,
                            `USER_ID` varchar(40) NOT NULL,
                            `LOCKSTATE` boolean 
                            ) ENGINE=InnoDB;""", 
        'BATTERY_STATE' : """CREATE TABLE `BATTERY_STATE` (
                            `TIMESTAMP` TIMESTAMP NOT NULL,
                            `USER_ID` varchar(40) NOT NULL,
                            `BATTERY_STATE` varchar(20) 
                            ) ENGINE=InnoDB;""",    
        'BATTERY_LEVEL' : """CREATE TABLE `BATTERY_LEVEL` (
                            `TIMESTAMP` TIMESTAMP NOT NULL,
                            `USER_ID` varchar(40) NOT NULL,
                            `BATTERY_LEFT` tinyint 
                            ) ENGINE=InnoDB;""" ,
        'SMS' :  """CREATE TABLE `SMS` (
                    `ADDRESS` varchar(255),
                    `TYPE` tinyint,
                    `TIMESTAMP` timestamp,
                    `READ` tinyint,
                    `BODY` smallint,
                    `STATUS` tinyint,
                    `THREAD_ID` smallint
                    ) ENGINE=InnoDB;"""                                
             
        }