import logging 
import pandas as pd
import os
import json
from json.decoder import JSONDecodeError
import ast
import sys
import re

#TODO input values might contain quotation marks, semicolons, % and _ wildcard characters

logger = logging.getLogger('make_db')

def parse_ios_df(df_ex, dbloc):
    """Take as input one pandas dataframe for ios sensing streams and 
    return a dictionary with keys corresponding to the sensing stream ids contained in the input dataframe 
    and pandas dataframes of the desired format as values """
    
    #TODO check in the final device info table whether there are multiple device ids connected to the same user_id. 
    # If so it makes more sense to save data in sensor tables by device id and not user id, for ios. 
    
    df_ex['timestamp'] = pd.to_datetime(df_ex['timestamp'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')
    ret = dict()
    ret['DEVICE_INFO'] = df_ex[['user_id','device_id']].copy()
    ret['DEVICE_INFO'] = ret['DEVICE_INFO'].assign(os='ios')
    #ret['DEVICE_STATE'] = pd.DataFrame()

    def explode_json(df,ev_id, drop_timestamp = False):
        if drop_timestamp: 
            df1 = df.loc[df['event_id'] == ev_id].drop(['timestamp'],axis = 1)
        else:
            df1 = df.loc[df['event_id'] == ev_id]
        try:    
            df2 = df.loc[df['event_id'] == ev_id,'data'].apply(json.loads).apply(pd.Series)
        except JSONDecodeError:
            try:
                df2 = df.loc[df['event_id'] == ev_id,'data'].apply(lambda x: x.decode('utf-8'))
                df2 = df2.apply(lambda x: x + '}' if not x.strip().endswith('}') else x)
                df2 = df2.apply(lambda x: x + '"}' if not x.strip().endswith('"}') else x)
                df2 = df2.apply(json.loads).apply(pd.Series)    
            except JSONDecodeError:
                ex_type, ex_value, ex_traceback = sys.exc_info()
                logger.warning(f"Malformed json at location: {dbloc} --- error: {ex_value}")
                return pd.DataFrame()
        
        return pd.concat([df1, df2], axis = 1)

    for ev_id in df_ex['event_id'].unique():
        #ret = dict()
        if ev_id == 151: 
            dfp = explode_json(df_ex,ev_id)
            dfp = dfp[['timestamp', 'user_id', 'latitude', 'longitude', 'accuracy', 'altitude']]
            dfp['source'] = 'all'
            try:
                pd.concat([ret['LOCATION'],dfp])
            except KeyError:
                ret['LOCATION'] = dfp
        elif ev_id == 152:
            dfp = explode_json(df_ex,ev_id)        
            dfp = dfp[['timestamp', 'user_id','latitude', 'longitude', 'accuracy', 'altitude']]
            dfp['source'] = 'app'
            try:
                pd.concat([ret['LOCATION'],dfp])
            except KeyError:
                ret['LOCATION'] = dfp
        elif ev_id == 18: 
            dfp = explode_json(df_ex,ev_id,drop_timestamp=True)
            dfp['timestamp'] = pd.to_datetime(dfp['timestamp'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')
            ret['WIFI_CONNECTED'] = dfp[['timestamp', 'user_id', 'bssid', 'ssid']]
        elif ev_id == 181: 
            dfp = explode_json(df_ex,ev_id,drop_timestamp=True)
            dfp['timestamp'] = pd.to_datetime(dfp['timestamp'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')        
            #ret['DEVICE_STATE'] = pd.concat([ret['DEVICE_STATE'], dfp[['start_date', 'end_date', 'user_id', 'wifi_connected_old', 'wifi_connected', 'wifi_enabled']] ], axis = 1)
            ret['WIFI_STATE'] = dfp[['timestamp', 'user_id', 'wifi_connected', 'wifi_enabled']].drop_duplicates()
        elif ev_id == 19:   
            dfp = explode_json(df_ex,ev_id,drop_timestamp=True)
            dfp['timestamp'] = pd.to_datetime(dfp['timestamp'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')
            ret['BLUETOOTH'] = dfp[['timestamp', 'user_id', 'bt_address', 'bt_rssi', 'bt_name']] 
        elif ev_id ==  21:              # "1709500823,15,9.089999999850988,0,0 #will keep the timestamp present in the 'data' field 
            dfp = df_ex.loc[df_ex['event_id'] == ev_id, ['timestamp', 'user_id', 'data']]
            dfp['data'] = dfp['data'].str.decode("utf-8")
            dfp = pd.concat([dfp[['user_id','timestamp']], dfp['data'].str.split(',', expand=True)], axis=1)
            dfp.rename(columns={'timestamp' : 'start_time', 
                                0 : 'end_time',
                                1 : 'step_count',
                                2 : 'est_distance',
                                3 : 'floors_ascended',
                                4 : 'floors_descended'
                                                }, inplace = True ) 
            dfp['end_time'] = pd.to_datetime(dfp['end_time'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')   
            dfp['start_time'] = pd.to_datetime(dfp['start_time'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')         
            ret['STEPS_IOS'] = dfp
        elif ev_id == 22:   
            dfp = explode_json(df_ex,ev_id)
            if not dfp.empty:
                dfp = dfp.loc[dfp['sample_type'] == 'HKQuantityTypeIdentifierStepCount']
                dfp['sample_quantity'] = [x[:-6] for x in dfp['sample_quantity']]
                daytime_mapping = {
                    'am Namittag': 'PM', 
                    'am Vormittag' : 'AM'
                    }
                for key, value in daytime_mapping.items():
                    dfp['start_date'] = dfp['start_date'].str.replace(key, value)
                    dfp['end_date'] = dfp['end_date'].str.replace(key, value)

                dfp['start_date'] = pd.to_datetime(dfp['start_date'], infer_datetime_format=True).dt.tz_convert('Europe/Zurich')
                dfp['end_date'] = pd.to_datetime(dfp['end_date'], infer_datetime_format=True).dt.tz_convert('Europe/Zurich')
                dfp.rename(columns={'end_date' : 'end_time',
                                    'start_date' : 'start_time',
                                    'sample_quantity' : 'steps'}, inplace=True)

                ret['STEPS'] = dfp[['start_time', 'end_time','user_id', 'steps']]

                pattern = r'name:(.*?), bundle:(.*?), version:(.*?), productType:(.*?), operatingSystemVersion:(.*?)>'
                dfp[['name', 'bundle', 'version', 'productType', 'operatingSystemVersion']] = dfp['source'].str.extract(pattern)

                #dfp['operatingSystemVersion'] = dfp['source'].str.split("""\n""", expand=True)[2].str.split("""=""", expand=True)[1].str.strip('"').str.strip(';')
                #dfp['productType'] = dfp['source'].str.split("""\n""", expand=True)[3].str.split("""=""", expand=True)[1].str.strip('"').str.strip(';')
                #ret['DEVICE_STATE'] = pd.concat([ret['DEVICE_STATE'], dfp[['user_id','start_date','end_date','operatingSystemVersion']]], axis = 0)
                #ret['DEVICE_INFO'].loc[:,'productType'] = dfp['productType'].iloc[0]

                dfp['user_id'] = ret['DEVICE_INFO']['user_id'][0]
                dfp['device_id'] = ret['DEVICE_INFO']['device_id'][0]
                dfp['os'] = ret['DEVICE_INFO']['os'][0]
                ret['DEVICE_INFO'] = dfp[['user_id','device_id','os','name', 'bundle', 'version', 'productType', 'operatingSystemVersion','start_time']]
        elif ev_id == 23:   
            dfp = explode_json(df_ex,ev_id,drop_timestamp=True)   
            dfp = dfp.astype({"duration": float})
            dfp['timestamp'] = pd.to_datetime(dfp['timestamp'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')
            ret['CALL_LOG'] = dfp[['timestamp', 'user_id', 'callId', 'callType', 'duration']] #TODO harmonize with ios 'Disconnected', unknown, dialing, connected, incoming 
        elif ev_id == 987:     
            dfp = explode_json(df_ex,ev_id)
            dfp.rename(columns={
                                0 : 'heart_beat'}, inplace=True)
            ret['HEART_BEAT'] = dfp[['timestamp', 'user_id','heart_beat']]  
        elif ev_id == 16:  
            dfp = df_ex.loc[df_ex['event_id'] == ev_id, ['user_id', 'timestamp', 'data']]
            dfp['data'] = dfp['data'].str.decode("utf-8")
            dfp = pd.concat([dfp[['timestamp','user_id']], dfp['data'].str.split(',', expand=True)], axis=1)
            dfp.rename(columns={
                                0 : 'activity',
                                1 : 'confidence'
                                                }, inplace = True )
            ret['ACTIVITY'] = dfp[['timestamp','user_id','activity', 'confidence']]
        elif ev_id == 13:  
            dfp = explode_json(df_ex,ev_id,drop_timestamp=True)   
            dfp = dfp.astype({"brightness": float})
            dfp['timestamp'] = pd.to_datetime(dfp['timestamp'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')
            ret['BRIGHTNESS'] = dfp[['timestamp', 'user_id', 'brightness']]
        
        elif ev_id == 14:    
            dfp = explode_json(df_ex,ev_id)   
            dfp = dfp.astype({'LockState': float})
            dfp = dfp.astype({'LockState': bool})
            ret['SCREEN'] = dfp[['timestamp', 'user_id','LockState']]

        elif ev_id == 111:
            dfp = explode_json(df_ex,ev_id, drop_timestamp=True) 
            dfp['timestamp'] = pd.to_datetime(dfp['timestamp'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')
            #dfp.rename(columns={'timestamp' : 'start_date'}, inplace = True)
            dfp = dfp.astype({"battery_state": int})
            dfp["battery_state"] = dfp["battery_state"].map( {2: 'charging', 
                                                              1: 'unplugged', 
                                                              3: 'full', 
                                                              0: 'unknown'})
            #ret['DEVICE_STATE'] = pd.concat([ret['DEVICE_STATE'], dfp[['user_id','start_date','battery_state']]])
            ret['BATTERY_STATE'] = dfp[['timestamp','user_id','battery_state']]

        elif ev_id == 11:    #battery
            dfp = explode_json(df_ex,ev_id, drop_timestamp=True)   
            dfp['timestamp'] = pd.to_datetime(dfp['timestamp'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')
            dfp = dfp.astype({'battery_left': float})
            ret['BATTERY_LEVEL'] = dfp[['timestamp', 'user_id','battery_left']]
        
    try:
        ret['DEVICE_INFO'] = ret['DEVICE_INFO'].drop_duplicates(subset=['user_id','device_id','os','name', 'bundle', 'version', 'productType', 'operatingSystemVersion'])
    except KeyError:
        ret['DEVICE_INFO'] = ret['DEVICE_INFO'].drop_duplicates()
    #ret['DEVICE_INFO'].rename(columns={ 'productType' : 'product_type'}, inplace=True)

        
    return ret
        

def parse_and_df(df_ex, dbloc):
    def explode_json(df,ev_id, drop_timestamp = False):
        if drop_timestamp: 
            df1 = df.loc[df['event_id'] == ev_id].drop(['timestamp'],axis = 1)
        else:
            df1 = df.loc[df['event_id'] == ev_id]
        df2 = df.loc[df['event_id'] == ev_id,'data'].apply(json.loads).apply(pd.Series)   
        return pd.concat([df1, df2], axis = 1)

    def extract_from_json_list(row):
        # Convert the bytes to string
        json_str = row['data'].decode('utf-8')
        # Convert the string to a dictionary
        json_dict = json.loads(json_str)
        # Extract the data list and attach timestamp and user_id
        key = list(json_dict.keys())[0]
        return [
                    {**entry, 'timestamp': row['timestamp'], 'user_id': row['user_id']} 
                    for entry in json_dict[key]
                ]


    df_ex['timestamp'] = pd.to_datetime(df_ex['timestamp'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')
    df_ex['user_id'] = dbloc.split(os.sep)[-1].split('_')[1].split('.')[0] #get a portion of the folder name 
    ret = dict()
    ret['DEVICE_INFO'] = df_ex[['user_id']].copy()
    ret['DEVICE_INFO'] = ret['DEVICE_INFO'].assign(os='android')
    #ret['DEVICE_STATE'] = pd.DataFrame()


    for ev_id in df_ex['event_id'].unique():
        if ev_id == 171:    
            dfp = explode_json(df_ex,ev_id, drop_timestamp=False)   
            #dfp['timestamp'] = pd.to_datetime(dfp['timestamp'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')
            dfp.rename(columns={'level' : 'battery_left', "state": 'battery_state'}, inplace = True)
            dfp = dfp.astype({'battery_left': float, "battery_state": int})
            dfp['battery_state'] = dfp['battery_state'].map({2:'charging', 
                                                            3 : 'unplugged',  #'discharging' is changed to match with ios 
                                                            5 : 'full', 
                                                            4 : 'unplugged', #'not_charging' is changed to match with ios 
                                                            1 : 'unknown'})
            ret['BATTERY_LEVEL'] = dfp[['timestamp', 'user_id','battery_left']]
            ret['BATTERY_STATE'] = dfp[['timestamp', 'user_id','battery_state']]
            
        elif ev_id == 2: 
            dfp = explode_json(df_ex,ev_id)
            dfp.rename(columns={
                                'ALTITUDE' : 'altitude',
                                'LONGITUDE' : 'longitude',
                                'LATITUDE' : 'latitude',
                                'ACCURACY' : 'accuracy',
                                'PROVIDER' : 'source',
                                'STAELLITES' : 'satellites'
                                                }, inplace = True )     
            ret['LOCATION'] = dfp[['timestamp', 'user_id','latitude', 'longitude', 'accuracy', 'altitude','source']]
            desired_columns = {'timestamp', 'user_id', 'satellites', 'SPEED', 
                   'NEWWORKLOCATIONSOURCE', 'BEARING', 'HASBEARING', 
                   'NEWWORKLOCATIONTYPE', 'HASSPEED', 'TRAVELSTATE'}
            
            present_columns = desired_columns & set(dfp.columns)
            missing_columns = desired_columns - set(dfp.columns)
            df_c = dfp[list(present_columns)].copy()
            df_c[list(missing_columns)] = None
            ret['LOCATION_MORE'] = df_c

        elif ev_id == 902: #location ping one single numerical field: app tried to collect location data
            dfp = explode_json(df_ex, ev_id)    
            dfp.rename(columns={0 : 'ping'}, inplace = True )    
            ret['LOCATION_PING'] = dfp[['timestamp', 'user_id','ping']]
        elif ev_id == 91:  
            dfp = explode_json(df_ex,ev_id,drop_timestamp=False)
            dfp.loc[dfp['state']=='disconnected',['SSID','bssid']] = None
            dfp['bssid'] = dfp['bssid'].astype(object)
            dfp.loc[dfp['state']=='disconnected',['bssid']] = None
            dfp.rename(columns={'SSID' : 'ssid'}, inplace = True)    
            ret['WIFI_CONNECTED'] = dfp[['timestamp', 'user_id', 'bssid', 'ssid']]
        elif ev_id == 9: 
            wifi_data_list = df_ex.loc[df_ex['event_id'] == ev_id].apply(extract_from_json_list, axis=1).explode().tolist()
            wifi_data = pd.json_normalize(wifi_data_list)
            if not wifi_data.empty:
                #wifi_data['timestamp'] = pd.to_datetime(wifi_data['timestamp'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')
                wifi_data.columns = [x.lower() for x in wifi_data.columns]
                ret['WIFI_SCANNED'] = wifi_data
            else:
                par  = df_ex.loc[df_ex['event_id'] == ev_id, ['timestamp', 'user_id']].copy()
                par[['bssid', 'ssid']] = None
                ret['WIFI_SCANNED'] = par
        elif ev_id == 10:   
            devices_data_list = df_ex.loc[df_ex['event_id'] == ev_id].apply(extract_from_json_list, axis=1).explode().tolist()
            devices_df = pd.json_normalize(devices_data_list)
            #devices_df['timestamp'] = pd.to_datetime(devices_df['timestamp'], unit='s', utc=True).dt.tz_convert('Europe/Zurich')
            if not devices_df.empty:
                devices_df.rename(columns={'DEVICE' : 'bt_address', 'RSSI' : 'bt_rssi', 'CLASS' : 'bt_class'}, inplace = True)
                ret['BLUETOOTH'] = devices_df[['timestamp', 'user_id', 'bt_address', 'bt_rssi','bt_class']]
            else:
                par = df_ex.loc[df_ex['event_id'] == ev_id, ['timestamp','user_id']].copy()
                par[['bt_address', 'bt_rssi','bt_class']] = None
                ret['BLUETOOTH'] = par
             
        elif ev_id ==  202:      
            dfp = explode_json(df_ex,ev_id, drop_timestamp = True)
            dfp['start_time'] = pd.to_datetime(dfp['start_time'], unit='ms', utc=True).dt.tz_convert('Europe/Zurich') 
            dfp['end_time'] = pd.to_datetime(dfp['end_time'], unit='ms', utc=True).dt.tz_convert('Europe/Zurich')        
            ret['STEPS'] = dfp[['start_time', 'end_time','user_id', 'steps', 'steps_since_boot', 'time_since_boot']]
        elif ev_id == 210:   #"[{'number': '83653d9d0e8628eb301cef41df5722502f50eb94', 'type': 2, 'date': 1701967333263, 'duration': 73}]"
            ll = df_ex.loc[df_ex['event_id'] == 210,'data'].iloc[0].decode('utf-8')
            ll = ast.literal_eval(ll)
            #restructure the data field so that it matches the wifi data format, to use already built functions
            df_ex.loc[df_ex['event_id'] == 210, 'data'] = json.dumps({'calls' : ll}).encode('utf-8')
            calls_data_list = df_ex.loc[df_ex['event_id'] == 210].apply(extract_from_json_list, axis=1).explode().tolist()
            calls_data = pd.json_normalize(calls_data_list)
            calls_data.drop(['timestamp'],axis = 1, inplace=True) #drop timestamp relative to data dump
            calls_data.rename(columns={'date' : 'timestamp', 'type':'callType', 'number' : 'callId'}, inplace = True) #keep only internal timestamp
            calls_data['callType'] = calls_data['callType'].map({1: 'incoming', #connected ?
                                                                 2: 'outgoing', #dialing ?
                                                                 3: 'missed', 
                                                                 4: 'voicemail', 
                                                                 5: 'rejected', 
                                                                 6: 'blocked', 
                                                                 7: 'answered_externally'}) 
            #'Disconnected', unknown, dialing, connected , incoming  
            calls_data = calls_data.astype({"duration": float})
            ret['CALL_LOG'] = calls_data[['timestamp', 'user_id', 'callId', 'callType', 'duration']] 
        elif ev_id == 136:    
            dfp = explode_json(df_ex,ev_id)   
            dfp.rename(columns={
                                'screen_state' : 'LockState'}, inplace = True )
            dfp = dfp.astype({'LockState': float})
            dfp = dfp.astype({'LockState': bool})
            ret['SCREEN'] = dfp[['timestamp', 'user_id','LockState']]
        elif ev_id == 211:     #"[{'address': 'a41b5e08e76671e7507e7e83e69d71305ad163e8', 'type': 1, 'date': 1699082630522, 'read': 1, 'body': 18, 'status': -1, 'thread_id': 6}]",
            ll = df_ex.loc[df_ex['event_id'] == 211,'data'].iloc[0].decode('utf-8')
            ll = ast.literal_eval(ll)
            #restructure the data field so that it matches the wifi data format, to use already built functions
            df_ex.loc[df_ex['event_id'] == 211, 'data'] = json.dumps({'calls' : ll}).encode('utf-8')
            sms_data_list = df_ex.loc[df_ex['event_id'] == 211].apply(extract_from_json_list, axis=1).explode().tolist()
            sms_data = pd.json_normalize(sms_data_list)
            sms_data.drop(['timestamp'],axis = 1, inplace=True)
            sms_data.rename(columns={'date' : 'timestamp'}, inplace = True)
            sms_data['timestamp'] = pd.to_datetime(sms_data['timestamp'], unit = 'ms', utc=True).dt.tz_convert('Europe/Zurich') 
            ret['SMS'] = sms_data
        elif ev_id == 22:  #timestamp	time_in_foreground	package_name	package_category	user_id
            apps_data_list = df_ex.loc[df_ex['event_id'] == 22].apply(extract_from_json_list, axis=1).explode().tolist()
            apps_df = pd.json_normalize(apps_data_list)
            apps_df.drop(['timestamp'],axis = 1, inplace=True) #drop timestamp relative to data dump
            apps_df['last_time_used'] = pd.to_datetime(apps_df['last_time_used'], unit='ms', utc=True).dt.tz_convert('Europe/Zurich')
            apps_df.rename(columns={'last_time_used' : 'timestamp'}, inplace = True)
            ret['APP_USAGE'] = apps_df 
        elif ev_id == 11: 
            ret['SERVICES_STARTED'] = [] 
        elif ev_id == 199:  
            ret['SERVICES_RUNNING'] = [] 
        elif ev_id == 301:  
            dfp = explode_json(df_ex,301, drop_timestamp=False)  
            dfp.drop(['data'],axis = 1, inplace=True)
            ret['NOTIFICATIONS'] = dfp

    ret['DEVICE_INFO'].assign(DEVICE_ID='not_provided') #for compatibility with sql schema 
    ret['DEVICE_INFO'] = ret['DEVICE_INFO'].drop_duplicates()
    return ret        