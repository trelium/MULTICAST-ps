import requests
import os 
import zipfile
import logging
import argparse
from dotenv import load_dotenv

load_dotenv()

def download_pm_data(path, videos = False, id = ''):
    """
    :path: path to folder where to save files. A folder named pm will be created here. If already present, it will first be deleted.  
    :videos: if True, download also video data, if false download only passive sensing and questionnaire data 
    :id: only download data for one specific participant by specifying a participant id here. if not specified and VIDEOS = True, download data for all users in coaching WITH video files (Caution: Takes very long!)
    """
    pm_folder = os.path.join(path, 'pm')
    if os.path.exists(pm_folder):
        for root, dirs, files in os.walk(pm_folder, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for directory in dirs:
                os.rmdir(os.path.join(root, directory))
        os.rmdir(pm_folder)
    
    # Create a new 'pm' folder
    os.makedirs(pm_folder)

    #get auth info
    auth_url = 'https://pmcp-multicast.bli.uzh.ch/PMCPm-PCMS/api/v00/authentication/login'
    auth_cont = { 'username': os.environ.get('PM_USER'), 'password': os.environ.get('PM_PW'), 'twoFactorKey': "0", 'locale': "en-GB" }
    resp = requests.post(url = auth_url, json=auth_cont)
    auth_tok  = resp.json()
    auth_tok = auth_tok['token'] 
    int_url =  'https://pmcp-multicast.bli.uzh.ch/PMCPm-PCMS/api/v03/team/basics'
    resp = requests.get(url=int_url, headers={'authorization' : auth_tok})
    objectId = resp.json()
    objectId = objectId['interventionOptions'][0]['objectId']['objectId']
    data_url = 'https://pmcp-multicast.bli.uzh.ch/PMCP/api/v04/pcms/downloadInterventionAnalytics/' + objectId


    if len(id) != 0:
        data_url += '/' + id  
    else:
        data_url += '/-'
    if videos:
        data_url += '/-' 
    else:
        data_url += '/without-files'


    resp = requests.get(url=data_url, headers={'authorization' : auth_tok})
    logging.info('Got response')

    with open(os.path.join(pm_folder, "response.zip"), "wb") as f:
        f.write(resp.content)
    logging.info('Data saved')

    with zipfile.ZipFile(os.path.join(pm_folder, "response.zip"), 'r') as zip_ref:
        zip_ref.extractall(path=pm_folder)
        os.remove(os.path.join(pm_folder, "response.zip"))
    logging.info('Data extracted')



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)   
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--path",
        help="path to folder where to save files. A folder named pm will be created here. If already present, it will first be deleted.",
        type=str,  
        required=True   
    )
    parser.add_argument(
        "--id",
        help="participant id, if provided only download data for this participant",
        type=str,
        default=''
    )
    parser.add_argument(
        "--videos",
        help="bool, whether to include videos in this download (for either all or a single participant)",
        action="store_true",
        default=False
        )
    
    args = parser.parse_args()
    download_pm_data(args.path, args.videos, args.id )

    