""" Contains Gdrive authorization and contains a bunch of useful functions to load data """ 

import io
import pandas as pd
import numpy as np
import boto3
import pickle

import zipfile
import tempfile
import os

from apiclient import discovery
from httplib2 import Http
import oauth2client
from oauth2client import file, client, tools
from googleapiclient.http import MediaIoBaseDownload

from config.project_config import GDRIVE_SECRET

# authenticate your identity using the JSON credentials and provide access to your Google Drive #
# this step will create file call token.json if authenticate successfully
from apiclient import discovery
from httplib2 import Http
import oauth2client
from oauth2client import file, client, tools
obj = lambda: None
lmao = {"auth_host_name":'localhost', 'noauth_local_webserver':'store_true', 'auth_host_port':[8080, 8090], 'logging_level':'ERROR'}
for k, v in lmao.items():
    setattr(obj, k, v)
    
# authorization boilerplate code for gdrive
SCOPES = "https://www.googleapis.com/auth/drive.readonly"
store = file.Storage("token.json")
creds = store.get()
# creds = None
# The following will give you a link if token.json does not exist, the link allows the user to give this app permission
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets(
        GDRIVE_SECRET,
        SCOPES,
    )
    creds = tools.run_flow(flow, store, obj)

s3 = boto3.client("s3")

def upload_file_to_s3(local_path, s3_key, bucket_name):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    bucket.upload_file(
        Filename=local_path,
        Key=s3_key
    )
    
    
def download_file_from_s3(Filename, s3_key, bucket_name="hal-bi-bucket"):
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucket_name)
    bucket.download_file(Filename=Filename, Key=s3_key)


def load_nn_offline_train_data(train_model_file_path):
    final_1 = pd.read_csv(train_model_file_path )

    del final_1["Unnamed: 0"]
    del final_1["date_released"]

    final_1 = final_1[~final_1.isin([np.nan, np.inf, -np.inf]).any(1)]
    final_1["giveaway"] = np.where(final_1["giveaway"] > 0, 1, 0)
    
    return final_1


def load_label_encoder(s3bucket, label_encoder_path):
    object_ = s3.get_object(
        Bucket=s3bucket,
        Key=label_encoder_path,
    )
    serializedObject = object_["Body"].read()
    dict_all_loaded = pickle.loads(serializedObject)
    return dict_all_loaded


def load_external_data(filename: str(), fileid: str() , folderpath: str()) -> None:
    DRIVE = discovery.build("drive", "v3", http=creds.authorize(Http()))
    request = DRIVE.files().get_media(fileId=fileid)
    fh = io.FileIO(f'{folderpath}{filename}',
        mode="w",
    )
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))

        
def _zipdir(path, ziph):
    ''' helpder function for save_model_to_s3 '''
    # Zipfile hook to zip up model folders
    length = len(path) # Doing this to get rid of parent folders
    for root, dirs, files in os.walk(path):
        folder = root[length:] # We don't need parent folders! Why in the world does zipfile zip the whole tree??
        for file in files:
            ziph.write(os.path.join(root, file), os.path.join(folder, file))

        
def save_model_to_s3(model_name, s3_model_save_path, s3_bucket):
    ''' Zips a model in a temp folder and uploads that to S3
    Args:
        model_name: name of the model that gets saved
        s3_model_save_path: path to s3 where the model should be saves
        s3_bucket: bucket
    '''
    with tempfile.TemporaryDirectory() as tempdir:
        model_name.save(f"{tempdir}/{model_name}")
        # Zip it up first
        zipf = zipfile.ZipFile(f"{tempdir}/{model_name}.zip", "w", zipfile.ZIP_STORED)
        _zipdir(f"{tempdir}/{model_name}", zipf)
        zipf.close()
        upload_file_to_s3(f"{tempdir}/{model_name}.zip",s3_model_save_path, s3_bucket)
