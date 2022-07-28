import glob
import logging
import os

from google.cloud import storage


def findOccurrences(s, ch): # to find position of '/' in blob path ,used to create folders in local storage
    return [i for i, letter in enumerate(s) if letter == ch]


def download_from_bucket(bucket_name, blob_path, local_path):    
    # Create this folder locally
    if not os.path.exists(local_path):
        os.makedirs(local_path)        

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs=list(bucket.list_blobs(prefix=blob_path))

    startloc = 0
    for blob in blobs:
        startloc = 0
        folderloc = findOccurrences(blob.name.replace(blob_path, ''), '/') 
        if(not blob.name.endswith("/")):
            if(blob.name.replace(blob_path, '').find("/") == -1):
                downloadpath=local_path + '/' + blob.name.replace(blob_path, '')
                logging.info(downloadpath)
                blob.download_to_filename(downloadpath)
            else:
                for folder in folderloc:
                    
                    if not os.path.exists(local_path + '/' + blob.name.replace(blob_path, '')[startloc:folder]):
                        create_folder=local_path + '/' +blob.name.replace(blob_path, '')[0:startloc]+ '/' +blob.name.replace(blob_path, '')[startloc:folder]
                        startloc = folder + 1
                        os.makedirs(create_folder)
                    
                downloadpath=local_path + '/' + blob.name.replace(blob_path, '')

                blob.download_to_filename(downloadpath)
                logging.info(blob.name.replace(blob_path, '')[0:blob.name.replace(blob_path, '').find("/")])

    logging.info('Blob {} downloaded to {}.'.format(blob_path, local_path))


def upload_to_bucket(bucket_name, blob_path, local_path):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        if os.path.isfile(local_path):
            blob = bucket.blob(
                os.path.join(blob_path, os.path.basename(local_path)).replace("\\","/"))
            blob.upload_from_filename(local_path)
            return
        for item in glob.glob(local_path + '/*'):
            if os.path.isfile(item):
                blob = bucket.blob(
                    os.path.join(blob_path, os.path.basename(item)).replace("\\","/"))
                blob.upload_from_filename(item)
            else:
                upload_to_bucket(bucket_name, 
                os.path.join(blob_path, os.path.basename(item)).replace("\\","/"),
                item)
