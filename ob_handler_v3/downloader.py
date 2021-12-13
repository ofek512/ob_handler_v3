"""
Sattelite Data Downloader Script
Created by Lun Surdyaev on 2021-12-03
Last Updated on 2021-12-08
Maintained by Lun Surdyaev lunvang@gmail.com

Description:
This script downloads L2 files needed for further processing.

How-to-Use:
The script will download all L2 files listed in the File Management Database, that fullfill the following criteria:
1. They don't exist on the disk.
2. They haven't been processed yet.
Always run the queuer script before this one, otherwise there won't be any entries in the File Management Database to scan.
If the L2 folder weighs more than a certain threshold, the script will start waiting for the folder size to decrease before downloading more data.
If the folder size doesn't decrease after a certain time, the script will terminate.
If all L2 files in the File Management System have been processed, the script will terminate.

NOTE: Only one instance of this script may be running at a time.
"""

# local imports
import params
import _util as util
import _sqlhandler as sql
import _webhandler as web

# external imports
import os
import time

def FolderTooBig():
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(params.path_to_data): #for every non directory in the path_to_data, add the size of said non directory into total_size
        for f in filenames:
            fp = os.path.join(dirpath, f) # full path of an instance of a non directory in all of the non directories 
            total_size += os.path.getsize(fp) 

    return total_size > (params.max_folder_size * (2**40)) # returns True if the total size in that folder exceeds the max folder size. ***maybe >= instead of >***

def main():

    print("Downloader script started.")

    ready_files = True # *** need to add a check if there are files not ready for download so we can close the loop***
    while ready_files:

        timeout_counter = 0

        # check if folder size exceeds permitted volume
        while FolderTooBig(): 
            print("Data folder size exceeds", params.max_folder_size, "TB. Waiting", params.folder_size_check_interval, "minutes until next check.")
            print(timeout_counter, "tries left until program times out.")

            time.sleep(params.folder_size_check_interval*60)

            timeout_counter += 1

            if timeout_counter >= params.folder_size_check_timeout: 
                print("Downloader script terminated due to the data folder exceeding the permitted size for",
                      params.folder_size_check_interval*params.folder_size_check_timeout,
                      "minutes.")

        # download the next X files
        ready_files = sql.GetReadyForDownload(params.download_chunk_size) # getting a list of tuples from the db, based on priority. [0] is the id and [1] is the download url
        for file in ready_files: 

            # download the file into the data folder
            print("Downloading", file[0] + "...", end=' ', flush=True)
            web.DownloadFile(file[1], params.path_to_data) # sending the DownloadFile method, the download url and also giving it a download path.
            print("Done.")

            # rename and move the file into an appropriate subfolder
            
            # get file properties
            properties = util.GetFileProperties(file[0]) # getting the properties of a specific file according to its ID. identifier is Mission + Sensor
        
            # determine the appropriate folder (MISSION_SENSOR)
            subfolder = params.path_to_data + "L2/" + properties["identifier"] + '/' # giving subfolder a signature of a path. this path may exist or not
            
            # if the folder does not exist, create it
            if not os.path.isdir(subfolder): # if said path doesnt exist, we will create it and notify the user
                os.mkdir(subfolder)
                print(subfolder, "created")
                
            type_subfolder = subfolder + properties["type"] + '/' # *** need to ask lun what is "type" (components[3]) ***
            
            if not os.path.isdir(type_subfolder): # if said subfolder doesnt exist, we will create it and notify the user
                os.mkdir(type_subfolder)
                print(type_subfolder, "created")

            # move the file
            os.rename(params.path_to_data + file[1].split('/')[-1], type_subfolder + file[0]) # *** need to ask lun about the logic in this line ***

            # update database
            sql.FileDownloaded(file[0], type_subfolder)

    print("Downloader script terminated due to no files being queued up for downloading.")

if __name__ == "__main__":
    main()
