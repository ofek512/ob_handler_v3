"""
File Management Database Verifier Script
Created by Lun Surdyaev on 2021-12-03
Last Updated on 2022-01-03
Maintained by Ofek Yankis ofek5202@gmail.com

Description:
The script will check for inconsistensies in the file management database, and if found, will display and automatically fix them.

How-to-Use:
Run this script while no other scripts relating to the files are running (queuer.py, downloader.py, processor.py)
The inconsistensies come in three types:
1. A missing file being labeled as existing.
2. An existing file being labeled as missing.
3. A processed file being labeled as unprocessed.
In addition, the script will scan for unnecessary files (low-level files being kept post-processing).
A prompt will be displayed to the user, inquiring whether to delete the unnecessary files or not.
"""

import params
import _util as util
import _sqlhandler as sql

def HandleL2():
    
    hasTarget = false
    hasTarget = 
    print("Getting list of all L3m files on the disk...", end=' ', flush=True)
    l2List = util.GetExistingFilenamesAndPaths(params.path_to_data+"L2/") # gets a list that [0] is the location and [1] is the filename
    print("Done.")

    print("Verifying those files' entries in the database...", flush=True)
    location_pointer = 0
    for filename in l2List[1]:

        # get file status
        status = sql.GetFileStatus("L2_files", filename)
       
        # if the file isn't in the DB at all, notify user and insert it
        if status is None:
            print("The file", filename, "was not found in the database. Inserting it.")
            entry = {"id": filename, "location": l2List[0][location_pointer], "file_status": 1}
            sql.InsertL3m(entry)

        # else, check if the database displays correct status
        elif status == 0:
            print("The file", filename, "was listed as missing despite its presence on the disk. Marking it as existing.")
            sql.UpdateStatus("L3m_files", filename, 1)

        # as this file was dealt with, set its verifier bit to 1
        sql.VerifyFile("L2_files", filename)
        location_pointer += 1
    print("Existing file verification done.")

    # if there are any database entries remaining that are listed as existing,
    # but aren't on the disk, notify user and update DB
    print("Verifying that no missing files were marked as existing...")
    unverified_existing = sql.GetUnverifiedExisting("L3m_files") # need to add the function
    for id in unverified_existing:
        print("The file", id, "is listed as existing on the DB, but is not present on the disk. Marking it as missing.")
        sql.UpdateStatus("L2_files", id, 0)

    # reset the verifier bit to 0
    sql.ResetVerifier("L2_files")

    print("L2 Verification done.")


def HandleL3m():

    print("Getting list of all L3m files on the disk...", end=' ', flush=True)
    l3List = util.GetExistingFilenamesAndPaths(params.path_to_data+"L3m/") # gets a list that [0] is the location and [1] is the filename
    print("Done.")

    print("Verifying those files' entries in the database...", flush=True)
    location_pointer = 0
    for filename in l3List[1]:

        # get file status
        status = sql.GetFileStatus("L3m_files", filename)
       
        # if the file isn't in the DB at all, notify user and insert it
        if status is None:
            print("The file", filename, "was not found in the database. Inserting it.")
            file_location = l3List[0][location_pointer] 
            sql.UpdateL3m(file_location, filename)

        # else, check if the database displays correct status
        elif status == 0:
            print("The file", filename, "was listed as missing despite its presence on the disk. Marking it as existing.")
            sql.UpdateStatus("L3m_files", filename, 1)

        # as this file was dealt with, set its verifier bit to 1
        sql.VerifyFile("L3m_files", filename)
        location_pointer += 1
    print("Existing file verification done.")

    # if there are any database entries remaining that are listed as existing,
    # but aren't on the disk, notify user and update DB
    print("Verifying that no missing files were marked as existing...")
    unverified_existing = sql.GetUnverifiedExisting("L3m_files") # need to add the function
    for id in unverified_existing:
        print("The file", id, "is listed as existing on the DB, but is not present on the disk. Marking it as missing.")
        sql.UpdateStatus("L3m_files", id, 0)

    # reset the verifier bit to 0
    sql.ResetVerifier("L3m_files")

    print("L3m Verification done.")

def main():

    # handle L3m files
    HandleL3m()
    
    # handle L2 files
    HandleL2()

if __name__ == "__main__":
    main()
