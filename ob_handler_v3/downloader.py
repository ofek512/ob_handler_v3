"""
Sattelite Data Downloader Script
Created by Lun Surdyaev on 2021-12-03
Last Updated on 2021-12-04
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

def main():
    pass

if __name == "__main__":
    main()