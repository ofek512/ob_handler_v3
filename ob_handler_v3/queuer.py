"""
Sattelite Data Download Queuing Script
Created by Lun Surdyaev on 2021-12-03
Last Updated on 2021-12-04
Maintained by Lun Surdyaev lunvang@gmail.com

Description:
This script looks up and stores URLs to L2 files to be downloaded.

How-to-Use:
The script will ask for the following information:
1. A list of missions
2. A time interval, in date form
And will return a list of L2 files that match the criteria and are ready to be downloaded.
That list will then get stored in the File Management Database, to be accessed by the downloader script.

NOTE: This script doesn't actually download any data.
That is done by the downloader script.
This script solely provides the URLs for the downloader script to use.

NOTE 2: This script can be run during downloading/processing of data, to add more files to the download queue.
"""

def main():
    pass

if __name == "__main__":
    main()