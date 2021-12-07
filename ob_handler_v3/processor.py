"""
Sattelite Data Processor Script
Created by Lun Surdyaev on 2021-12-03
Last Updated on 2021-12-04
Maintained by Lun Surdyaev lunvang@gmail.com

Description:
This script looks for L2 files that are ready to get processed, and processes them.

How-to-Use:
This script automatically finds unprocessed L2 files, processes them to L3m, and deletes the L2 and L3b raw data.
It is multithreaded, and runs multiple child processes that do the actual work.
If no L2 files are available, the script will wait until they appear.
If a certain time passes without any L2 files available to be processed, the script terminates.

NOTE: Only one instance of this script may be running at a time.
"""

# local imports
import _sqlhandler as sql

def main():
    pass

if __name__ == "__main__":
    main()