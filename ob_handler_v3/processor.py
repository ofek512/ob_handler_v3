"""
Sattelite Data Processor Script
Created by Lun Surdyaev on 2021-12-03
Last Updated on 2021-12-07
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
import _util as util
import _sqlhandler as sql
import params
import time

class Worker:
    def __init__(self):
        pass

    def Execute(self, ):

        self.busy = True

        # l2bin

        # l3mapgen

        # if processing successful delete raw data (L2 & L3b)
        if success:
            pass
            # delete files

            # update DB

        self.busy = False

# returns a list of L2 files that have the same target and are all downloaded, but not processed yet
def GetTask():
    pass

def main():

    workers = [Worker() for i in range(params.threads)]
    ready_files = True
    while ready_files:

        for worker in workers:
            if not worker.busy:
                worker.Execute(GetTask())

        time.sleep(util.PROCESSING_DELAY)

if __name__ == "__main__":
    main()