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
    # returns a list of L2 files that have the same target and are all downloaded, but not processed yet
def GetTask():
    
    for i in range(params.data_availability_check_timeout):
        #creating a list that contains small lists that in each list, all the L2s have the same L3 "target"
        checker =  True
        initial_list = sql.GetFilesReadyForProcessing()#initial list that contains all the tuples of L2s in the database
        fatherList = [] #the list that contains all the lists
        littlemanlist = [] #the list inside fatherlist that all the tuples have the same L3
        targetCheck = initial_list[0][2]#the first L3 "target" before the for
        for man in initial_list #this for sorts all the lists into same L3 and stores these lists into the fatherList
            if targetCheck == man[2]:#if the tuple has the same L3 as the target check
                littlemanlist.append(man)
            else:
                targetCheck = man[2]
                fatherList.append(littlemanlist[:])#inserting the little list into the father list
                littlemanlist = []#initialising the litte list
        for littlemanlist in fatherList #checking for the first little list that all of the L2s inside are downloaded.
            for alldashid in littlemanlist:
                if alldashid[1] != 1:
                    checker = False
            if checker: #if checker is true, it means all the L2 in the list are downloaded and its ready to get processed.
                return [item[0] for item in littlemanlist]
        print ("No list was found suited for processing... waiting 60 minutes.")
     

    print ("No batch of L2s with the same L3 target were all ready to be processed.")
    exit("program terminated")


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
