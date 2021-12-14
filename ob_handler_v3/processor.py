"""
Sattelite Data Processor Script
Created by Lun Surdyaev on 2021-12-03
Last Updated on 2021-12-13
Maintained by Lun Surdyaev lunvang@gmail.com

Description:
This script looks for L2 files that are ready to get processed, and processes them.

How-to-Use:
This script automatically finds unprocessed L2 files, processes them to L3m, and deletes the L2 and L3b raw data.
It is multithreaded, and runs multiple workers that do the actual work.
If no L2 files are available, the script will wait until they appear.
If a certain time passes without any L2 files available to be processed, the script terminates.

NOTE: Only one instance of this script may be running at a time.
"""

# local imports
import _util as util
import _sqlhandler as sql
import params

import subprocess as sp
from queue import Queue
from threading import Thread
from datetime import datetime
import os
import time

class Worker(Thread):
    def __init__(self, queue, id):
        Thread.__init__(self)
        self.queue = queue
        self.id = id

    def run(self):
        while True:
            try:
                task = self.queue.get(block=True)
                print("Worker", self.id, "given a task.")
                self.Execute(task)
            except Exception as e:
                print(datetime.now(), "Worker", self.id, "threw an exception:", e)
            finally:
                self.queue.task_done()

    def Execute(self, L2_file_list):

        # get general properties about this batch
        props = util.GetFileProperties(L2_file_list)

        # add full path to L2 filenames
        L2_location = sql.GetFileLocation("L2_files", L2_file_list[0])
        L2_file_list = [L2_location+filename for filename in L2_file_list]

        # l2bin

        # if the L3b directory does not exist, create it
        if not os.path.isdir(params.path_to_data + "L3b/"):
            os.mkdir(params.path_to_data + "L3b/")

        # produce L3b filename
        L3bFilename = params.path_to_data + "L3b/" + util.ProduceL3bFilename(L2_file_list[0])

        # prepare input
        input_file = f"/tmp/{sensor_name}{data_type}_l2bin_temp_{props['date']}.txt"
        f = open(input_file, 'w')
        for filename in L2_file_list:
            f.write(filename+"\n")
        f.close()

        args = [
            "l2bin",
            f"ifile={input_file}",
            f"ofile={L3bFilename}",
            f"l3bprod={util.TYPE_TO_PRODUCTS[props['type']]}",
            "resolution=1",
            "verbose=1"
            ]
        
        print(datetime.now(), "Worker", self.id, "started binning", L3bFilename.split('/')[-1])
        sp.run(args, env=os.environ.copy(), stdout=sp.DEVNULL)

        # l3mapgen

        # if the L3m directory does not exist, create it
        if not os.path.isdir(params.path_to_data + "L3m/"):
            os.mkdir(params.path_to_data + "L3m/")

        # if the mission directory does not exist, create it
        mission_subdirectory = params.path_to_data + "L3m/" + props["identifier"] + '/'
        if not os.path.isdir(mission_subdirectory):
            os.mkdir(mission_subdirectory)
        # if the type subdirectory does not exist, create it
        type_subdirectory = mission_subdirectory + props["type"] + '/'
        if not os.path.isdir(type_subdirectory):
            os.mkdir(type_subdirectory)

        L3mFilename = type_subdirectory + util.ProduceL3mFilename(L2_file_list[0])
        
        args = [
            "l3mapgen",
            f"ifile={L3bFilename}",
            f"ofile={L3mFilename}",
            f"product={util.TYPE_TO_PRODUCTS[props['type']]}",
            "resolution=1",
            "verbose=1",
            "interp=area"
            ]
        
        print(datetime.now(), "Worker", self.id, "started mapping", L3bFilename.split('/')[-1])
        sp.run(args, env=os.environ.copy(), stdout=sp.DEVNULL)

        # if processing successful delete raw data (L2 & L3b)
        if os.path.isfile(L3m_filename):
            print(datetime.now(), "Worker", self.id, "finished mapping", L3bFilename.split('/')[-1], "now deleting input files.")
            # delete files
            for filename in L2_file_list:
                os.remove()
            
            # update DB entries' statuses to 2 (processed)
            for filename in L2_file_list:
                sql.UpdateStatus("L2_files", filename, 2)
            # update L3m DB entry status to 1 (exists)
            sql.UpdateStatus("L3m_files", L3m_filename.split('/')[-1], 1)

            print(datetime.now(), "Worker", self.id, "finished task successfully.")
        else:
            # alert user
            print(datetime.now(), "Worker", self.id, "didn't produce any output.")

def LoadEnvVariables():
    source = f"source {os.environ['OCSSWROOT']}/OCSSW_bash.env"
    dump = '/usr/bin/python3 -c "import os, json;print(json.dumps(dict(os.environ)))"'
    pipe = sp.Popen(['/bin/bash', '-c', '%s && %s' %(source,dump)], stdout=sp.PIPE)
    env = json.loads(pipe.stdout.read())
    os.environ = env

# returns a list of L2 files that have the same target and are all downloaded, but not processed yet
def GetTask():
    
    for i in range(params.data_availability_check_timeout):
        # creating a list that contains small lists that in each list, all the L2s have the same L3 "target"
        checker =  True
        initial_list = sql.GetFilesReadyForProcessing() # initial list that contains all the tuples of L2s in the database
        father_list = [] # the list that contains all the lists
        inner_list = [] # the list inside fatherlist that all the tuples have the same L3
        target_check = initial_list[0][2] # the first L3 "target" before the for
        for list_info in initial_list: # this for sorts all the lists into same L3 and stores these lists into the fatherList
            if target_check == list_info[2]: # if the tuple has the same L3 as the target check 
                inner_list.append(list_info)
            else:
                target_check = list_info[2]
                father_list.append(inner_list[:]) # inserting the little list into the father list we used [:] so it wont be a reference but the obj itself!
                inner_list = [] # initialising the litte list
        for inner_list in father_list: # checking for the first inner list that all of the L2s inside are downloaded.
            for inner_list_info in inner_list:
                if inner_list_info[1] != 1: # [1] is the file status. 0 not downloaded 1 downloaded 2 processed
                    checker = False
            if checker:  # if checker is true, it means all the L2 in the list are downloaded and its ready to get processed.
                return [item[0] for item in inner_list] # returning a list of all the relevant L2 files.
        print ("No list was found suited for processing... waiting 60 minutes.")
        time.sleep(util.PROCESSING_DELAY)
        print (params.data_availability_check_timeout - i - 1, "tries left.")

    print ("No batch of L2s with the same L3 target were all ready to be processed.")
    exit("program terminated")

def main():
    # DONT REMOVE THIS
    LoadEnvVariables()

    tasks = Queue()
    for i in range(params.threads):
        worker = Worker(tasks, i)
        worker.start()

    while ThereAreUnprocessedFiles():

        task = GetTask()

        tasks.put(task)

    tasks.join()

if __name__ == "__main__":
    main()
