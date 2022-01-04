"""
SQL Handling Utility
Created by Lun Surdyaev on 2021-12-04
Last Updated on 2022-01-03
Maintained by Ofek Yankis ofek5202@gmail.com
Description:
This is a utility for handling the SQL-based File Management Database.
It uses the constants defined in params.py to find the database, and provides an interface to it via its functions.
"""

# local imports
import params
import _util as util

import os
import sqlite3
from datetime import datetime

# queries
create_tables = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS L3m_files (  id              TEXT PRIMARY KEY,
                                        location        TEXT,
                                        file_status     INTEGER,
                                        created_at      TEXT,
                                        verifier_bit    INTEGER DEFAULT 0,
                                        UNIQUE(id)
                                        );
CREATE TABLE IF NOT EXISTS L2_files (   id              TEXT PRIMARY KEY,
                                        download_url    TEXT,
                                        location        TEXT,
                                        target          TEXT,
                                        file_status     INTEGER,
                                        priority        INTEGER,
                                        created_at      TEXT,
                                        verifier_bit    INTEGER DEFAULT 0,
                                        FOREIGN KEY (target) REFERENCES L3m_files(id),
                                        UNIQUE(id)
                                        );
"""

# selection queries
count_files = """   SELECT count()
                        FROM {0}
                        WHERE id='{1}'"""
count_unprocessed_files = """   SELECT count()
                                    FROM L2_files
                                    WHERE file_status=1"""
count_existing = """SELECT count()
                        FROM {0}
                        WHERE id='{1}'
                            AND file_status>0
                    """
select_existing = """   SELECT id
                            FROM {0}
                            WHERE file_status>0"""
select_ready_for_download = """ SELECT id, download_url
                                    FROM L2_files
                                    WHERE file_status=0
                                    ORDER BY priority ASC
                                    LIMIT {0}"""
select_ready_for_processing = """ SELECT id, file_status, target
                                    FROM L2_files
                                    ORDER BY priority ASC, target ASC
                                    """ 
select_unverified_existing = """ SELECT id
                                    FROM {0}
                                    WHERE verifier_bit=0
                                        AND file_status>0 """
# get queries
get_file_location = """ SELECT location
                            FROM {0}
                            WHERE id='{1}'"""

get_file_status = """ SELECT file_status
                           FROM {0}
                           WHERE id='{1}' """

# insertion queries
insert_L2_processed = """   INSERT
                                INTO L2_files ({0})
                                VALUES ({1})"""
insert_L2_unprocessed = """ INSERT OR IGNORE
                                INTO L3m_files ({2})
                                VALUES ({3});
                            INSERT OR IGNORE
                                INTO L2_files ({0})
                                VALUES ({1});"""
insert_L3m = """INSERT or REPLACE
                    INTO L3m_files ({0})
                    VALUES ({1});"""

update_L3m = """ UPDATE L3m_files
                   SET file_status = 1, location = '{0}'
                   WHERE id = '{1}'
                             """

# updating queries
file_downloaded = """   UPDATE L2_files
                            SET location='{1}', file_status=1, created_at='{2}'
                            WHERE id='{0}'"""
file_produced = """ UPDATE L3m_files
                        SET location='{1}', file_status=1, created_at='{2}'
                        WHERE id='{0}'"""
update_status = """ UPDATE {0}
                        SET file_status={2}
                        WHERE id='{1}'"""
update_priority = """ UPDATE {0}
                         SET priority={2}
                         WHERE id='{1}'"""
verify_file = """   UPDATE {0}
                        SET verifier_bit=1
                        WHERE id='{1}'"""
reset_verifier = """UPDATE {0}
                        SET verifier_bit=0"""
# deleting files
delete_L2file = """ DELETE FROM L2_files WHERE id='{0}'"""
                       

# execute a given query, and return a value according to the return_type argument
def Execute(query, return_type = None):
    try:
        # preparation
        conn = sqlite3.connect(params.path_to_data + params.db_filename, isolation_level=None)
        cur = conn.cursor()
        cur.execute("BEGIN")
        # breaking query into commands
        commands = query.split(';')

        try:
            # execute all commands
            for com in commands:
                #print("Executing", com)
                cur.execute(com)

            # decide on return value
            if return_type is None:
                retval = None
            elif return_type == "scalar":
                retval = cur.fetchone()[0]
            else:
                retval = cur.fetchall()

            cur.execute("COMMIT")

        except conn.Error as error:
            print("Error while processing transaction:", com, error)
            cur.execute("ROLLBACK")

        conn.close()

    except sqlite3.Error as error:
        print("Error while connecting to database:", error)
        conn.close()
        exit("Program terminated.")

    return retval

# checks if an entry exists in the specified table
def Exists(table, entry):
    return bool(Execute(count_files.format(table, entry), "scalar"))

# checks if an entry exists in a table with status > 0
def ExistsOnDisk(table, entry):
    return bool(Execute(count_existing.format(table, entry), "scalar"))

# converts a filename into a dictionary containing all fields
def FilenameToDict(filename, location):
    d = {"id": filename,
         "location": location
         }

    p = util.GetFileProperties(filename)
    if p["level"] == "L2":
        d["target"] = util.ProduceL3mFilename(filename)
        
    return d

# formats an entry (given in the form of a dictionary) into a fields-values pair
def FormatEntry(entry):
    formatted_entry = dict()
    for item in entry.items():
        if isinstance(item[1], str):
            formatted_entry[item[0]] = "'"+item[1]+"'"
        elif isinstance(item[1], int):
            formatted_entry[item[0]] = str(item[1])

    return ','.join(formatted_entry.keys()), ','.join(formatted_entry.values())
  
# updates L3m file location and status
def UpdateL3m(location, filename):
    Execute(update_L3m.format(location, filename))

# insert a L3m file into the database
def InsertL3m(entry):
    query = insert_L3m.format(*FormatEntry(entry))
    Execute(query)
    
# insert an existing L2 file into the database
def InsertL2(entry):
    # check if a target L3m entry exists

    # if so, insert the L2 entry with file_status=2 ('processed')
    if ExistsOnDisk("L3m_files", entry["target"]):
        entry["file_status"] = 2
        Execute(insert_L2_processed.format(*FormatEntry(entry)))

    # otherwise, insert the L2 entry with file_status=1 ('unprocessed') and insert a L3m entry with file_status=0 ('missing')
    else:
        entry["file_status"] = 1
        L3m_entry = {"id":entry["target"], "file_status":0}
        Execute(insert_L2_unprocessed.format(*FormatEntry(entry), *FormatEntry(L3m_entry)))
      
# insert files from a certain type and from a certain folder into the DB
def InsertFiles(path, filetype):
    filelist = os.listdir(path)
    for item in filelist:

        # if the item is a directory, go through it recursively
        if os.path.isdir(path+item):
            InsertFiles(path+item+'/', filetype)

        # else, try inserting the file
        else:
            db_entry = FilenameToDict(item, path)

            if filetype == "L2":
                db_entry["priority"] = 4 # the default priority for L2 files that already exist
                InsertL2(db_entry)
            else:
                db_entry["file_status"] = 1
                InsertL3m(db_entry)

# get all existing files from a table
def GetExisting(table):
    return [item[0] for item in Execute(select_existing.format(table), "list")]

# get <limit> files that are ready to be downloaded
def GetReadyForDownload(limit):
    return Execute(select_ready_for_download.format(limit), "list")

# gets filestatus  
def GetFileStatus(table, filename):
    return Execute(get_file_status.format(table, filename))
  
# gets all the unverified files 
def GetUnverifiedExisting(table):
    return Execute(select_unverified_existing.format(table))
  
# queue up a L2 file to be downloaded
def QueueFile(entry):
    entry["file_status"] = 0
    L3m_entry = {"id":entry["target"], "file_status":0}
    Execute(insert_L2_unprocessed.format(*FormatEntry(entry), *FormatEntry(L3m_entry)))

# update the entry concerning the specified L2 file, when it has been downloaded
def FileDownloaded(filename, location):
    Execute(file_downloaded.format(filename, location, datetime.now().strftime("%Y-%m-%d %H:%M")))

# update the entry concerning the specified L3m file, when it has been produced
def FileProduced(filename, location):
    Execute(file_produced.format(filename, location, datetime.now().strftime("%Y-%m-%d %H:%M")))

# update the given file's status in the given table
def UpdateStatus(table, filename, status):
    Execute(update_status.format(table, filename, status))

# update the give file's priority in the given table    
def UpdatePriority(table, filename, priority):
    Execute(update_priority.format(table, filename, priority))    
    
# set verifier_bit to 1
def VerifyFile(table, filename):
    Execute(verify_file.format(table, filename))

# set all files's verifier_bit to 0
def ResetVerifier(table):
    Execute(reset_verifier.format(table))

# get all files that fullfill verifier_bit=0 AND file_status>0:
def GetUnverifiedExisting(table):
    return [item[0] for item in Execute(select_unverified_existing.format(table), "list")]

# get all files that are ready for processing
def GetFilesReadyForProcessing():
    return Execute(select_ready_for_processing, "list")

def GetFileLocation(table, filename):
    return Execute(get_file_location.format(table, filename), "scalar")

# returns false if and only if the database doesn't contain any L2 files with status 1
def ThereAreUnprocessedFiles():
    return bool(Execute(count_unprocessed_files, "scalar"))
  
def DeleteSpecificFile(filename):
    return Execute(delete_L2file.format(filename))

if __name__ == "__main__":
    # if run as its own script, this produces the File Management Database

    # create file and tables
    Execute(create_tables)

    # cycle through all files in the data directory recursively and insert them into the DB (if they aren't there already)
    # this ordering is important, as inserting L2 files will check whether L3m files are in the database, and not on the disk itself.
    print("Inserting L3m files...", end=' ', flush=True)
    InsertFiles(params.path_to_data+"L3m/", "L3m")
    print("Done.")
    print("Inserting L2 files...", end=' ', flush=True)
    InsertFiles(params.path_to_data+"L2/", "L2")
    print("Done.")
