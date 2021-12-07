"""
SQL Handling Utility
Created by Lun Surdyaev on 2021-12-04
Last Updated on 2021-12-06
Maintained by Lun Surdyaev lunvang@gmail.com

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
CREATE TABLE IF NOT EXISTS L3m_files (  id TEXT     PRIMARY KEY,
                                        location    TEXT,
                                        file_status INTEGER,
                                        created_at  TEXT,
                                        UNIQUE(id)
                                        );
CREATE TABLE IF NOT EXISTS L2_files (   id              TEXT PRIMARY KEY,
                                        download_url    TEXT,
                                        location        TEXT,
                                        target          TEXT,
                                        file_status     INTEGER,
                                        priority        INTEGER,
                                        created_at      TEXT,
                                        FOREIGN KEY (target) REFERENCES L3m_files(id),
                                        UNIQUE(id)
                                        );
"""

# selection queries
count_files = """   SELECT count()
                        FROM {0}
                        WHERE id='{1}'"""
select_existing = """   SELECT id
                            FROM {0}
                            WHERE file_status>0"""
select_ready_for_download = """ SELECT (id, download_url)
                                    FROM L2_files
                                    WHERE file_status=0
                                    ORDER BY priority ASC
                                    LIMIT {0}"""

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
insert_L3m = """INSERT
                    INTO L3m_files ({0})
                    VALUES ({1})"""

# updating queries
file_downloaded = """   UPDATE L2_files
                            SET location='{1}', file_status=1, created_at='{2}'
                            WHERE id='{0}'"""

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
                print("Executing", com)
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
            print("Error while processing transaction:", error)
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

# insert a L3m file into the database
def InsertL3m(entry):
    query = insert_L3m.format(*FormatEntry(entry))
    Execute(query)
    
# insert an existing L2 file into the database
def InsertL2(entry):
    # check if a target L3m entry exists

    # if so, insert the L2 entry with file_status=2 ('processed')
    if Exists("L3m_files", entry["target"]):
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

# queue up a L2 file to be downloaded
def QueueFile(entry):
    entry["file_status"] = 0
    L3m_entry = {"id":entry["target"], "file_status":0}
    Execute(insert_L2_unprocessed.format(*FormatEntry(entry), *FormatEntry(L3m_entry)))

# update the entry concerning the specified L2 file, when it has been downloaded
def FileDownloaded(filename, location):
    Execute(file_downloaded.format(filename, location, datetime.now().strftime("%Y-%m-%d %H:%M")))

if __name__ == "__main__":
    # if run as its own script, this produces the File Management Database

    # create file and tables
    Execute(create_tables)

    # cycle through all files in the data directory recursively and insert them into the DB (if they aren't there already)
    # this ordering is important, as inserting L2 files will check whether L3m files are in the database, and not on the disk itself.
    InsertFiles(params.path_to_data+"L3m/", "L3m")
    InsertFiles(params.path_to_data+"L2/", "L2")
