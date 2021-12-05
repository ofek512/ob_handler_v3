"""
SQL Handling Utility
Created by Lun Surdyaev on 2021-12-04
Last Updated on 2021-12-05
Maintained by Lun Surdyaev lunvang@gmail.com

Description:
This is a utility for handling the SQL-based File Management Database.
It uses the constants defined in params.py to find the database, and provides an interface to it via its functions.
"""

# local params
import params
from _util import *

import os
import sqlite3

# queries
create_tables = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS L3m_files (  id TEXT     PRIMARY KEY,
                                        location    TEXT,
                                        file_exists INTEGER,
                                        created_at  TEXT
                                        );
CREATE TABLE IF NOT EXISTS L2_files (   id              TEXT PRIMARY KEY,
                                        download_url    TEXT,
                                        location        TEXT,
                                        target          TEXT,
                                        file_exists     INTEGER,
                                        created_at      TEXT,
                                        FOREIGN KEY (target) REFERENCES L3m_files(id)
                                        );
"""
count_files = "SELECT count() FROM {0} WHERE id='{1}'"
insert_file = "INSERT INTO {0} ({1}) VALUES ({2})"

# execute a given query, and return a value according to the return_type argument
def Execute(query, return_type = None):
    try:
        # execute query
        conn = sqlite3.connect(params.path_to_data + params.db_filename)
        cur = conn.cursor()

        queries = query.split(';')
        for q in queries:
            cur.execute(q)
            print("er")

        # decide on return value
        if return_type is None:
            retval = None
        elif return_type == "scalar":
            retval = cur.fetchone()[0]
        else:
            retval = cur.fetchall()

    except sqlite3.Error as error:
        print("Sqlite3 error:", error)
    finally:
        conn.close()

    return retval

# checks if an entry exists in the specified table
def Exists(table, entry):
    return bool(Execute(count_files.format(table, entry), "scalar"))

def Insert(table, entry):

    for item in entry.items():
        if isinstance(item[1], str):
            entry[item[0]] = "'"+item[1]+"'"
        elif isinstance(item[1], int):
            entry[item[0]] = str(item[1])


    query = insert_file.format(table, ','.join(entry.keys()), ','.join(entry.values()))
    Execute(query)

# converts a filename into a dictionary containing all fields
def FilenameToDict(filename, location):
    d = {"id": filename,
         "file_exists": 1,
         "location": location
         }

    p = GetFileProperties(filename)
    if p["level"] == "L2":
        d["target"] = f"{p['mission']}_{p['sensor']}.{p['date']}.L3m.{p['period']}.{p['type']}.{params.resolution}.nc"
        
    return d

def InsertFiles(path, filetype):
    filelist = os.listdir(path)
    for item in filelist:

        # if the item is a directory, go through it recursively
        if os.path.isdir(path+item):
            InsertFiles(path+item+'/', filetype)

        # else, if the file isn't in the DB, insert it
        elif not Exists(filetype+"_files", item):
            # insert file into DB
            db_entry = FilenameToDict(item, path)
            Insert(filetype+"_files", db_entry)

            # only for L2 files, check whether their target is in the database, and if not so, insert it.
            if filetype == "L2" and not Exists("L3m_files", db_entry["target"]):
                Insert("L3m_files", {"id":db_entry["target"], "location":None, "exists":0, "created_at":None})

if __name__ == "__main__":
    # if run as its own script, this produces the File Management Database

    # create file and tables
    Execute(create_tables)

    # cycle through all files in the data directory recursively and insert them into the DB (if they aren't there already)

    # this ordering is important, as inserting L2 files will check whether L3m files are in the database, and not on the disk itself.
    InsertFiles(params.path_to_data+"L3m/", "L3m")
    InsertFiles(params.path_to_data+"L2/", "L2")
