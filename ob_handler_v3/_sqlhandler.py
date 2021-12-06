"""
SQL Handling Utility
Created by Lun Surdyaev on 2021-12-04
Last Updated on 2021-12-05
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

# queries
create_tables = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS L3m_files (  id TEXT     PRIMARY KEY,
                                        location    TEXT,
                                        file_exists INTEGER,
                                        created_at  TEXT,
                                        UNIQUE(id)
                                        );
CREATE TABLE IF NOT EXISTS L2_files (   id              TEXT PRIMARY KEY,
                                        download_url    TEXT,
                                        location        TEXT,
                                        target          TEXT,
                                        file_exists     INTEGER,
                                        created_at      TEXT,
                                        FOREIGN KEY (target) REFERENCES L3m_files(id),
                                        UNIQUE(id)
                                        );
"""
count_files = "SELECT count() FROM {0} WHERE id='{1}'"
select_existing = "SELECT id FROM {0} WHERE file_exists=1"

insert_L2 = """ INSERT OR IGNORE    INTO L3m_files ({2}) VALUES ({3});
                INSERT OR IGNORE    INTO L2_files  ({0}) VALUES ({1});"""
insert_L3m = "INSERT OR IGNORE INTO L3m_files ({0}) VALUES ({1})"

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
         "file_exists": 1,
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

def InsertL3m(entry):
    query = insert_L3m.format(*FormatEntry(entry))
    Execute(query)
    
def InsertL2(entry):
    # generate a L3m entry
    L3m_entry = {"id":entry["target"], "file_exists":0}
    query = insert_L2.format(*FormatEntry(entry), *FormatEntry(L3m_entry))
    Execute(query)

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
                InsertL2(db_entry)
            else:
                InsertL3m(db_entry)

def GetExisting(table):
    return [item[0] for item in Execute(select_existing.format(table), "list")]

if __name__ == "__main__":
    # if run as its own script, this produces the File Management Database

    # create file and tables
    Execute(create_tables)

    # cycle through all files in the data directory recursively and insert them into the DB (if they aren't there already)
    # this ordering is important, as inserting L2 files will check whether L3m files are in the database, and not on the disk itself.
    InsertFiles(params.path_to_data+"L3m/", "L3m")
    InsertFiles(params.path_to_data+"L2/", "L2")
