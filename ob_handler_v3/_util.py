"""
General Utility file
Created by Lun Surdyaev on 2021-12-04
Last Updated on 2022-01-01
Maintained by Ofek Yankis ofek5202@gmail.com
"""

import os
from os import listdir
from os.path import isfile, join

from datetime import datetime

import params

# print messages
mission_prompt = """Please type the identifiers of the missions you are interested in gathering data from.
i.e. If you only want AQUA_MODIS, TERRA_MODIS, and SNPP_VIIRS data, type 'atn'.
If you want data from all sattelites, press [Enter].
List of valid identifiers:
a - AQUA_MODIS
t - TERRA_MODIS
s - SeaStar_SeaWiFS
j - JPSS1_VIIRS
n - SNPP_VIIRS
Your answer: """

# sensor identifiers
ID_TO_NAME = {
    "A": "AQUA_MODIS",
    "T": "TERRA_MODIS",
    "V": "VIIRS",
    "S": "SeaStar_SeaWiFS",
    # more specific VIIRS identifiers
    "J": "JPSS1_VIIRS",
    "N": "SNPP_VIIRS"
    }

MISSION_TO_SHORTNAMES = {
    "SeaStar_SeaWiFS": ["SeaWiFS_L2_OC"],
    "AQUA_MODIS": ["MODISA_L2_OC", "MODISA_L2_SST"],
    "TERRA_MODIS": ["MODIST_L2_OC", "MODIST_L2_SST"],
    "JPSS1_VIIRS": ["VIIRSJ1_L2_OC"],
    "SNPP_VIIRS": ["VIIRSN_L2_OC", "VIIRSN_L2_SST"]
    }

TYPE_TO_PRODUCT = {
    "OC": "chlor_a",
    "SST": "sst"
    }

PAGE_SIZE = 800

# Returns OBPG file properties (mission, sensor, date, level and data type)
def GetFileProperties(filename_with_extension):

    filename, ext = os.path.splitext(filename_with_extension)

    components = filename.split('.')
    
    mission, sensor = components[0].split('_')
    date = components[1][:8]
    level = components[2]
    
    properties = {
        "mission": mission,
        "sensor": sensor,
        "identifier": mission+'_'+sensor,
        "date": datetime.strptime(date, "%Y%m%d"),
        "level": level
    }
    
    # some properties are only relevant for L3 data
    if level == "L2":
        properties["type"] = components[3] # usually OC or SST
    else:
        properties["period"] = components[3] # usually DAY
        properties["type"] = components[4] # usually OC or SST
        properties["resolution"] = components[5] # usually 1km
    
    return properties

def ProduceL3bFilename(L2_filename):
    p = GetFileProperties(L2_filename)
    p["date"] = p["date"].strftime("%Y%m%d")
    return f"{p['mission']}_{p['sensor']}.{p['date']}.L3b.DAY.{p['type']}.{params.resolution}.nc"

def ProduceL3mFilename(L2_filename):
    p = GetFileProperties(L2_filename)
    p["date"] = p["date"].strftime("%Y%m%d")
    return f"{p['mission']}_{p['sensor']}.{p['date']}.L3m.DAY.{p['type']}.{params.resolution}.nc"

# gets all file paths from the directory and sub directories 
def getListOfFiles(path):
    # create a list of file and sub directories 
    # names in the given directory 
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)
                
    return allFiles    


def GetExistingFilenamesAndPaths(path):
    allList = []
    locations = getListOfFiles(path)
    fileNames = [os.path.splitext(l)[0].split('/')[-1] for l in locations]
    allList.append(locations)
    allList.append(fileNames)
    return allList
    
    
                                                
                                                
                                             
