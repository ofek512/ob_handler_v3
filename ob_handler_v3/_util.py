"""
General Utility file
Created by Lun Surdyaev on 2021-12-04
Last Updated on 2021-12-05
Maintained by Lun Surdyaev lunvang@gmail.com
"""

import os
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
    "SNPP_VIIRS": ["VIIRSN_L2_OC", "VIIRSN_L2_SST", "VIIRSN_L2_SST3"]
    }

PAGE_SIZE = 200

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
        properties["type"] = components[3]
    else:
        properties["period"] = components[3]
        properties["type"] = components[4]
        properties["resolution"] = components[5]
    
    return properties

def ProduceL3mFilename(L2_filename):
    p = GetFileProperties(L2_filename)
    return f"{p['mission']}_{p['sensor']}.{p['date']}.L3m.DAY.{p['type']}.{params.resolution}.nc"