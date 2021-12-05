"""
General Utility file
Created by Lun Surdyaev on 2021-12-04
Last Updated on 2021-12-05
Maintained by Lun Surdyaev lunvang@gmail.com
"""

import os

# Returns OBPG file properties (mission, sensor, date, level and data type)
def GetFileProperties(filename_with_extension):

    filename, ext = os.path.splitext(filename_with_extension)

    components = filename.split('.')
    
    mission, sensor = components[0].split('_')
    date = components[1][:8]
    level = components[2]
    
    properties = {"mission":mission, 
        "sensor":sensor,
        "date":date,
        "level":level
    }
    
    # some properties are only relevant for L3 data
    if level == "L2":
        properties["type"] = components[3]
    else:
        properties["period"] = components[3]
        properties["type"] = components[4]
        properties["resolution"] = components[5]
    
    return properties
