"""
Sattelite Data Download Queuing Script
Created by Lun Surdyaev on 2021-12-03
Last Updated on 2021-12-14
Maintained by Lun Surdyaev lunvang@gmail.com

Description:
This script looks up and stores URLs of L2 files to be downloaded.

How-to-Use:
The script will ask for the following information:
1. A list of missions
2. A time interval, in date form
3. A priority tag (1-5)
It will then generate and store a list of L2 files to be downloaded in the File Management Database.

NOTE: This script doesn't actually download any data.
That is done by the downloader script.
This script solely provides the URLs for the downloader script to use.

NOTE 2: This script can be run during downloading/processing of data, to add more files to the download queue.
"""

from datetime import datetime, timedelta

import params
import _util as util
import _sqlhandler as sql
import _webhandler as web

class Interval:
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def split(self, date):
        if date > self.end or date < self.start:
            return [Interval(self.start, self.end)]
        elif date == self.start and date == self.end:
            return []
        elif date == self.start:
            return [Interval(self.start + timedelta(1), self.end)]
        elif date == self.end:
            return [Interval(self.start, self.end + timedelta(1))]
        else:
            return [Interval(self.start, date-timedelta(1)), Interval(date+timedelta(1), self.end)]

    def __eq__(self, other):
        return self.start == other.start and self.end == other.end

    # a string representation, for search API
    def __str__(self):
        return self.start.strftime("%Y-%m-%d") + "," + (self.end + timedelta(1)).strftime("%Y-%m-%d")

# converts a YYYYDDDHHMMSS date format into a YYYYMMDDTHHMMSS time format
def GenerateTimestamp(ts):
    return datetime.strptime(ts, "%Y%j%H%M%S").strftime("%Y%m%dT%H%M%S")

# generate a filename in the new OB.DAAC file naming convention
def GenFilename(filename):
    # split filename into components
    components = filename.split('.')
    
    # if the file is not already in the new convention, update it
    if components[1][0] == 'L':
        data_type = components[1].split('_')
        
        # special case: the file is a VIIRS file: have to make use of data_type
        if components[0][0] == 'V':
            return '.'.join([
                data_type[1] + '_' + util.ID_TO_NAME[components[0][0]],
                GenerateTimestamp(components[0][1:]),
                data_type[0],
                data_type[-1],
                'nc'
                ])
        # Aqua, Terra, SeaWifs
        else:
            return '.'.join([
                util.ID_TO_NAME[components[0][0]],
                GenerateTimestamp(components[0][1:]),
                data_type[0],
                data_type[-1],
                'nc'
                ])
        
    # else, return it
    return filename

# prompts the user for data regarding the batch of files to be queued
def GetUserInput():
    # what sattelites do we want
    missions = input(util.mission_prompt)
    if missions == "":
        missions = params.default_missions
    missions = [util.ID_TO_NAME[id.upper()] for id in missions]

    # what dates
    try:
        start_date = datetime.strptime(
            input("Please put the start date from which you want the data, in the following format: YYYY-MM-DD.\nYour answer: "),
            "%Y-%m-%d")
        end_date   = datetime.strptime(
            input("Please put the last date from which you want the data, in the following format: YYYY-MM-DD.\nYour answer: "),
            "%Y-%m-%d")
        timespan = Interval(start_date, end_date)
    except:
        print("An invalid date was entered.")
        exit("Program terminated.")
        
    # priority
    priority = int(input("Please specify the priority for this batch of files (1 - Highest, 5 - Lowest).\nYour answer: "))
    if priority > 5 or priority < 1:
        print("Invalid priority.")
        exit("Program terminated.")

    return missions, timespan, priority

def main():

    missions, timespan, priority = GetUserInput()

    # check database for existing L3m data
    L3m_files = [util.GetFileProperties(file) for file in sql.GetExisting("L3m_files")]

    # bin data into missions
    dates_by_mission = {mission: [] for mission in missions}
    for file in L3m_files:
        if file["identifier"] in missions:
            # check if there's an overlap
            if file["date"] >= timespan.start and file["date"] <= timespan.end:
                dates_by_mission[file["identifier"]].append(file["date"])

    # fix overlap
    mission_to_requests = {mission:[] for mission in missions}
    for mission,dates in dates_by_mission.items():
        dates.sort()
        intervals = [timespan]
        for date in dates:
            i = intervals[-1]
            del intervals[-1]
            intervals += i.split(date)

        if len(intervals) == 0:
            print("For the mission", mission,
                  "no files will be downloaded, as all data in the provided timespan already exists in L3m format.")
        elif len(intervals) > 1:
            print("For the mission", mission,
                  """some data in the provided timespan already exists in L3m format. These dates have been excluded.
Because of that, instead of following the entire timespan, the following timespans will be downloaded:""")
            for i in intervals:
                print(i.start.strftime("%Y-%m-%d"), "to", i.end.strftime("%Y-%m-%d"))

        for i in intervals:
            for shortname in util.MISSION_TO_SHORTNAMES[mission]:
                mission_to_requests[mission].append((shortname, str(i)))

    # check number of expected files to be downloaded
    s = 0
    print("Counting number of files to be downloaded...")
    for mission, requests in mission_to_requests.items():
        print("Number of", mission, "files to be downloaded:", end=' ')
        n = sum([web.GetNumberOfFiles(*request) for request in requests])
        s += n
        print(n)

    # final green light
    if input("Do you wanna queue " + str(s) + " files to be downloaded? [Y/n] ").lower() != 'y':
        exit("Program terminated")

    # fetch filenames
    filenames = []
    for mission, requests in mission_to_requests.items():
        print("Gathering", mission, "file download URLs...", end=' ', flush=True)
        for request in requests:
            filenames += web.GetDownloadURLs(*request)
        print("Gathered.")

    # put filenames in DB
    print("Inserting download URLs into database...", end=' ', flush=True)
    for filename in filenames:
        # fix name
        name = GenFilename(filename.split('/')[-1])

        # if the file isn't in the timespan, don't queue it up
        date = util.GetFileProperties(name)["date"]
        if date > timespan.end or date < timespan.start:
            continue

        # if the file is in the database, don't queue it up and alert user.
        if sql.Exists("L2_files", name):
            print("The file", name, "is already present, either as a queued file, or on the disk. It won't be downloaded.")
            continue

        db_entry = {
            "id": name,
            "download_url": filename,
            "target": util.ProduceL3mFilename(name),
            "priority": priority
            }
        sql.QueueFile(db_entry)

    print("Done.")

if __name__ == "__main__":
    main()