"""
Sattelite Data Download Queuing Script
Created by Lun Surdyaev on 2021-12-03
Last Updated on 2021-12-05
Maintained by Lun Surdyaev lunvang@gmail.com

Description:
This script looks up and stores URLs to L2 files to be downloaded.

How-to-Use:
The script will ask for the following information:
1. A list of missions
2. A time interval, in date form
And will return a list of L2 files that match the criteria and are ready to be downloaded.
That list will then get stored in the File Management Database, to be accessed by the downloader script.

NOTE: This script doesn't actually download any data.
That is done by the downloader script.
This script solely provides the URLs for the downloader script to use.

NOTE 2: This script can be run during downloading/processing of data, to add more files to the download queue.
"""

from datetime import datetime, timedelta
from urllib.request import urlopen
import pprint
import json

import _util as util
import params
import _sqlhandler as sql
from math import ceil

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

def GetNumberOfFiles(shortname, timespan):
    request = f"https://cmr.earthdata.nasa.gov/search/granules.umm_json\
?short_name={shortname}\
&provider=OB_DAAC\
&temporal={timespan}"

    response = urlopen(request)
    search_results = json.loads(response.read())

    if not search_results["items"]:
        print("Unexpected error occured. No files were found.")
        pprint.pprint(search_results)
        exit("Program terminated")

    return search_results["hits"]

def GetDownloadURLs(shortname, timespan):

    n_pages = ceil(GetNumberOfFiles(shortname, timespan)/util.PAGE_SIZE)

    # page through the hits
    hits = []
    for i in range(1, n_pages+1):

        request = f"https://cmr.earthdata.nasa.gov/search/granules.umm_json\
?page_size={util.PAGE_SIZE}\
&page_num={i}\
&short_name={shortname}\
&provider=OB_DAAC\
&temporal={timespan}"
    
        response = urlopen(request)
        search_results = json.loads(response.read())
    
        if not search_results["items"]:
            print("Unexpected error occured. No items were found.")
            pprint.pprint(search_results)
            exit("Program terminated")

        hits += [item["umm"]["RelatedUrls"][0]["URL"] for item in search_results["items"]]

    return hits

def main():
    # what sattelites do we want
    missions = input(util.mission_prompt)
    if missions == "":
        missions = params.default_missions
    missions = [util.ID_TO_NAME[id.upper()] for id in missions]

    # what dates
    try:
        start_date = datetime.strptime(
            input("Please put the start date from which you want the data, in the following format: YYYY-MM-DD.\n"),
            "%Y-%m-%d")
        end_date   = datetime.strptime(
            input("Please put the last date from which you want the data, in the following format: YYYY-MM-DD.\n"),
            "%Y-%m-%d")
    except:
        print("An invalid date was entered.")
        exit("Program terminated.")

    # check database for existing L3m data
    L3m_files = [util.GetFileProperties(file) for file in sql.GetExisting("L3m_files")]

    # bin data into missions
    dates_by_mission = {mission: [] for mission in missions}
    for file in L3m_files:
        if file["identifier"] in missions:
            # check if there's an overlap
            if file["date"] >= start_date and file["date"] <= end_date:
                dates_by_mission[file["identifier"]].append(file["date"])

    #for item in dates_by_mission.items():
    #    print(item[0], [date.strftime("%Y-%m-%d") for date in dates_by_mission[item[0]]])

    # fix overlap
    mission_to_requests = {mission:[] for mission in missions}
    for mission,dates in dates_by_mission.items():
        dates.sort()
        intervals = [Interval(start_date, end_date)]
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
    sum = 0
    print("Counting number of files to be")
    for mission, requests in mission_to_requests:
        print("Number of", mission, "files to be downloaded:", end=' ')
        n = sum([GetNumberOfFiles(*request) for request in requests])
        sum += n
        print(n)

    # final green light
    if input("Do you wanna queue", sum, "files to be downloaded? [Y/n]").lower() != 'y':
        exit("Program terminated")

    # fetch filenames
    filenames = []
    for mission, requests in mission_to_requests:
        print("Gathering", mission, "file download URLs...", end=' ')
        for request in requests:
            filenames += GetDownloadURLs(*request)
        print("Gathered.")

    # put filenames in DB
    print("Inserting download URLs into database...", end=' ')
    for filename in filenames:
        db_entry = {
            "id": filename.split('/')[-1],
            "download_url": filename,
            "exists": 0,
            "target": util.ProduceL3mFilename(filename.split('/')[-1])
            }
        sql.InsertL2(db_entry)
    print("Done.")

if __name__ == "__main__":
    main()