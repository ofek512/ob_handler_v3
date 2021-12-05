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

from _util import *
import params
import _sqlhandler as sql

class Interval:
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def split(self, date):
        if date > self.end or date < self.start:
            return [Interval(self.start, self.end)]
        elif date == self.start:
            return [Interval(self.start + timedelta(1), self.end)]
        elif date == self.end:
            return [Interval(self.start, self.end + timedelta(1))]
        else:
            return [Interval(self.start, date-timedelta(1)), Interval(date+timedelta(1), self.end)]

def GetNumberOfFiles(requests):
    pass

def GetDownloadURLs(requests):
    pass

def main():
    # what sattelites do we want
    missions = input(mission_prompt)
    if missions == "":
        missions = params.default_missions
    missions = [ID_TO_NAME[id.upper()] for id in missions]

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
    L3m_files = [GetFileProperties(file) for file in sql.GetExisting("L3m_files", [start_date, end_date])]

    # bin data into missions
    dates_by_mission = {mission: [] for mission in missions}
    for file in L3m_files:
        if file["mission"] in missions:
            dates_by_mission[file["mission"]].append(file["date"])

    for item in dates_by_mission.items():
        print(item[0], [date.strftime("%Y-%m-%d") for date in dates_by_mission])

    # notify user if there is any overlap

    # check number of expected files to be downloaded
    # increment the end date by 1, for API querying
    end_date += timedelta(1)

    # final green light

    # fetch filenames

    # put filenames in DB

if __name__ == "__main__":
    main()