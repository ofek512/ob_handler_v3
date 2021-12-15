"""
Priority Change
Created by Ofek Yankis on 2021-12-15
Last Updated on 2021-12-15
Maintained by Ofek Yankis ofek5202@gmail.com

Description:
This is a application for changing the priority of a lists of L2 files from specified time span chosen by the user. (KOLEL!!!)
"""

from datetime import datetime

import _util as util
import _sqlhandler as sql


def UserInput():
    try:
        start_date = datetime.strptime(
            input("Please put the start date from which you want the data, in the following format: YYYY-MM-DD.\nYour answer: "),
            "%Y-%m-%d")
        end_date   = datetime.strptime(
            input("Please put the last date from which you want the data, in the following format: YYYY-MM-DD.\nYour answer: "),
            "%Y-%m-%d")       
    except:
        print("An invalid date was entered.")
        exit("Program terminated.")



    priority = int(input("Please specify the priority for this batch of files (1 - Highest, 5 - Lowest).\nYour answer: "))
    if priority > 5 or priority < 1:
        print("Invalid priority.")
        exit("Program terminated.")


    return start_date, end_date, priority

def main():
    start_date, end_date, priority = UserInput()  
    
    all_files = sql.GetExisting("L2_files")

    for file in all_files:

        properties = util.GetFileProperties(file[0])
        if properties["date"] >= start_date and properties["date"] <= end_date:
            sql.UpdatePriority("L2_files",file ,priority)
    


if __name__ == "__main__":
    main()
