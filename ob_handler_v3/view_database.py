import params
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

    return start_date, end_date, 


def main():
    
    start_date, end_date = UserInput()  
    files = sql.GetExisting("L3m_files") 
    
    for f in files:
        properties = util.GetFileProperties(file)
        if properties["date"] >= start_date and properties["date"] <= end_date:
            # the logic is in here
            print("The file name:" f /n "The file status:" sql.GetFileStatus("L3m_files", f))
            print(/n)
            print(/n)
    
    
    

    
    
    
    
    
    
    
if __name__ == "__main__":
    main()
