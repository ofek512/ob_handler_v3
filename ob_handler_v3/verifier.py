"""
File Management Database Verifier Script
Created by Lun Surdyaev on 2021-12-03
Last Updated on 2021-12-04
Maintained by Lun Surdyaev lunvang@gmail.com

Description:
The script will check for inconsistensies in the file management database, and if found, will display and automatically fix them.

How-to-Use:
Run this script while no other scripts relating to the files are running (queuer.py, downloader.py, processor.py)
The inconsistensies come in three types:
1. A missing file being labeled as existing.
2. An existing file being labeled as missing.
3. A processed file being labeled as unprocessed.
In addition, the script will scan for unnecessary files (low-level files being kept post-processing).
A prompt will be displayed to the user, inquiring whether to delete the unnecessary files or not.
"""

def main():
    pass

if __name__ == "__main__":
    main()