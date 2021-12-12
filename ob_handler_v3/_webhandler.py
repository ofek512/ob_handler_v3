"""
Web Handling Helper Script
Created by Lun Surdyaev on 2021-12-07
Last updated on 2021-12-07
Maintained by Lun Surdyaev lunvang@gmail.com

Description:
This file contains functions that deal with web accesses, from API calls to downloading data.
"""

# local imports
import _util as util

# external
from urllib.request import urlopen
from urllib.parse import urlparse
from math import ceil
import pprint
import json
import argparse
import os
import re
import sys
import subprocess
import logging
import requests
from requests.adapters import HTTPAdapter
from datetime import datetime
import time
import textwrap
from pathlib import Path

DEFAULT_CHUNK_SIZE = 131072

# requests session object used to keep connections around
obpgSession = None

# get the number of L2 files corresponding to the provided shortname and timespan
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

# get the download URLs of L2 files corresponding to the provided shortname and timespan
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
    
        if search_results["items"] is None:
            print("Unexpected error occured. No items were found.")
            pprint.pprint(search_results)
            exit("Program terminated")

        hits += [item["umm"]["RelatedUrls"][0]["URL"] for item in search_results["items"]]

    return hits

def getSession(verbose=0, ntries=5):
    global obpgSession

    if not obpgSession:
        # turn on debug statements for requests
        if verbose > 1:
            print("Session started")
            logging.basicConfig(level=logging.DEBUG)

        obpgSession = requests.Session()
        obpgSession.mount('https://', HTTPAdapter(max_retries=ntries))

    else:
        if verbose > 1:
            print("Reusing existing session")

    return obpgSession

def isRequestAuthFailure(req) :
    ctype = req.headers.get('Content-Type')
    if ctype and ctype.startswith('text/html'):
        if "<title>Earthdata Login</title>" in req.text:
            return True
    return False

def httpdl(server, request, localpath='.', outputfilename=None, ntries=5,
           uncompress=False, timeout=30., verbose=0, force_download=False,
           chunk_size=DEFAULT_CHUNK_SIZE):

    status = 0
    urlStr = 'https://' + server + request

    global obpgSession
    localpath = Path(localpath)
    getSession(verbose=verbose, ntries=ntries)

    modified_since = None
    headers = {}

    if not force_download:
        if outputfilename:
            ofile = localpath / outputfilename
            modified_since = get_file_time(ofile)
        else:
            rpath = Path(request.rstrip())
            if 'requested_files' in request:
                rpath = Path(request.rstrip().split('?')[0])
            ofile = localpath / rpath.name
            if re.search(r'(?<=\?)(\w+)', ofile.name):
                ofile = Path(ofile.name.split('?')[0])

            modified_since = get_file_time(ofile)

        if modified_since:
            headers = {"If-Modified-Since":modified_since.strftime("%a, %d %b %Y %H:%M:%S GMT")}

    with obpgSession.get(urlStr, stream=True, timeout=timeout, headers=headers) as req:

        if req.status_code != 200:
            status = req.status_code
        elif isRequestAuthFailure(req):
            status = 401
        else:
            if not Path.exists(localpath):
                os.umask(0o02)
                Path.mkdir(localpath, mode=0o2775)

            if not outputfilename:
                cd = req.headers.get('Content-Disposition')
                if cd:
                    outputfilename = re.findall("filename=(.+)", cd)[0]
                else:
                    outputfilename = urlStr.split('/')[-1]

            ofile = localpath / outputfilename

            # This is here just in case we didn't get a 304 when we should have...
            download = True
            if 'last-modified' in req.headers:
                remote_lmt = req.headers['last-modified']
                remote_ftime = datetime.strptime(remote_lmt, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=None)
                if modified_since and not force_download:
                    if (remote_ftime - modified_since).total_seconds() < 0:
                        download = False
                        if verbose:
                            print("Skipping download of %s" % outputfilename)

            if download:
                total_length = req.headers.get('content-length')
                length_downloaded = 0
                total_length = int(total_length)
                if verbose >0:
                    print("Downloading %s (%8.2f MBs)" % (outputfilename, total_length /1024/1024))

                with open(ofile, 'wb') as fd:

                    for chunk in req.iter_content(chunk_size=chunk_size):
                        if chunk: # filter out keep-alive new chunks
                            length_downloaded += len(chunk)
                            fd.write(chunk)
                            if verbose > 0:
                                percent_done = int(50 * length_downloaded / total_length)
                                sys.stdout.write("\r[%s%s]" % ('=' * percent_done, ' ' * (50-percent_done)))
                                sys.stdout.flush()

                if uncompress:
                    if ofile.suffix in {'.Z', '.gz', '.bz2'}:
                        if verbose:
                            print("\nUncompressing {}".format(ofile))
                        compressStatus = uncompressFile(ofile)
                        if compressStatus:
                            status = compressStatus
                else:
                    status = 0

                if verbose:
                    print("\n...Done")

    return status

def uncompressFile(compressed_file):
    """
    uncompress file
    compression methods:
        bzip2
        gzip
        UNIX compress
    """

    compProg = {".gz": "gunzip -f ", ".Z": "gunzip -f ", ".bz2": "bunzip2 -f "}
    exten = Path(compressed_file).suffix
    unzip = compProg[exten]
    p = subprocess.Popen(unzip + str(compressed_file.resolve()), shell=True)
    status = os.waitpid(p.pid, 0)[1]
    if status:
        print("Warning! Unable to decompress %s" % compressed_file)
        return status
    else:
        return 0

def get_file_time(localFile):
    ftime = None
    localFile = Path(localFile)
    if not Path.is_file(localFile):
        while localFile.suffix in {'.Z', '.gz', '.bz2'}:
            localFile = localFile.with_suffix('')

    if Path.is_file(localFile):
        ftime = datetime.fromtimestamp(localFile.stat().st_mtime)

    return ftime

def RetrieveURL(request, localpath, appkey):
    server = "oceandata.sci.gsfc.nasa.gov"
    parsedRequest = urlparse(request)
    netpath = parsedRequest.path

    if parsedRequest.netloc:
        server = parsedRequest.netloc
    else:
        if not re.match(".*getfile",netpath):
            netpath = '/ob/getfile/' + netpath

    joiner = '?'
    if (re.match(".*getfile",netpath)) and appkey:
        netpath = netpath + joiner +'appkey=' + appkey
        joiner = '&'

    if parsedRequest.query:
        netpath = netpath + joiner + parsedRequest.query

    return httpdl(server, netpath, localpath=localpath, uncompress=True, verbose=False, force_download=False)

# download the file specified by the download URL, and rename it to the specified name
def DownloadFile(download_url, download_location):

    # download file
    status = RetrieveURL(download_url, download_location, "6d5b459daa8cfab9462d3e893ee09e0e052cfe92")

    # check status
    if status:
        if status == 304:
            print(request, "is not newer than local copy, skipping download")
        else:
            print(f"Whoops... problem retrieving {request} (received status {status})")