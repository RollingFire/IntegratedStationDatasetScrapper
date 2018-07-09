'''
By Austin Dorsey
Started: 5/22/18
Last Modified: 6/15/18
Discription: Handles and manages the Intedrated Station Database Stataions.
             Provides download, reading, and distilling functions for the data.
Todo: Add support for no-manditory fields.
      Read a file containing data from maultiple stations.
      If a File is not fully downloaded on exit, it is removed.
'''

import time
import os
import statistics
import queue
import threading
from threading import Thread
from collections import defaultdict
from matplotlib import pyplot as plt
import HTTP_Functions
from IntegratedSurfaceDatabaseStationsStations import customStrToFloat


def downloadStationYear(station, year):
    '''Downloads the station's data that is between tthe start and end year.'''
    host = "https://www.ncei.noaa.gov/data/global-hourly/access/"
    if year < int(station["BEGIN"][:4]) or year > int(station["END"][:4]):
        print("Station wasn't around during", year)
    destination = os.path.join(os.path.abspath("./IntegrationSerfaceDataStationsFiles/"), str(year))
    try:
        os.makedirs(destination)
    except:
        pass
    filename = getStationFilename(station, year)
    if year != time.localtime(time.time())[0] and os.path.exists(os.path.join(destination, filename)):
        return
    else:
        try:
            HTTP_Functions.downloadFile(host + str(year), filename, destination)
        except Exception as err:
            print(err)


class DownloadWorker(Thread):
    '''Class for threads of workers that download the station files.
    Gets the station and year for the queue and used downloadStationYear
    to download the file.
    '''
    def __init__(self, queue, outQueue):
        Thread.__init__(self)
        self.queue = queue
        self.outQueue = outQueue

    def run(self):
        while True:
            try:
                station, year = self.queue.get_nowait()
                downloadStationYear(station, year)
                self.outQueue.put((station, year))
                self.queue.task_done()
            except queue.Empty:
                break



def breakdownByDay(timeKey):
    '''Converts and returns timeKey to "YYYY-MM-DD" for use as a distilled timeKey.'''
    day = convertTime(timeKey)[0]
    return str(day[0] + "-" + day[1] + "-" + day[2])


def breakdownByMonth(timeKey):
    '''Converts and returns timeKey to "YYYY-MM" for use as a distilled timeKey.'''
    month = convertTime(timeKey)[0][:2]
    return str(month[0] + "-" + month[1])


def getData(stations, fields, breakdownFn=breakdownByDay, distillFns={},
            startYear=1901, endYear=time.localtime(time.time())[0], numWorkers=3):
    '''Calls all the needed functions to get the requested data, for the 
    requested stations.
    '''
    #Downloads the station's files.
    downloadQueue = queue.Queue()
    correctQueue = queue.Queue()
    for station in stations:
        begin = int(correctField("BEGIN", station["BEGIN"][:4]))
        end = int(correctField("END", station["END"][:4]))
        for year in range(startYear, endYear + 1):
            if year < begin or year > end:
                continue
            downloadQueue.put((station, year))

    workers = []
    for x in range(numWorkers):
        worker = DownloadWorker(downloadQueue, correctQueue)
        worker.daemon = True
        worker.start()
        workers.append(worker)

    correcters = []
    for x in range(numWorkers * 2):
        worker = CorrectWorker(correctQueue, downloadQueue)
        worker.daemon = True
        worker.start()
        correcters.append(worker)

    downloadQueue.join()
    correctQueue.join()
    
    #Reads the files and returns the data.
    data = {}
    for station in stations:
        for year in range(startYear, endYear + 1):
            if year < int(station["BEGIN"][:4]) or year > int(station["END"][:4]):
                continue
            stationData = readStationFiles(station, fields, startYear, endYear)
            if stationData == None:
                continue
            number = station["USAF"] + station["WBAN"]
            data[number] = distillData(stationData, breakdownFn, distillFns)

    return data


def readStationFiles(station, fields=None, startYear=1901, endYear=time.localtime(time.time())[0]):
    '''Returns the fields of the given station between the start and end years.'''
    data = {}
    keys = ("STATION", "DATE", "SOURCE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "REPORT_TYPE",
                  "CALL_SIGN", "QUALITY_CONTROL", "WND", "CIG", "VIS", "TMP", "DEW", "SLP")
    subKeys = {"WND": ("DIRECTION", "DIRECTION_QUALITY_CODE", "TYPE_CODE", "SPEED", "SPEED_QUALITY_CODE"),
               "CIG": ("CEILING_HEIGHT", "CEILING_HEIGHT_QUALITY_CODE", "CEILING_DETERMINATION_CODE", "CAVOK_CODE"),
               "VIS": ("DISTANCE", "DISTANCE_QUALITY_CODE", "VARIABILITY_CODE", "VARIABILITY_QUALITY_CODE"),
               "TMP": ("AIR_TEMPERATURE", "AIR_TEMPERATURE_QUALITY_CODE"),
               "DEW": ("DEW_POINT_TEMPERATURE", "DEW_POINT_TEMPERATURE_QUALITY_CODE"),
               "SLP": ("SEA_LEVEL_PRESSURE", "SEA_LEVEL_PRESSURE_QUALITY_CODE")}

    startYear = max(startYear, 1901)
    endYear = min(endYear, time.localtime(time.time())[0])

    begin = int(correctField("BEGIN", station["BEGIN"][:4]))
    end = int(correctField("END", station["END"][:4]))
    for year in range(startYear, endYear + 1):
        if year < begin or year > end:
            continue
        path = os.path.join(os.path.abspath("./IntegrationSerfaceDataStationsFiles/"),
                            str(year), getStationFilename(station, year))
        try:
            with open(path, 'r') as file:
                lines = file.readlines()
                for line in lines[1:]:
                    line = line.replace('\n', '')
                    dictionary = defaultdict(str)
                    timeKey = ""
                    #Each column
                    for key, val in zip(keys, line.split('","')):
                        val = val.replace('"', '')
                        if key == "DATE":
                            timeKey = val
                        #If a wanted field
                        elif fields == None or key in fields:
                            if key == "NAME":
                                dictionary[key] = val
                                continue
                            if key in subKeys.keys():
                                subData = {}
                                try:
                                    subVals = val.split(',')
                                    for i, subKey in enumerate(subKeys[key]):
                                        try:
                                            subData[subKey] = subVals[i]
                                        except IndexError:
                                            subData[subKey] = ''
                                except KeyError:
                                    for subKey, subVal in enumerate(subVals):
                                        subData[subKey] = subVal
                                dictionary[key] = subData
                            else:
                                dictionary[key] = val
                    data[timeKey] = dictionary

        except FileNotFoundError as err:
            print(err)
    if not data:
        return None
    else:
        return data


def writeFile(data, dest):
    '''Saves the data into the file at dest. Data is in the same format as getData's output.
    Station{Time{Fields{}}}
    '''
    if data is None:
        print("No data to write.")
        return
    keys = ("STATION", "DATE", "SOURCE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "REPORT_TYPE",
            "CALL_SIGN", "QUALITY_CONTROL", "WND", "CIG", "VIS", "TMP", "DEW", "SLP")
    for _ in range(3):
        try:
            with open(dest, 'w') as file:
                file.write(str('"' + keys[0] + '"'))
                for key in keys[1:]:
                    file.write(str(',"' + key + '"'))
                file.write('\n')
                for station, stationData in data.items():
                    for timeKey, entry in stationData.items():
                        file.write(str('"' + station + '","' + timeKey + '"'))

                        for field in keys:
                            if field == "STATION" or field == "DATE":
                                continue
                            val = ""
                            try:
                                val = entry[field]
                            except KeyError:
                                file.write(',""')
                                continue
                            if isinstance(val, dict):
                                subList = [x for x in val.values()]
                                val = ""
                                val += str(subList[0])
                                for sub in subList[1:]:
                                    val += ',' + str(sub)
                            file.write(',"' + str(val) + '"')
                        file.write('\n')
            break
        except PermissionError:
            input(str("Does not have persmition to open file " + str(dest)
                      + " \nMake sure file is not open and press enter to try again."))


def correctFile(station, year):
    '''Checks and corrects a whole file when given the station and year for the file to correct.'''
    path = os.path.join(os.path.abspath("./IntegrationSerfaceDataStationsFiles/"),
                        str(year), getStationFilename(station, year))
    oldData = readStationFiles(station, startYear=year, endYear=year)
    if oldData is None:
        return
    newData = {}
    stationNumber = station["USAF"] + station["WBAN"]
    newData[stationNumber] = {}
    for timeKey, line in oldData.items():
        newData[stationNumber][timeKey] = correctLine(line)
    writeFile(newData, path)


class CorrectWorker(Thread):
    '''Class for threads of workers that download the station files.
    Gets the station and year for the queue and used downloadStationYear
    to download the file.
    '''
    def __init__(self, queue, whileQueue):
        Thread.__init__(self)
        self.queue = queue
        self.whileQueue = whileQueue

    def run(self):
        while self.whileQueue.unfinished_tasks != 0 or not self.queue.empty():
            try:
                station, year = self.queue.get_nowait()
                try:
                    correctFile(station, year)
                finally:
                    self.queue.task_done()
            except queue.Empty:
                time.sleep(5)


def correctLine(line):
    '''Takes a dictionary of values, and calls correctField for each.
    Returns the corrected line.
    '''
    corrected = {}
    for key, val in line.items():
        corrected[key] = correctField(key, val)
    return corrected
            

def correctField(field, data):
    '''Checks to see if the data is the proper format and matches the field. If it is, it
    returns the origenal. Else it prompts the user for an update and returns that.
    '''
    verifyTrue = lambda x: True
    #Test per field dictionary.
    tests = {"STATION": verifyTrue, "DATE": verifyDate, "SOURCE": verifyTrue,
             "LATITUDE": verifyNumber, "LONGITUDE": verifyNumber, "ELEVATION": verifyNumber,
             "NAME": verifyTrue, "REPORT_TYPE": verifyTrue, "CALL_SIGN": verifyTrue, 
             "QUALITY_CONTROL": verifyTrue, "BEGIN": verifyNumber, "END": verifyNumber,
             "WND": {"DIRECTION": verifyNumber, "DIRECTION_QUALITY_CODE": verifyTrue, "TYPE_CODE": verifyTrue,
                     "SPEED": verifyNumber, "SPEED_QUALITY_CODE": verifyTrue},
             "CIG": {"CEILING_HEIGHT": verifyNumber, "CEILING_HEIGHT_QUALITY_CODE": verifyTrue,
                     "CEILING_DETERMINATION_CODE": verifyTrue, "CAVOK_CODE": verifyTrue},
             "VIS": {"DISTANCE": verifyNumber, "DISTANCE_QUALITY_CODE": verifyTrue,
                     "VARIABILITY_CODE": verifyTrue, "VARIABILITY_QUALITY_CODE": verifyTrue},
             "TMP": {"AIR_TEMPERATURE": verifyNumber, "AIR_TEMPERATURE_QUALITY_CODE": verifyTrue},
             "DEW": {"DEW_POINT_TEMPERATURE": verifyNumber, "DEW_POINT_TEMPERATURE_QUALITY_CODE": verifyTrue},
             "SLP": {"SEA_LEVEL_PRESSURE": verifyNumber, "SEA_LEVEL_PRESSURE_QUALITY_CODE": verifyTrue}}

    #If it is a dictionaried field like WND, TMP, etc.
    if isinstance(data, dict):
        dictionary = {}
        for subKey, val in data.items():
            while True:
                try:
                    if not tests[field][subKey](val):
                        val = promptCorection(val, field, subKey)
                        continue
                    else:
                        dictionary[subKey] = val
                        break
                except KeyError:
                    val = promptCorection(val, field, subKey)
                    continue
        return dictionary
    #If it is a raw, non dictionaried value.
    else:
        while True:
            try:
                if not tests[field](data):
                    data = promptCorection(data, field)
                    continue
                return data
            except KeyError:
                data = promptCorection(data, field)


def verifyDate(date):
    '''Used convertTime to check if propper date. Returns true if is, else false.'''
    try:
        dateList = convertTime(date)
        if len(dateList[0]) == 3 and len(dateList[1]) == 3:
            return True
        return False
    except:
        return False


def verifyNumber(number):
    '''Used customStrToFloat to check if number. Returns true if is, else false.'''
    try:
        customStrToFloat(number)
        return True
    except:
        return False


promptLock = threading.Lock()
def promptCorection(old, field, subField=None):
    '''Presents the user with the old value for a field and asks for an updated one.'''
    with promptLock:
        if subField is None:
            print("Field", field, "was:", old)
        else:
            print("Field", field, subField, "was:", old)
        return input("New value:")


def distillData(data, breakdownFn=breakdownByDay, distillFns={}):
    '''Takes the data, and distills it down. The entries are distilled down to the time
    periods that the breakdownFn dictates. Data fields are distilled down to a single
    entry per time period using the given distillFn for that field. If no function is
    given for that for that field, statistics.mean() will be used. If the function can
    not be used on the data, the last entry for time period will be used for the field.
    '''
    sections = defaultdict(list)
    for timeKey, row in data.items():
        timeKey = correctField("DATE", timeKey)
        newTimeKey = breakdownFn(timeKey)
        sections[newTimeKey].append(row)
    
    distilled = {}
    for timeKey, section in sections.items():
        fields = defaultdict(list)
        for entry in section:
            for field, val in entry.items():
                if not all9s(val):
                    fields[field].append(val)

        distilled[timeKey] = {}
        for field, fieldData in fields.items():
            try:
                if isinstance(fieldData[0], dict):
                    distilled[timeKey][field] = commaDistill(fieldData, distillFns.get(field, {}))
                else:
                    distilled[timeKey][field] = distillFns.get(field, statistics.mean)([customStrToFloat(x) for x in fieldData])
            except ValueError:
                distilled[timeKey][field] = fieldData[-1]
    return distilled


def getStationFilename(station, year):
    '''Determins the proper file name for a station and returns that filename.'''
    if station["USAF"].isalpha():
        return str(station["USAF"] + "-" + station["WBAN"] + "-" + str(year) + ".csv")
    else:
        return str(station["USAF"] + station["WBAN"] + ".csv")


def all9s(a=""):
    '''Checks to see if the string is a number with only 9 which means that it is missing.'''
    if a is '9':
        return False
    for x in a:
        if x is not '9' and x is not '.' and x is not '-' and x is not '+':
            return False
    return True


def convertTime(timeKey):
    '''Converts YYYY-MM-DDTHH:MM:SS to [YYYY, MM, DD, HH, MM, SS]'''
    date, clock = timeKey.split('T')
    return [date.split('-'), clock.split(':')]


def commaDistill(list, distillFns={}):
    '''Takes care of fields who's data is a dictionary of subfields. Uses the
    provided subfield distill function on that subfield. If no function has
    been provided, the last element of that subfield is used.
    '''
    dictionary = {}
    for key in list[0].keys():
        try:
            dictionary[key] = distillFns.get(key, lambda x: x[-1])([customStrToFloat(x) for entry in list for testKey, x in entry.items() if testKey == key])
        except ValueError:
            dictionary[key] = [x for entry in list for testKey, x in entry.items() if testKey == key][-1]
        except:
            print([x for entry in list for testKey, x in entry.items() if testKey == key])
    return dictionary

def showData(stationData, field):
    graphData = []
    number = ""
    if isinstance(field, dict):
        for first, second in field.items():
            field = str(first + ' ' + second)
            for timeKey, data in stationData.items():
                graphData.append((timeKey, data.get(first, {}).get(second, 0)))
                number = str(data.get("STATION", ''))
            break
    else:
        graphData = [(timeKey, data.get(field, 0)) for timeKey, data in stationData.items()]
        for timeKey, data in stationData.items():
            number = str(data.get("STATION", ''))
            break

            
    plt.plot([date for date, _ in graphData], [val for _, val in graphData], color="green", marker="o", linestyle="solid")
    plt.title(str(field + " data for station " + number))
    plt.show()
