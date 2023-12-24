# Notes to future self:
# The times for switches seem to be way off. Investigate


from datetime import datetime
import numpy as np
import pandas as pd
# import matplotlib.pyplot as plt
from functools import reduce
# import analyzer as analyzer

# plt.close("all")

# input files for the data
# dataFile = "testdata.csv"
classroom = "a152"
tempFile = "temperature_"+classroom+".csv"
rhFile = "humidity_"+classroom+".csv"
co2File = "co2_"+classroom+".csv"
tvocFile = "tvoc_"+classroom+".csv"
eco2File = "eco2_"+classroom+".csv"
doorFile = "door_"+classroom+".csv"
windowfile = "window_"+classroom+".csv"

# List of all files for easier calculations and data handling
dataFiles = [tempFile, rhFile, co2File, tvocFile, eco2File, doorFile, windowfile]

# Input file for the time intervals
timeFile = "time_filter_"+classroom+".csv"

dateFormat = '%Y-%m-%d %H:%M'

# Measurement threshold constants -------------------

class Range():
    upper: float
    lower: float
    def __init__(self, lower: float, upper: float):
        self.lower = lower
        self.upper = upper

# Temperature
temp_badlower = Range(0, 20)
temp_good = Range(20, 24)
temp_badupper = Range(24, 50)

# Humidity
rh_badlower = Range(0, 0)
rh_good = Range(0, 0)
rh_badupper = Range(0, 0)

# CO2
co2_good = Range(0, 600)
co2_warn = Range(600, 1000)
co2_bad = Range(1000, 20000)

# TVOC
tvoc_good = Range(0, 0)
tvoc_warn = Range(0, 0)
tvoc_bad = Range(0, 0)

# eCO2
eco2_good = co2_good
eco2_warn = co2_warn
eco2_bad = co2_bad

# ---------------------------------------------------


# Data from all sensors for a period of time
class DataChunk:
    
    dataSeries: pd.DataFrame # Holding all the data

    def __init__(self, startTime, endTime, inputData):
        self.startTime = startTime
        self.endTime = endTime

        # print(inputData)
        self.beginDoorState = self.calcDoorWindowStateBefore(inputData.filter(regex="Time|_Door"))
        self. beginWindowState = self.calcDoorWindowStateBefore(inputData.filter(regex="Time|_Window"))
        
        self.dataSeries = self.filterData(self.startTime, self.endTime, inputData)
        self.duration = endTime-startTime
        # print(self.dataSeries)
        
        self.doorOpenings = 0
        self.doorClosings = 0
        self.doorShareOpen = 0
        self.doorShareClosed = 0

        self.windowOpenings = 0
        self.windowClosings = 0
        self.windowShareOpen = 0
        self.windowShareClosed = 0

    def filterData(self, startTime, endTime, data: pd.DataFrame):
        # Filtering out unwanted times from the data
        data = data[(data["Time"] >= startTime) & (data["Time"] <= endTime)]
        return data
    
    def calcDoorWindowStateBefore(self, data):
        data = data.dropna()
        # print("")
        # print(self.startTime)
        # print(data.tail(25))
        lastState = data.iloc[0, 1]
        for i in range(len(data)):
            if data.iloc[i, 0] <= self.startTime:
                lastState = data.iloc[i, 1]
            else:
                break
        # print(lastState)
        return lastState
    
    
    # Collect the relevant data from the chunk into an appropriate data format
    # Should return a pandas dataframe with the following information:
    # - Start time
    # - End time
    # - Duration
    # - All temperature shares
    # - All CO2 shares
    # - All eCO2 shares
    # - Other shares when available
    # - Number of times door/windows opened/closed
    # - Share of time doow/window open/closed
    def compileData(self):
        tempValues = self.calcValues("temperature", self.dataSeries)
        co2Values = self.calcValues("co2", self.dataSeries)
        eco2Values = self.calcValues("eco2", self.dataSeries)
        doorValues = self.calcValues("door", self.dataSeries.filter(regex="Time|_Door"))
        windowValues = self.calcValues("window", self.dataSeries.filter(regex="Time|_Window"))
        resultList = [self.startTime, self.endTime, self.duration] + tempValues + co2Values + eco2Values + doorValues + windowValues
        # resultList = [[element] for element in resultList]
        resultList = [resultList]
        resultFrame = pd.DataFrame(resultList, columns=["Start Time", "End Time", "Duration", "Temperature badlower", "Temperature good", "Temperature badupper", "CO2 good", "CO2 warning", "CO2 bad", "eCO2 good", "eCO2 warning", "eCO2 bad", "Door openings", "Door closings", "Door closed", "Door open", "Window closings", "Window openings", "Window closed", "Window open"])
        # print(resultFrame)
        return resultFrame
    
    def getValuesInRange(self, range: Range, data: pd.DataFrame):
        # print(data.filter(regex="^(?!.*Time).*$").squeeze())
        return data.filter(regex="^(?!.*Time).*$").squeeze().between(left=range.lower, right=range.upper, inclusive="neither").sum()
    
    # Note: Values may be slightly wrong. Should count time, but actually counts number of measurements. Should work as the measurements are taken at fixed timings.
    def calcValues(self, quantity: str, data: pd.DataFrame):
        returndata = None
        quantity = quantity.lower()
        if quantity == "temperature":
            data = data.filter(regex="Time|_Temperature")
            # print(data)
            valueNum = data.filter(regex="_Temperature").squeeze().count()
            # print(valueNum)
            shareBadLower = self.getValuesInRange(temp_badlower, data)/valueNum
            shareGood = self.getValuesInRange(temp_good, data)/valueNum
            shareBadUpper = self.getValuesInRange(temp_badupper, data)/valueNum
            # print(f"BadLower:{shareBadLower} Good: {shareGod} BadUpper: {shareBadUpper}")
            returndata = [shareBadLower, shareGood, shareBadUpper]

        elif quantity == "rh":
            data = data.filter(regex="Time|_rH")
            # print(data)
            valueNum = data.filter(regex="_rH").squeeze().count()
            # print(valueNum)
            shareBadLower = self.getValuesInRange(rh_badlower, data)/valueNum
            shareGood = self.getValuesInRange(rh_good, data)/valueNum
            shareBadUpper = self.getValuesInRange(rh_badupper, data)/valueNum
            # print(f"BadLower:{shareBadLower} Good: {shareGod} BadUpper: {shareBadUpper}")
            returndata = [shareBadLower, shareGood, shareBadUpper]

        elif quantity == "co2":
            data = data.filter(regex="Time|_CO2")
            # print(data)
            valueNum = data.filter(regex="_CO2").squeeze().count()
            # print(valueNum)
            shareGood = self.getValuesInRange(co2_good, data)/valueNum
            shareWarn = self.getValuesInRange(co2_warn, data)/valueNum
            shareBad = self.getValuesInRange(co2_bad, data)/valueNum
            # print(f"Good:{shareGood} Warn: {shareWarn} Bad: {shareBad}")
            returndata = [shareGood, shareWarn, shareBad]

        elif quantity == "tvoc":
            data = data.filter(regex="Time|_TVOC")
            # print(data)
            valueNum = data.filter(regex="_TVOC").squeeze().count()
            # print(valueNum)
            shareGood = self.getValuesInRange(tvoc_good, data)/valueNum
            shareWarn = self.getValuesInRange(tvoc_warn, data)/valueNum
            shareBad = self.getValuesInRange(tvoc_bad, data)/valueNum
            # print(f"Good:{shareGood} Warn: {shareWarn} Bad: {shareBad}")
            returndata = [shareGood, shareWarn, shareBad]

        elif quantity == "eco2":
            data = data.filter(regex="Time|_eCO2")
            # print(data)
            valueNum = data.filter(regex="_eCO2").squeeze().count()
            # print(valueNum)
            shareGood = self.getValuesInRange(eco2_good, data)/valueNum
            shareWarn = self.getValuesInRange(eco2_warn, data)/valueNum
            shareBad = self.getValuesInRange(eco2_bad, data)/valueNum
            # print(f"Good:{shareGood} Warn: {shareWarn} Bad: {shareBad}")
            returndata = [shareGood, shareWarn, shareBad]
        elif quantity == "door" or quantity == "window":
            data = data.dropna()
            noTimeData = data.filter(regex="^(?!.*Time).*$")
            openings = len(noTimeData == 1)
            closings = len(noTimeData == 0)
            timeOpen = pd.Timedelta(0)
            timeClosed = pd.Timedelta(0)
            returndata = None
            # print(data)

            if len(data) == 0:
                if self.beginDoorState == 1:
                    timeOpen = self.duration
                    timeClosed = self.duration - self.duration
                else:
                    timeClosed = self.duration
                    timeOpen = self.duration -self.duration
                shareOpen = timeOpen/self.duration
                shareClosed = timeClosed/self.duration
                returndata = [closings, openings, shareClosed, shareOpen]
                print(f"Returndata: {returndata}")
                print("")
                print("")
            else:

                doorState = self.beginDoorState
                for i in range(len(data)):
                    data.iloc[i, 1] = pd.Timestamp(data.iloc[i, 1])
                    if i==0:
                        previousTime = self.startTime
                        doorState = self.beginDoorState
                    else:
                        doorState = data.iloc[i, 1]
                        previousTime = data.iloc[i-1, 0]
                    timeDiff = data.iloc[i, 0]-previousTime
                    if doorState == 1:
                        timeOpen += timeDiff
                    elif doorState == 0:
                        timeClosed += timeDiff
                    print(f"timeOpen: {timeOpen}")
                    
                
                if len(data) == 0:
                    doorState = data.iloc[0, 1]
                else:
                    doorState = data.iloc[len(data)-1, 1]
                
                timeDiff = self.endTime-data.iloc[len(data)-1, 0]
                if doorState == 1:
                    timeOpen += timeDiff
                else:
                    timeClosed += timeDiff

                shareOpen = timeOpen/self.duration
                shareClosed = timeClosed/self.duration
                returndata = [closings, openings, shareClosed, shareOpen]
                print(f"timeopen: {timeOpen}")
                print(f"Returndata: {returndata}")
                print("")
                print("")

                
        return returndata

class DataCollection:
    dataChunks = list()
    chunkNum = 0
    
    def __init__(self, timeFile, dataFiles):
        self.timeFile = timeFile
        self.dataFiles = dataFiles

    # Read input data from the files and split the data up into the chunks
    def fillChunks(self):
        self.readTimeLimits(self.timeFile)
        self.readDataFiles(self.dataFiles)

        # for index, row in self.timeLimits.iterrows():
        #     self.dataChunks.append(DataChunk(row["Start Time"], row["End Time"], self.dataSet))
        
        # for i in range(len(self.timeLimits["Start Time"])):
        #     self.dataChunks.append(DataChunk(self.timeLimits.iloc(i, 0), self.timeLimits.loc(i, "End Time"), self.dataSet))
            # Trim away already processed data? Optimization for later


        for ind in self.timeLimits.index:
            self.dataChunks.append(DataChunk(self.timeLimits["Start Time"][ind], self.timeLimits["End Time"][ind], self.dataSet))

        # for chunk in self.dataChunks:
        #     print(chunk.dataSeries)

    # Reads a time file into a list of time limits
    def readTimeLimits(self, timeFile):
        inputFile = pd.read_csv(timeFile, sep=None, engine="python") # Automatically determine separator
        
        chunkNum = inputFile["Start Time"].count()
        print(f"Processing {chunkNum} datachunks")
        
        inputFile["Start Time"] = pd.to_datetime(inputFile["Start Time"], format=dateFormat)
        # inputFile["Start Time"] = inputFile["Start Time"].dt.tz_localize("Europe/Stockholm")
        inputFile["End Time"] = pd.to_datetime(inputFile["End Time"], format=dateFormat)
        # inputFile["End Time"] = inputFile["End Time"].dt.tz_localize("Europe/Stockholm")
        # print(inputFile)
        # print(inputFile.dtypes)
        self.timeLimits = inputFile
        # print(self.timeLimits)

        # Time limits now exist in a DataFrame object, converted to dateTime objects


    # Reads data from a file into a list of timestamps and the data
    def readDataFiles(self, dataFiles):
        pandaSets = list()
        for file in dataFiles:
            if file == "":
                pandaSets.append(None)
            else:
                inputFile = pd.read_csv(file, sep=",", engine="python")
                inputFile["Time"] = pd.to_datetime(inputFile["Time"], unit="ms")
                inputFile["Time"] = inputFile["Time"].dt.tz_localize("UTC")
                inputFile["Time"] = inputFile["Time"].dt.tz_convert("Europe/Stockholm")
                inputFile["Time"] = inputFile["Time"].dt.tz_localize(None)
                pandaSets.append(inputFile)
                # print(inputFile.dtypes)
        
        mergedSet = reduce(lambda set1,set2: pd.merge(set1,set2,how="outer",on="Time"), pandaSets)
        mergedSet = mergedSet.sort_values(by=["Time"])
        # print(mergedSet)
        self.dataSet = mergedSet

    # Returns compiled data from all datachunks in one DataFrame
    def compileData(self):
        frames = list()
        for chunk in self.dataChunks:
            frames.append(chunk.compileData())

        resultFrame = pd.concat(frames, ignore_index=True)
        return resultFrame

        # Datasets should be merged and sorted by now.


# END OF DATACOLLECTION



dataCollection = DataCollection(timeFile, dataFiles)
dataCollection.fillChunks()

    
finishedData = dataCollection.compileData()
print(finishedData)

finishedData.to_csv("compiledData_"+classroom+".csv", index=False, sep=";", decimal=",")

# plt.figure()
# dataCollection.dataChunks[0].dataSeries.plot()