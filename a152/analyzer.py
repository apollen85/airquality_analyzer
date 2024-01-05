# Note:
# The raw window data is unreasonable. I don't think it is the code.

from datetime import datetime
import numpy as np
import pandas as pd
# import matplotlib.pyplot as plt
from functools import reduce
# import analyzer as analyzer
from string import Template

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
globalStartTime = datetime(2023, 11, 27, 00, 00, 00)
globalStopTime = datetime(2023, 12, 22, 23, 59, 59)

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

class DeltaTemplate(Template):
    delimiter="%"


# Data from all sensors for a period of time
class DataChunk:
    
    dataSeries: pd.DataFrame # Holding all the data

    def __init__(self, startTime, endTime, inputData):
        self.startTime = startTime
        self.endTime = endTime

        # print(inputData)
        self.beginDoorState = self.calcDoorWindowStateBefore(inputData.filter(regex="Time|_Door"))
        self.beginWindowState = self.calcDoorWindowStateBefore(inputData.filter(regex="Time|_Window"))
        
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

    def strfdelta(self, deltaObj, format):
        d = {"D": deltaObj.days}
        hours, rem = divmod(deltaObj.seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        d["H"] = '{:02d}'.format(hours)
        d["M"] = '{:02d}'.format(minutes)
        d["S"] = '{:02d}'.format(seconds)
        t = DeltaTemplate(format)
        return t.substitute(**d)

    def filterData(self, startTime, endTime, data: pd.DataFrame):
        # Filtering out unwanted times from the data
        data = data[(data["Time"] >= startTime) & (data["Time"] <= endTime)]
        return data
    
    def calcDoorWindowStateBefore(self, data: pd.DataFrame):
        data = data.dropna()
        # print(data)
        # print("")
        # print("")
        # print("")
        # data = data.ffill()
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
        timeColumns = ["Start Time", "End Time", "Duration"]

        tempValues = self.calcValues("temperature", self.dataSeries)
        tempColumns = ["Temperature badlower", "Temperature good", "Temperature badupper"]

        co2Values = self.calcValues("co2", self.dataSeries)
        co2Columns = ["CO2 good", "CO2 warning", "CO2 bad"]

        eco2Values = self.calcValues("eco2", self.dataSeries)
        eco2Columns = ["eCO2 good", "eCO2 warning", "eCO2 bad"]

        eco2Diffs = self.calcEco2Diff(self.dataSeries.filter(regex="Time|_CO2"), self.dataSeries.filter(regex="Time|_eCO2"))
        eco2DiffColumns = ["eCO2Diff absolute", "eCO2Diff signed"]

        doorValues = self.calcValues("door", self.dataSeries.filter(regex="Time|_Door"))
        doorColumns = ["Door closings", "Door Openings", "Door closed", "Door open"]

        windowValues = self.calcValues("window", self.dataSeries.filter(regex="Time|_Window"))
        windowColumns = ["Window closings", "Window openings", "Window closed", "Window open"]

        self.duration = self.strfdelta(self.duration, "%H:%M")

        resultList = [self.startTime, self.endTime, self.duration] + tempValues + co2Values + eco2Values + eco2Diffs + doorValues + windowValues
        resultList = [resultList]
        resultFrame = pd.DataFrame(resultList, columns=timeColumns+tempColumns+co2Columns+eco2Columns+eco2DiffColumns+doorColumns+windowColumns)
        # resultFrame = pd.DataFrame(resultList, columns=["Start Time", "End Time", "Duration", "Temperature badlower", "Temperature good", "Temperature badupper", "CO2 good", "CO2 warning", "CO2 bad", "eCO2 good", "eCO2 warning", "eCO2 bad", "Door openings", "Door closings", "Door closed", "Door open", "Window closings", "Window openings", "Window closed", "Window open"])
        # print(resultFrame)
        return resultFrame
    
    def getValuesInRange(self, range: Range, data: pd.DataFrame):
        # print(data.filter(regex="^(?!.*Time).*$").squeeze())
        return data.filter(regex="^(?!.*Time).*$").squeeze().between(left=range.lower, right=range.upper, inclusive="neither").sum()
    
    def calcEco2Diff(self, co2Data: pd.DataFrame, eco2Data: pd.DataFrame):
        returndata = None
        # Merge  that are close enough
        co2Data = co2Data.dropna()
        eco2Data = eco2Data.dropna()
        co2Data.index = co2Data.iloc[:, 0]
        eco2Data.index = eco2Data.iloc[:, 0]
        tol = pd.Timedelta(seconds=5)
        combinedFrame = pd.merge_asof(left=co2Data, right=eco2Data, left_index=True, right_index=True, direction='nearest')
        # print(combinedFrame)

        # Calculate differences (signed and absolute)
        signedSum = 0
        absoluteSum = 0
        for i in range(len(combinedFrame)):
            diff = combinedFrame.iloc[i, 1]-combinedFrame.iloc[i, 3]
            signedSum += diff
            absoluteSum += abs(diff)

        # Average differences
        valueNum = combinedFrame.filter(regex="Time").squeeze().count()
        signedDiff = signedSum/valueNum
        absoluteDiff = absoluteSum/valueNum

        # Return differences
        returndata = [absoluteDiff, signedDiff]
        return returndata
    
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
            doorwin_closed = 1
            doorwin_open = 0
            data = data.dropna()
            noTimeData = data.filter(regex="^(?!.*Time).*$")
            openings = len(noTimeData == 1)
            closings = len(noTimeData == 0)
            timeOpen = pd.Timedelta(0)
            timeClosed = pd.Timedelta(0)
            returndata = None
            beginState = None
            state = None
            if quantity=="door":
                beginState = self.beginDoorState
                state = beginState
                
            else:
                beginState = self.beginWindowState
                state = beginState
            # print(data)

            if len(data) == 0:
                if state == doorwin_open:
                    timeOpen = self.duration
                else:
                    timeClosed = self.duration
                shareOpen = timeOpen/self.duration
                shareClosed = timeClosed/self.duration
                returndata = [closings, openings, shareClosed, shareOpen]
                # print(f"Returndata: {returndata}")
                # print("")
                # print("")
            else:
                # if quantity=="window":
                #     print(self.startTime)
                #     print(quantity)
                #     print(data)
                
                for i in range(len(data)):
                    # Convert times to timestamp (to enable time difference calculation)
                    data.iloc[i, 0] = pd.Timestamp(data.iloc[i, 0])

                    if i==0:
                        previousTime = self.startTime
                        state = beginState
                    else:
                        state = data.iloc[i-1, 1]
                        previousTime = data.iloc[i-1, 0]

                    timeDiff = data.iloc[i, 0]-previousTime

                    if state == doorwin_open:
                        timeOpen += timeDiff
                    else:
                        timeClosed += timeDiff

                    # if quantity=="window":
                    #     print(f"timeOpen: {timeOpen}")
                    
                
                # Special case for last iteration
                state = data.iloc[-1, 1]
                timeDiff = self.endTime-data.iloc[-1, 0]
                if state == doorwin_open:
                    timeOpen += timeDiff
                else:
                    timeClosed += timeDiff

                shareOpen = timeOpen/self.duration
                shareClosed = timeClosed/self.duration
                returndata = [closings, openings, shareClosed, shareOpen]
                # print(f"timeopen: {timeOpen}")
                # print(f"Returndata: {returndata}")
                # if quantity=="window":
                #     print("")
                #     print("")

                
        return returndata

class DataCollection:
    dataChunks = list()
    chunkNum = 0
    
    def __init__(self, timeFile, dataFiles):
        self.timeFile = timeFile
        self.dataFiles = dataFiles

    # Read input data from the files and split the data up into the chunks
    def fillChunks(self, invertTimes=False):
        self.readTimeLimits(self.timeFile)
        self.readDataFiles(self.dataFiles)

        # for index, row in self.timeLimits.iterrows():
        #     self.dataChunks.append(DataChunk(row["Start Time"], row["End Time"], self.dataSet))
        
        # for i in range(len(self.timeLimits["Start Time"])):
        #     self.dataChunks.append(DataChunk(self.timeLimits.iloc(i, 0), self.timeLimits.loc(i, "End Time"), self.dataSet))
            # Trim away already processed data? Optimization for later


        if invertTimes==True:
            self.dataChunks.append(DataChunk(globalStartTime, self.timeLimits.iloc[0, 0], self.dataSet))
            for i in range(len(self.timeLimits)-1):
                self.dataChunks.append(DataChunk(self.timeLimits.iloc[i, 1], self.timeLimits.iloc[i+1, 0], self.dataSet))
            self.dataChunks.append(DataChunk(self.timeLimits.iloc[len(self.timeLimits)-1, 1], globalStopTime, self.dataSet))
        else:
            # Fill the chunks with the correct start and end times
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

outsideLessons = DataCollection(timeFile, dataFiles)
outsideLessons.fillChunks(True)

    
finishedData = dataCollection.compileData()
print(finishedData)

outsideData = outsideLessons.compileData()
print(outsideData)

finishedData.to_csv("compiledData_"+classroom+".csv", index=False, sep=";", decimal=",")

# plt.figure()
# dataCollection.dataChunks[0].dataSeries.plot()
