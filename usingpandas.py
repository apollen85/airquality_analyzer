# Notes to future self:
# Need to check time zone things


from datetime import datetime
import numpy as np
import pandas as pd
# import matplotlib.pyplot as plt
from functools import reduce
# import analyzer as analyzer

# plt.close("all")

# input files for the data
# dataFile = "testdata.csv"
tempFile = "testdata.csv"
rhFile = "testdata1.csv"
co2File = "testdata2.csv"
tvocFile = "testdata3.csv"
eco2File = "testdata4.csv"
doorFile = "testdata5.csv"
windowfile = "testdata6.csv"

# List of all files for easier calculations and data handling
dataFiles = [tempFile, rhFile, co2File, tvocFile, eco2File, doorFile, windowfile]

# Input file for the time intervals
timeFile = "filtertimes.csv"

dateFormat = '%Y-%m-%d %H:%M'


# Data from all sensors for a period of time
class DataChunk:
    
    dataSeries: pd.DataFrame # Holding all the data

    def __init__(self, startTime, endTime, inputData):
        self.startTime = startTime
        self.endTime = endTime
        self.dataSeries = self.filterData(self.startTime, self.endTime, inputData)

    def filterData(self, startTime, endTime, data: pd.DataFrame):
        # Filtering out unwanted times from the data
        data = data[(data["Time"] >= startTime) & (data["Time"] <= endTime)]
        return data

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
        inputFile["End Time"] = pd.to_datetime(inputFile["End Time"], format=dateFormat)
        # print(inputFile)
        # print(inputFile.dtypes)
        self.timeLimits = inputFile

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
                pandaSets.append(inputFile)
                # print(inputFile.dtypes)
        
        mergedSet = reduce(lambda set1,set2: pd.merge(set1,set2,how="outer",on="Time"), pandaSets)
        mergedSet = mergedSet.sort_values(by=["Time"])
        # print(mergedSet)
        self.dataSet = mergedSet

        # Datasets should be merged and sorted by now.

def getValuesInRange(minval, maxval, data: pd.DataFrame):
    return data.filter(regex="_CO2").squeeze().between(left=minval, right=maxval).sum()

co2_good_lower = 0
co2_good_upper = 600
co2_warn_upper = 1000
co2_bad_upper = 20000

def clacCO2Values(data: pd.DataFrame):
    data = data.filter(regex="Time|_CO2")
    # print(data)
    valueNum = data.filter(regex="_CO2").squeeze().count()
    print(valueNum)
    shareGood = getValuesInRange(co2_good_lower, co2_good_upper, data)/valueNum
    shareWarn = getValuesInRange(co2_good_upper, co2_warn_upper, data)/valueNum
    shareBad = getValuesInRange(co2_warn_upper, co2_bad_upper, data)/valueNum
    print(f"Good:{shareGood} Warn: {shareWarn} Bad: {shareBad}")

dataCollection = DataCollection(timeFile, dataFiles)
dataCollection.fillChunks()

for chunk in dataCollection.dataChunks:
    clacCO2Values(chunk.dataSeries)
    

# plt.figure()
# dataCollection.dataChunks[0].dataSeries.plot()
