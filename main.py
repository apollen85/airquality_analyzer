# TODO:
# Read normal date and time from file, convert to unix timestamp
# What datatype is rows?
# Can you store rows of rows?
# Calculate statistics on a timegroup basis, and also store the total measured time (is read in the beginning)
# Calculate averaged (must be weighted average) statistics
# Export analyzed data to excel (csv) for nice diagrams? Maybe even export filtered csv data and do analysis in excel?




# importing csv module
import csv
from datetime import datetime

# File names
dataFile = "testdata.csv"
timeFile = "filtertimes.csv"

def strToDate(dateString):
    return datetime.strptime(dateString, '%Y-%m-%d %H:%M')

def timeStampToDate(dateStampString):
    return datetime.utcfromtimestamp(float(dateStampString)/1000)

def isInTimeSpan(startTime, endTime, timestamp):
    pass

def findTimeLimits(filename):
    with open(filename, "r") as csvfile:
        csvreader = csv.reader(csvfile)
        
        # Discard headers
        next(csvreader)

        timeLimits = list(list())

        # Gather time limits
        for row in csvreader:
            timeLimits.append([row[0], row[1]])

        return timeLimits
    
def getData(filename):
    with open(filename, "r") as csvfile:
        csvreader = csv.reader(csvfile)

        # Discard gibberish and headers
        next(csvreader)
        next(csvreader)

        data = list(list())

        # Get the data from the file
        for row in csvreader:
            data.append([row[0], row[1]])

        return data
        

class DataSeries:
    data = list(list())

    def add(self, timestampIn, dataIn):
        self.data.append([timestampIn, dataIn])

class DataChunk:
    startTime = list()
    endTime = list()
    temperatureData = DataSeries()
    rhData = DataSeries()
    co2Data = DataSeries()
    tvocData = DataSeries()
    eco2Data = DataSeries()
    doorData = DataSeries()
    windowData = DataSeries()
    
    def __init__(self, startTime, endTime, data):
        self.fill(startTime, endTime, data)
        # self.startTime = datetime.strptime("2023-01-01 00:00", '%Y-%m-%d %H:%M')
        # self.endTime = datetime.strptime("2023-01-02 00:00", '%Y-%m-%d %H:%M')

    def __str__(self):
        pass

    # Data should start with values directly
    def fill(self, startTime, endTime, data):
        self.startTime = startTime
        self.endTime = endTime

        for row in data:
            if row[0] >=startTime and row[0] <= endTime:
                self.co2Data.add(row[0], row[1])
            else:
                pass

    
                

# Get all time limits
timeLimits = findTimeLimits(timeFile)

# Convert time limit strings to datetime objects
for limit in timeLimits:
    limit[0] = strToDate(limit[0])
    limit[1] = strToDate(limit[1])
    # for time in limit:
    #     time = strToDate(time)

# Get all CO2 data
co2Data = getData(dataFile)

# Convert timestamps to datetime objects
for dataPoint in co2Data:
    dataPoint[0] = timeStampToDate(dataPoint[0])

co2DataChunks = list()

for limit in timeLimits:
    co2DataChunks.append(DataChunk(limit[0], limit[1], co2Data))
    

i=0
for datachunk in co2DataChunks:
    print("DataChunk:" + str(i))
    print(datachunk.co2Data.data)
    i+=1


# Goal here: have datachunks for all variables, separated by specified time values


# with open(timeFilter, "r") as csvfile:
#     csvreader = csv.reader(csvfile)
    
#     # Read header, because we already know it
#     fields = next(csvreader)

#     for row in csvreader:


#     dateString = next(csvreader)
#     dateString = dateString[0]
#     print("Original String: " + dateString)
#     print("Type: " + str(type(dateString)))
#     convertedDate = datetime.strptime(dateString, '%Y-%m-%d %H:%M')
#     print("Converted time: " + str(convertedDate))