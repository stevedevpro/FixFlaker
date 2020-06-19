# pyFixFlaker - flaky fake FIX log generator
# Generates synthetic FIX logs
# bidirectional flow between client and broker
# NOT A FIX ENGINE OR SIMULATOR

import os
import sys
import platform
import datetime
import time
import random
#import urllib2
from urllib.request import urlopen



def getTimeStampString():
  return datetime.datetime.now().strftime(timeFormat)

#main
#reentrant
pid = str(os.getpid())
hostname = platform.node()[3:-13] #works on aws linux
print ("STARTING " + pid + " on " + hostname)

#configuration
fixLogPath = "/tmp/"
fixLogFileName = "fix.log"
timeFormat = "%Y%m%d-%H:%M:%S.%f"
fixDelimeter = "|"

#current state msg rate
engineDelay = .001
execDelay = .005
#10x msg rate
engineDelay = .0001
execDelay = .00025
#max msg rate
# engineDelay = 0
# execDelay = 0

#get stock list
stockfile = urlopen("https://datahub.io/core/s-and-p-500-companies/r/constituents.csv")

stock = []
for rec in stockfile:
  recstr = str(rec)
  stk = recstr[0:recstr.find(",")]
  if stk != "Symbol":
    stock.append(stk)
stocklen = len(stock)
print (str(stocklen) + " stocks loaded.")
#sys.exit()

fixLogFile = fixLogPath + fixLogFileName
try:
  fh = open(fixLogFile, "w")
except:
  print("problem opening file for writing. EXITING!")
  sys.exit()

fh.write("HEADER\n")

secList = ["AMZN", "GOOG", "IBM", "XOM"]

clientmsgseq = 0
brokermsgseq = 0
clordid = 0
brordid = 0
execid = 0
clienttarget = "CLIENT_" + pid + "_" + hostname
brokertarget = "BROKER_" + pid + "_" + hostname
fix8 = "8=FIX4.4" + fixDelimeter #version

msgCount = 0
totalBytes = 0
startSessionTime = datetime.datetime.now()

for sec in stock:
#for sec in secList:

  transactionTime = getTimeStampString()
  time.sleep(engineDelay)
  #transaction
  fix55 = "55=" + sec + fixDelimeter #security
  qty = random.randint(1,100) * 100
  fix38 = "38=" + str(qty) + fixDelimeter #quantity
  price = str(random.randint(5, 10000)) + "." + str(random.random())[3:5]
  fix44 = "44=" + str(price) + fixDelimeter #price
  side = random.randint(1,2)
  fix54 = "54=" + str(side) + fixDelimeter #side
  fix40 = "40=2" + fixDelimeter #order type (2:limit)

  #client order
  fix35 = "35=D" + fixDelimeter #msg type
  fix58 = "58=NewOrderSingle" + fixDelimeter #text
  clordid +=1
  fix11 = "11=" + str(clordid).zfill(10) + fixDelimeter #client order ID
  fix59 = "59=0" + fixDelimeter #time in force (0:day)
  fix49 = "49=" + clienttarget + fixDelimeter #sender
  fix56 = "56=" + brokertarget + fixDelimeter #receiver
  fix60 = "60=" + transactionTime[:-3] + fixDelimeter #transactionTime

  #bundle client order msg
  clientOrderBody = fix11 + fix38 + fix40 + fix44 + fix54 + fix55 + fix59 + fix58 + fix60

  #admin layer
  clientmsgseq += 1
  fix34 = "34=" + str(clientmsgseq) + fixDelimeter #msg seq
  #start bundling header
  clientOrderHeader = fix35 + fix34 + fix49 + fix56
  
  sendTime = getTimeStampString()
  fix52 = "52=" + sendTime[:-3] + fixDelimeter #message send time

  clientOrderHeader = clientOrderHeader + fix52
  clientOrderMsg = clientOrderHeader + clientOrderBody
  fix9 = "9=" + str(len(clientOrderMsg)) + fixDelimeter
  clientOrderMsg = fix8 + fix9 + clientOrderMsg
  fix10 = "10=000" + fixDelimeter
  clientOrderMsg = clientOrderMsg + fix10

  logtime = getTimeStampString() #with microseconds
  #print(logtime + " " + clientOrderMsg + "\n")
  fh.write(logtime + " " + clientOrderMsg + "\n")
  msgCount += 1
  totalBytes += len(logtime + " " + clientOrderMsg + "\n")

  time.sleep(execDelay)

  #broker message
  brTransactionTime = getTimeStampString()
  time.sleep(engineDelay)
  brfix35 = "35=8" + fixDelimeter #msgtype
  brfix58 = "58=ExecReportFilled" + fixDelimeter #text
  brfix49 = "49=" + brokertarget + fixDelimeter #sender
  brfix56 = "56=" + clienttarget + fixDelimeter #receiver
  brfix60 = "60=" + brTransactionTime[:-3] + fixDelimeter #transactionTime
  brordid +=1
  fix37 = "37=" + str(brordid).zfill(10) + fixDelimeter #broker order ID
  execid +=1
  fix17 = "17=" + str(execid).zfill(10) + fixDelimeter #broker exec ID
  fix41 = "41=" + str(clordid).zfill(10) + fixDelimeter #orig client order id
  fix39 = "39=2" + fixDelimeter #OrdStatus (2:filled)
  fix150 = "150=F" + fixDelimeter #ExecType (F:[partial] fill)
  fix151 = "151=0" + fixDelimeter #LeavesQty
  fix14 = "14=" + str(qty) + fixDelimeter #Cum quantity
  fix32 = "32=" + str(qty) + fixDelimeter #Last quantity
  fix6 = "6=" + str(price) + fixDelimeter #Avg price
  fix31 = "31=" + str(price) + fixDelimeter #Last price

  #bundle broker app msg
  brokerBody = fix37 + fix41 + fix17 + fix39 + fix150 + \
    fix151 + fix14 + fix32 + fix6 + fix31 + \
    fix38 + fix40 + fix44 + fix54 + fix55 + fix59 + brfix58 + brfix60 

  #broker admin layer
  brokermsgseq += 1
  brfix34 = "34=" + str(brokermsgseq) + fixDelimeter #msg seq
  #start bundling header
  brokerHeader = brfix35 + brfix34 + brfix49 + brfix56

  brsendTime = getTimeStampString()
  fix52 = "52=" + brsendTime[:-3] + fixDelimeter #message send time

  brokerHeader = brokerHeader + fix52
  brokerMsg = brokerHeader + brokerBody
  fix9 = "9=" + str(len(brokerMsg)) + fixDelimeter
  brokerMsg = fix8 + fix9 + brokerMsg
  #fix10 = "10=000" + fixDelimeter
  brokerMsg = brokerMsg + fix10

  logtime = getTimeStampString() #with microseconds
  #print(logtime + " " + brokerMsg + "\n")
  fh.write(logtime + " " + brokerMsg + "\n")
  msgCount += 1
  totalBytes += len(logtime + " " + brokerMsg + "\n")

endSessionTime = datetime.datetime.now()
sessionRunTime = endSessionTime - startSessionTime
sessionSeconds = sessionRunTime.total_seconds()
trailer = "ENDOFFILE StartTime:" + startSessionTime.strftime(timeFormat) + \
  " EndTime:" + endSessionTime.strftime(timeFormat) + \
    " TotalMsgs:" + str(msgCount) + \
      " Total Seconds:" + str(sessionSeconds) + \
        " Msgs/Sec:" + str(msgCount / sessionSeconds) + \
          " Total Bytes:" + str(totalBytes) + \
            " Average Message Size:" + str(totalBytes / msgCount)
fh.write(trailer)
print(trailer)
fh.close()
print("DONE")