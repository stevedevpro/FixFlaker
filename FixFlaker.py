# FixFlaker.py - flaky fake FIX log generator
# Generates synthetic FIX logs
# bidirectional flow between client and broker
# NOT A FIX ENGINE OR SIMULATOR
# tested on Python 3.7.6

import os
import sys
import platform
import datetime
import time
import random
import urllib.request

class FixSession:
  def __init__(self, pid, hostname):
    self.clientmsgseq = 0
    self.brokermsgseq = 0
    self.clordid = 0
    self.brordid = 0
    self.execid = 0
    self.msgCount = 0
    self.totalBytes = 0
    self.session = pid + "@" + hostname
    self.clienttarget = "CLIENT_" + pid + "_" + hostname
    self.brokertarget = "BROKER_" + pid + "_" + hostname
    self.fixversion = "8=FIX4.4"
    self.fixDelimeter = "|"
    
    #MESSAGE RATES
    #current state msg rate
    self.engineDelay = .001
    self.execDelay = .005
    #10x msg rate
    #self.engineDelay = .0001
    #self.execDelay = .00025
    #max msg rate
    #self.engineDelay = 0
    #self.execDelay = 0

def getTimeFormat():
  #example: 20200619-16:27:15.276270
  #return "%Y%m%d-%H:%M:%S.%f"
  #example: 20200619-16:27:15.276270
  return "%Y-%m-%dT%H:%M:%S.%f"

def getTimeStampString():
  return datetime.datetime.now().strftime(getTimeFormat())

def getStockList():
  response = urllib.request.urlopen("https://datahub.io/core/s-and-p-500-companies/r/constituents.csv")
  stockfile = response.read().decode('utf-8')
  stocks = []
  for stockline in stockfile.splitlines():
    stk = stockline[0:stockline.find(",")]
    if stk != "Symbol":
      stocks.append(stk)
      print(stk)

  stocklen = len(stocks)
  print (str(stocklen) + " stocks loaded.")
  return stocks

def genFix(fixs, sec, fh):
  transactionTime = getTimeStampString()
  time.sleep(fixs.engineDelay)
  fixDelimeter = fixs.fixDelimeter
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
  fixs.clordid +=1
  fix11 = "11=" + str(fixs.clordid).zfill(10) + fixDelimeter #client order ID
  fix59 = "59=0" + fixDelimeter #time in force (0:day)
  fix49 = "49=" + fixs.clienttarget + fixDelimeter #sender
  fix56 = "56=" + fixs.brokertarget + fixDelimeter #receiver
  fix60 = "60=" + transactionTime[:-3] + fixDelimeter #transactionTime

  #bundle client order msg
  clientOrderBody = fix11 + fix38 + fix40 + fix44 + fix54 + fix55 + fix59 + fix58 + fix60

  #admin layer
  fixs.clientmsgseq += 1
  fix34 = "34=" + str(fixs.clientmsgseq) + fixDelimeter #msg seq
  #start bundling header
  clientOrderHeader = fix35 + fix34 + fix49 + fix56
  
  sendTime = getTimeStampString()
  fix52 = "52=" + sendTime[:-3] + fixDelimeter #message send time

  clientOrderHeader = clientOrderHeader + fix52
  clientOrderMsg = clientOrderHeader + clientOrderBody
  fix9 = "9=" + str(len(clientOrderMsg)) + fixDelimeter
  fix8 = fixs.fixversion + fixDelimeter
  clientOrderMsg = fix8 + fix9 + clientOrderMsg
  fix10 = "10=000" + fixDelimeter
  clientOrderMsg = clientOrderMsg + fix10

  logtime = getTimeStampString() #with microseconds

  fh.write(logtime[:-3] + " " + fixs.session + " " + clientOrderMsg + "\n")
  fixs.msgCount += 1
  fixs.totalBytes += len(logtime + " " + clientOrderMsg + "\n")

  time.sleep(fixs.execDelay)

  #broker message
  brTransactionTime = getTimeStampString()
  time.sleep(fixs.engineDelay)
  brfix35 = "35=8" + fixDelimeter #msgtype
  brfix58 = "58=ExecReportFilled" + fixDelimeter #text
  brfix49 = "49=" + fixs.brokertarget + fixDelimeter #sender
  brfix56 = "56=" + fixs.clienttarget + fixDelimeter #receiver
  brfix60 = "60=" + brTransactionTime[:-3] + fixDelimeter #transactionTime
  fixs.brordid +=1
  fix37 = "37=" + str(fixs.brordid).zfill(10) + fixDelimeter #broker order ID
  fixs.execid +=1
  fix17 = "17=" + str(fixs.execid).zfill(10) + fixDelimeter #broker exec ID
  fix41 = "41=" + str(fixs.clordid).zfill(10) + fixDelimeter #orig client order id
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
  fixs.brokermsgseq += 1
  brfix34 = "34=" + str(fixs.brokermsgseq) + fixDelimeter #msg seq
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
  fh.write(logtime[:-3] + " " + fixs.session + " " + brokerMsg + "\n")
  fixs.msgCount += 1
  fixs.totalBytes += len(logtime + " " + brokerMsg + "\n")

def main():
  print("Running...")
  pid = str(os.getpid())
  hostname = platform.node()[3:-13] #works on aws linux

  stocks = ["AMZN", "GOOG", "IBM", "XOM"]
  stocks = getStockList()

  fixs = FixSession(pid, hostname)

  startSessionTime = datetime.datetime.now()

  print ("STARTING " + pid + " on " + hostname + " at " + startSessionTime.strftime(getTimeFormat()))
  fixLogPath = "/tmp/"
  fixLogFileName = "fix.log"

  fixLogFile = fixLogPath + fixLogFileName + "." + startSessionTime.strftime(getTimeFormat())
  try:
    fh = open(fixLogFile, "w")
  except:
    print("problem opening file for writing. EXITING!")
    sys.exit()

  #fh.write("HEADER\n")

  ####sequential mode####
  # for sec in stocks:
  #   genFix(fixs, sec, fh)

  ####duration mode####
  sessionFinish = startSessionTime + datetime.timedelta(hours=0, minutes=10)
  while datetime.datetime.now() <= sessionFinish:
    stock = random.choice(stocks)
    genFix(fixs, stock, fh)

  ####counter mode####
  # maxtxn = 3
  # txncount = 1
  # while txncount <= maxtxn:
  #   stock = random.choice(stocks)
  #   genFix(fixs, stock, fh)
  #   txncount+=1

  endSessionTime = datetime.datetime.now()
  sessionRunTime = endSessionTime - startSessionTime
  sessionSeconds = sessionRunTime.total_seconds()
  trailer = "ENDOFFILE StartTime:" + startSessionTime.strftime(getTimeFormat()) + \
    "\nEndTime:" + endSessionTime.strftime(getTimeFormat()) + \
      "\nTotalMsgs:" + str(fixs.msgCount) + \
        "\nTotal Seconds:" + str(sessionSeconds) + \
          "\nMsgs/Sec:" + str(fixs.msgCount / sessionSeconds) + \
            "\nTotal Bytes:" + str(fixs.totalBytes) + \
              "\nAverage Message Size:" + str(fixs.totalBytes / fixs.msgCount)
  #fh.write(trailer)
  print(trailer)
  fh.close()
  print("DONE")

if __name__ == "__main__":
  main()