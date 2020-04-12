from __future__ import print_function
import logging

import grpc
import time
import sys
import random
import time

import centralserver_pb2
import centralserver_pb2_grpc

totalKeyRange = 50000
hotspotKeyRange = 20000
totalRequest = 25000
centralServer = 'pcap1.utah.cloudlab.us:50050'


#totalKeyRange = 50
#hotspotKeyRange = 20
#totalRequest = 15
#centralServer = 'localhost:50050'

totalTime = 0

def runClient(clientNumber):
    lowerRange = (clientNumber-1)*totalKeyRange + 1
    UpperRange = lowerRange + totalKeyRange - 1
    hotspotUpperRange = lowerRange + hotspotKeyRange - 1
    with grpc.insecure_channel(centralServer) as channel:
        stub = centralserver_pb2_grpc.CentralServerStub(channel)
        for i in range(totalRequest):
            if(i%200 == 0):
                keyID = random.randint(hotspotUpperRange+1, UpperRange)
            else:
                keyID = random.randint(lowerRange, hotspotUpperRange)
            s = str(keyID)
            len_s = len(s)
            x = 128 -49 - len_s
            zeroes = '0'*x
            key = zeroes+s
            before = time.time()
            response = stub.getValue(centralserver_pb2.CentralServerValueRequest(key=key))
            after = time.time()
            global totalTime 
            totalTime += (after-before)
            if(response.value is None):
                print(keyID)
            #else:
            #    print(key, " -GotValue")    


if __name__ == '__main__':
    if(len(sys.argv) != 2):
        print("Usage: python3 clientCentralServer_read <clientNumber>")
        exit(1)
    runClient(int(sys.argv[1]))
    print("timeTaken:", totalTime)
    print("AverageTime:", totalTime/totalRequest)
    print("success")