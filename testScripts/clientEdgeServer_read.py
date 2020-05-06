from __future__ import print_function
import logging

import grpc
import time
import sys
import random
import time

import kvstore_pb2
import kvstore_pb2_grpc

totalKeyRange = 80000
hotspotKeyRange = 8000
totalRequest = 30000
centralServer = 'pcap1.utah.cloudlab.us:50050'
edgeServers = ["c220g2-010629.wisc.cloudlab.us:50051", "c220g2-011303.wisc.cloudlab.us:50052", "c220g2-010631.wisc.cloudlab.us:50053", "c220g2-010630.wisc.cloudlab.us:50054"]

#totalKeyRange = 50
#hotspotKeyRange = 20
#totalRequest = 15
#centralServer = 'localhost:50050'
#edgeServers = ["localhost:50051", "localhost:50052", "localhost:50053", "localhost:50054"]

totalTime = 0

def runClient(clientNumber, totalClients):
    lowerRange = (clientNumber-1)*totalKeyRange + 1
    UpperRange = lowerRange + totalKeyRange - 1
    hotspotUpperRange = lowerRange + hotspotKeyRange - 1
    clientID = "client" + str(clientNumber)
    timeList = []
    with grpc.insecure_channel(edgeServers[(clientNumber-1)%4]) as channel:
        stub = kvstore_pb2_grpc.MultipleValuesStub(channel)
        sToken = stub.bindToServer(kvstore_pb2.bindRequest(clientID = clientID))
        for i in range(totalRequest):
            if(i%10 == 0):
                keyID = random.randint(hotspotUpperRange+1, UpperRange)
            else:
                keyID = random.randint(lowerRange, hotspotUpperRange)
            s = str(keyID)
            len_s = len(s)
            x = 128 -49 - len_s
            zeroes = '0'*x
            key = zeroes+s
            before = time.time()
            response = stub.getValue(kvstore_pb2.ValueRequest(key=key, token = sToken))
            after = time.time()
            timeList.append(after-before)
            global totalTime 
            totalTime += (after-before)
            if(response.value is None):
                print(keyID)
            #else:
            #    print(key, " -GotValue")
        
        fileName = 'read_' + clientID + "_of_" + str(totalClients) + '.txt'
        with open(fileName, 'w') as file1:
            for t in timeList:
                file1.write(str(t))
                file1.write('\n')


if __name__ == '__main__':
    if(len(sys.argv) != 3):
        print("Usage: python3 clientCentralServer_read <clientNumber> <totalClients>")
        exit(1)
    runClient(int(sys.argv[1]), int(sys.argv[2]))
    print("timeTaken:", totalTime)
    print("AverageTime:", totalTime/totalRequest)
    print("success")