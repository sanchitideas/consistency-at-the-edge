from __future__ import print_function
import logging
import string

import grpc
import time
import sys
import random
import time

import kvstore_pb2
import kvstore_pb2_grpc

totalReadKeyRange = 80000
totalWriteKeyRange = 50000
readHotspotKeyRange = 8000
writeHotspotKeyRange = 5000
totalRequests = 30000
centralServer = 'pcap1.utah.cloudlab.us:50050'
edgeServers = ["c220g2-010629.wisc.cloudlab.us:50051", 
"c220g2-011303.wisc.cloudlab.us:50052", 
"c220g2-010631.wisc.cloudlab.us:50053", 
"c220g2-010630.wisc.cloudlab.us:50054"]

#totalKeyRange = 50
#hotspotKeyRange = 20
#totalRequests= 15
#centralServer = 'localhost:50050'
#edgeServers = ["localhost:50051", "localhost:50052", "localhost:50053", "localhost:50054"]

totalTime = 0

value_size_bytes = 4*1024 - 49

def random_generator(size=1048576-49, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def runClient(clientNumber, totalClients):
    lowerRange = (clientNumber-1)*totalReadKeyRange + 1
    readUpperRange = lowerRange + totalReadKeyRange - 1
    writeUpperRange = lowerRange + totalWriteKeyRange - 1
    readHotspotUpperRange = lowerRange + readHotspotKeyRange - 1
    writeHotspotUpperRange = lowerRange + writeHotspotKeyRange - 1
    clientID = "client" + str(clientNumber)
    timeList = []
    with grpc.insecure_channel(edgeServers[(clientNumber-1)%4]) as channel:
        stub = kvstore_pb2_grpc.MultipleValuesStub(channel)
        sToken = stub.bindToServer(kvstore_pb2.bindRequest(clientID = clientID))
        for i in range(totalRequests):
            toss = random.randint(1,10)
            if(i%10 == 0):
                if (toss%2==0):
                    keyID = random.randint(readHotspotUpperRange+1, 
                        readUpperRange)
                else:
                    keyID = random.randint(writeHotspotUpperRange+1, 
                        writeUpperRange)                    
            else:
                if (toss%2==0):
                    keyID = random.randint(lowerRange, readHotspotUpperRange)
                else:
                    keyID = random.randint(lowerRange, writeHotspotUpperRange)


            s = str(keyID)
            len_s = len(s)
            x = 128 -49 - len_s
            zeroes = '0'*x
            key = zeroes+s

            global totalTime

            
            if(toss%2==0): #perform read
                before = time.time()
                response = stub.getValue(kvstore_pb2.ValueRequest(key=key, token = sToken))
                after = time.time()                
                totalTime += (after-before)
                timeList.append(after-before)
                if(response.value is None):
                    print("could not read: ",keyID)
            else: #perfrom write
                value = random_generator(value_size_bytes)
                before = time.time()
                response = stub.setValue(kvstore_pb2.SetRequest(key=key, value = value, token = sToken))
                after = time.time()
                totalTime += (after-before)
                timeList.append(after-before)
                if(response.success is None):
                    print("could not write: ",keyID)
        fileName = "readWrite_" + clientID + "_of_" + str(totalClients) + ".txt"
        with open(fileName, 'w') as file1:
            for t in timeList:
                file1.write(str(t))
                file1.write('\n')

if __name__ == '__main__':
    if(len(sys.argv) != 3):
        print("Usage: python3 clientCentralServer_readWrite.py <clientNumber> <totalClients>")
        exit(1)
    runClient(int(sys.argv[1]), int(sys.argv[2]))
    print("timeTaken:", totalTime)
    print("AverageTime:", totalTime/totalRequest)
    print("success")