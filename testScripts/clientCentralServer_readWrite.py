from __future__ import print_function
import logging

import grpc
import time
import sys
import random
import time
import string

import centralserver_pb2
import centralserver_pb2_grpc

totalKeyRange = 50000
hotspotKeyRange = 20000
totalRequest = 15000
centralServer = 'pcap1.utah.cloudlab.us:50050'


# totalKeyRange = 50
# hotspotKeyRange = 20
# totalRequest = 15
#centralServer = 'localhost:50050'

totalTime = 0

value_size_bytes = 4*1024 - 49

def random_generator(size=1048576-49, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for x in range(size))


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

            global totalTime 

            toss = random.randint(1,10)
            if(toss%2==0): #perform read
                before = time.time()
                response = stub.getValue(centralserver_pb2.CentralServerValueRequest(key=key))
                after = time.time()                
                totalTime += (after-before)
                if(response.value is None):
                    print("could not read: ",keyID)
            else: #perfrom write
                value = random_generator(value_size_bytes)
                before = time.time()
                response = stub.setValue(centralserver_pb2.CentralServerSetRequest(key=key, value = value))
                after = time.time()
                totalTime += (after-before)
                if(response.success is None):
                    print("could not write: ",keyID)

if __name__ == '__main__':
    if(len(sys.argv) != 2):
        print("Usage: python3 clientCentralServer_read <clientNumber>")
        exit(1)
    runClient(int(sys.argv[1]))
    print("timeTaken:", totalTime)
    print("AverageTime:", totalTime/totalRequest)