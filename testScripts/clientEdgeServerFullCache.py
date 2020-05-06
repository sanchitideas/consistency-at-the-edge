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


# edit this workload here if required
totalReadKeyRange = 80000
totalWriteKeyRange = 50000
readHotspotKeyRange = 8000
writeHotspotKeyRange = 5000
totalRequests = 30000


centralServer = 'pcap1.utah.cloudlab.us:50050'
edgeServers = ["c220g2-011331.wisc.cloudlab.us:50051", 
"c220g2-010631.wisc.cloudlab.us:50052", "c220g2-010630.wisc.cloudlab.us:50053", 
"c220g2-011330.wisc.cloudlab.us:50054"]

'''
totalKeyRange = 50
hotspotKeyRange = 20
totalRequests = 15
centralServer = 'localhost:50050'
edgeServers = ["localhost:50051", "localhost:50052", "localhost:50053", "localhost:50054"]
'''

totalTime = 0

value_size_bytes = 4*1024 - 49

def random_generator(size=1048576-49, chars=string.ascii_letters + string.digits):
	return ''.join(random.choice(chars) for x in range(size))

def runClient(clientNumber, totalClients, seeder):
	random.seed(seeder)
	lowerRange = (clientNumber-1)*totalReadKeyRange + 1
	readUpperRange = lowerRange + totalReadKeyRange - 1
	writeUpperRange = lowerRange + totalWriteKeyRange - 1
	readHotspotUpperRange = lowerRange + readHotspotKeyRange - 1
	writeHotspotUpperRange = lowerRange + writeHotspotKeyRange - 1
	clientID = "client" + str(clientNumber)
	timeList = []
	# there will be transitions to 4 edge servers, back to the very first one
	lastRequestForEdgeServer = []

	for j in range(len(edgeServers)):
		# prevent the case of all clients establishing connections at once
		lowerTxRange = int(totalRequests * (2 * j + 1)/8)
		upperTxRange = int(totalRequests * (j + 1)/4)
		lastRequestNo = random.randint(lowerTxRange, upperTxRange - 1)
		lastRequestForEdgeServer.append(lastRequestNo)
	# back to the first edge server
	lastRequestForEdgeServer.append(totalRequests)
	#print(lastRequestForEdgeServer)

	initialReqNo = 0
	firstEdgeServer = (clientNumber-1)%4
	edgeServerToConnect = firstEdgeServer
	edgeServersVisited = []
	sToken = None

	for k in range(len(lastRequestForEdgeServer)):
		# each edgeServer connection

		with grpc.insecure_channel(edgeServers[edgeServerToConnect]) as channel:
			stub = kvstore_pb2_grpc.MultipleValuesStub(channel)
			if k == 0:
				sToken = stub.bindToServer(kvstore_pb2.bindRequest(clientID = \
					clientID))
			
			lastReqNo = lastRequestForEdgeServer[k]
			for i in range(initialReqNo, lastReqNo):
				toss = random.randint(1,10)
				if(i%10 == 0):
					if(toss%2==0): #perform read
						keyID = random.randint(readHotspotUpperRange+1, 
							readUpperRange)
					else:
						keyID = random.randint(writeHotspotUpperRange+1, 
							writeUpperRange)
				else: # THE 10%
					if(toss%2==0): #perform read
						keyID = random.randint(lowerRange, 
							readHotspotUpperRange)
					else:
						keyID = random.randint(lowerRange, 
							writeHotspotUpperRange)
				s = str(keyID)
				len_s = len(s)
				x = 128 -49 - len_s
				zeroes = '0'*x
				key = zeroes+s

				global totalTime

				if(toss%2==0): #perform read
					before = time.time()
					response = stub.getValue(kvstore_pb2.ValueRequest(key=key, 
						token = sToken))
					after = time.time()                
					totalTime += (after-before)
					timeList.append(after-before)
					if i == initialReqNo:
						sToken = response.token
					if(response.value is None):
						print("could not read: ",keyID)
				else: #perfrom write
					value = random_generator(value_size_bytes)
					before = time.time()
					response = stub.setValue(kvstore_pb2.SetRequest(key=key, 
						value = value, token = sToken))
					after = time.time()
					totalTime += (after-before)
					timeList.append(after-before)
					if i == initialReqNo:
						sToken = response.token
					if(response.success is None):
						print("could not write: ",keyID)

		initialReqNo = lastRequestForEdgeServer[k]


		if k == len(edgeServers) - 1:
			edgeServerToConnect = firstEdgeServer
		elif k < len(edgeServers) - 1:
			edgeServersVisited.append(edgeServerToConnect)
			while edgeServerToConnect in edgeServersVisited:
				edgeServerToConnect = random.randint(0, len(edgeServers) - 1)
				#print(edgeServerToConnect)
	fileName = 'LRU2/ReadWrite_' + clientID + "_of_" + totalClients + '.txt'
	with open(fileName, 'w') as file1:
		file1.truncate()
		for t in timeList:
			file1.write(str(t))
			file1.write('\n')

if __name__ == '__main__':
	if(len(sys.argv) != 4):
		print("Usage: python3 clientEdgeServer_read <clientNumber> <totalClients> <seed>")
		exit(1)
	runClient(int(sys.argv[1]), sys.argv[2], int(sys.argv[3]))
	print("timeTaken:", totalTime)
	print("AverageTime:", totalTime/totalRequests)
	print("success")