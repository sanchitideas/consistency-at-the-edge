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
totalRequests = 30000


'''
totalKeyRange = 50
hotspotKeyRange = 20
totalRequests = 15
'''
centralServer = 'pcap1.utah.cloudlab.us:50050'
edgeServers = ["c220g2-011331.wisc.cloudlab.us:50051", 
"c220g2-010631.wisc.cloudlab.us:50052", "c220g2-010630.wisc.cloudlab.us:50053",
"c220g2-011330.wisc.cloudlab.us:50054"]

totalTime = 0

def runClient(clientNumber, totalClients, seeder):
	random.seed(seeder)
	lowerRange = (clientNumber-1)*totalKeyRange + 1
	UpperRange = lowerRange + totalKeyRange - 1
	hotspotUpperRange = lowerRange + hotspotKeyRange - 1
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
				sToken = stub.bindToServer(kvstore_pb2.bindRequest(clientID = clientID))
			lastReqNo = lastRequestForEdgeServer[k]
			for i in range(initialReqNo, lastReqNo):
				if(i%10 == 0):
					keyID = random.randint(hotspotUpperRange+1, UpperRange)
				else:
					keyID = random.randint(lowerRange, hotspotUpperRange)
				s = str(keyID)
				len_s = len(s)
				x = 128 - 49 - len_s
				zeroes = '0'*x
				key = zeroes+s
				before = time.time()
				response = stub.getValue(kvstore_pb2.ValueRequest(key=key, 
					token = sToken))
				after = time.time()
				if i == initialReqNo:
					sToken = response.token
				timeList.append(after-before)
				global totalTime 
				totalTime += (after-before)
				if(response.value is None):
					print(keyID)
				#else:
				#    print(key, " -GotValue")
		initialReqNo = lastRequestForEdgeServer[k]

		if k == len(edgeServers) - 1:
			edgeServerToConnect = firstEdgeServer
		elif k < len(edgeServers) - 1:
			edgeServersVisited.append(edgeServerToConnect)
			while edgeServerToConnect in edgeServersVisited:
				edgeServerToConnect = random.randint(0, len(edgeServers) - 1)
		

		
	fileName = "LRU2/" + 'Read_' + clientID + "_of_" + totalClients + '.txt'
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