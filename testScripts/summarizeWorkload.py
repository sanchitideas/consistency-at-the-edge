import random
import sys

def summarizeWorkload(seedNumber, clientNo):
	random.seed(int(seedNumber))

	# edit this workload here if required
	totalKeyRange = 80000
	hotspotKeyRange = 8000
	totalRequests = 30000
	clientNumber = int(clientNo)
	lowerRange = (clientNumber-1)*totalKeyRange + 1
	UpperRange = lowerRange + totalKeyRange - 1
	hotspotUpperRange = lowerRange + hotspotKeyRange - 1
	lastRequestForEdgeServer = []
	for j in range(4):
		# prevent the case of all clients establishing connections at once
		lowerTxRange = int(totalRequests * (2 * j + 1)/8)
		upperTxRange = int(totalRequests * (j + 1)/4)
		lastRequestNo = random.randint(lowerTxRange, upperTxRange - 1)
		lastRequestForEdgeServer.append(lastRequestNo)

	lastRequestForEdgeServer.append(totalRequests)
	initialReqNo = 0

	# request numbers that are cache-hits for communicating edge-servers
	cacheHitsForCommunicatingServers = []
	cacheHitsForNonCommunicatingServers = []
	reqIDsOfPotentialCacheMisses = []
	numUniqueRequests = 0
	# index is the request no, value is the keyID that was requested
	allRequests = []
	cache = []
	cacheHits = 0

	for k in range(len(lastRequestForEdgeServer)):
		localReqs = []
		potentialCacheMisses = 0
		uniqueReadsOnThisEdgeServer = 0
		localReReads = 0
		lastReqNo = lastRequestForEdgeServer[k]
		for i in range(initialReqNo, lastReqNo):
			if(i%10 == 0):
				keyID = random.randint(hotspotUpperRange+1, UpperRange)
			else:
				keyID = random.randint(lowerRange, hotspotUpperRange)
			allRequests.append(keyID)
			if keyID not in localReqs:
				localReqs.append(keyID)
				if keyID in cache:
					# must've been read at the previous edge-server
					potentialCacheMisses += 1
					reqIDsOfPotentialCacheMisses.append(i)
			else:
				'''
				cache hit for non-communicating edge-server,
				if cache on this server doesn't cycle 10000 items
				'''
				localReReads += 1
				cacheHitsForNonCommunicatingServers.append(i)
			if keyID not in cache:
				''' 
				an item locally read can be removed from cache, 
				so that's why this case's here
				'''
				uniqueReadsOnThisEdgeServer += 1
				cache.append(keyID)
				if len(cache) == 10001:
					#print("cache full")
					cache = cache[-10000:]
			else:
				cacheHits += 1
				cacheHitsForCommunicatingServers.append(i)
		# Note that cache TX doesn't avoid all cache misses 
		print("non-communicating edge server handled %i requests, "
			"%i had %i cache-hits and %i "
			"cache misses which cache-transfer would've" 
			" avoided"%(lastReqNo - initialReqNo, k, 
			localReReads, potentialCacheMisses))
		initialReqNo = lastRequestForEdgeServer[k]

	print("\nAll the communicating edge servers had %i cache-hits"
		" in total"%(cacheHits))

	'''
	# uncomment the multi-line comments to view
	print("\n\nRequest IDs of potential cache misses:")
	print(reqIDsOfPotentialCacheMisses)
	print(allRequests)
	'''

if __name__ == '__main__':
	if(len(sys.argv) != 3):
		print("Usage: python3 summarize.py <INTEGER_AS_SEED> <CLIENT_NUMBER>")
		exit(1)
	summarizeWorkload(sys.argv[1], sys.argv[2])
