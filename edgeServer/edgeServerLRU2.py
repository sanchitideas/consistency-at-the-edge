from concurrent import futures
import time
import math
import logging
import heapq

import grpc

import kvstore_pb2
import kvstore_pb2_grpc

import centralserver_pb2
import centralserver_pb2_grpc

import yaml
import sys
from threading import Lock
from threading import Timer

import collections


INVALID_SESSION = "invalid_session"
INITIAL_CACHE_CAPACITY = 2000

class User:
	def __init__(self, sessID, clientID, currentEpoch):
		self.sessionID = sessID
		# not sure why we'd need clientID, but leaving it here for the future
		self.clientID = clientID
		self.cache = LRUCache(INITIAL_CACHE_CAPACITY)
		self.epoch = currentEpoch


class LRUCache:
	# @param capacity, an integer
	def __init__(self, capacity): #capacity- number of rows allowed
		self.incrementSize = capacity
		self.currentReqNo = -1
		self.maxCacheCapacity = 5*capacity
		self.capacity = capacity
		self.firstLevelCache = collections.OrderedDict()
		self.secondLevelCache = dict()
		# heap to select the earliest req numbers
		self.heapQueue = []
		# mapping from key to request ID in the heap
		self.keyHeapMapper = dict()
		# mapping from request ID to key in the heap
		self.reqHeapMapper = dict()
		# latest access time for a key accessed more than once
		self.latestAccessTime = dict()

	# @return an integer
	def get(self, key):
		self.currentReqNo += 1
		if key in self.firstLevelCache:
			value = self.firstLevelCache.pop(key)
			self.secondLevelCache[key] = value
			# add entry to latestAccessTime
			self.latestAccessTime[key] = self.currentReqNo			
		elif key in self.secondLevelCache:
			value = self.secondLevelCache[key]	
			heapItemOfKey = self.keyHeapMapper[key]
			del self.reqHeapMapper[heapItemOfKey]
			self.heapQueue.remove(heapItemOfKey)
			heapq.heapify(self.heapQueue)
			lastAccessTime = self.latestAccessTime[key]
			self.latestAccessTime[key]= self.currentReqNo
			heapq.heappush(self.heapQueue, lastAccessTime)
			self.reqHeapMapper[lastAccessTime] = key
			self.keyHeapMapper[key] = lastAccessTime
		else:       
			return None
		return value

	# @param key, an integer
	# @param value, an integer
	# @return nothing
	def set(self, key, value):
		self.currentReqNo += 1
		if key in self.secondLevelCache:
			# key is being accessed 3rd or more time
			self.secondLevelCache[key] = value
			heapItemOfKey = self.keyHeapMapper[key]
			del self.reqHeapMapper[heapItemOfKey]
			self.heapQueue.remove(heapItemOfKey)
			heapq.heapify(self.heapQueue)
			lastAccessTime = self.latestAccessTime[key]
			self.latestAccessTime[key]= self.currentReqNo
			heapq.heappush(self.heapQueue, lastAccessTime)
			self.reqHeapMapper[lastAccessTime] = key
			self.keyHeapMapper[key] = lastAccessTime 

		elif key in self.firstLevelCache:
			# key being accessed the second time
			self.firstLevelCache.pop(key)
			self.secondLevelCache[key] = value
			# add entry to latestAccessTime
			self.latestAccessTime[key] = self.currentReqNo
		else:
			# first-time insertion
			currentSize = len(self.firstLevelCache) + len(self.secondLevelCache)
			if currentSize == self.capacity:
				if self.capacity == self.maxCacheCapacity:
					if len(self.firstLevelCache) >= 1:
						# remove an item from the first-level cache
						keyToRemove = self.firstLevelCache.popitem(last=False)[0]
						heapItemOfKeyToRemove = self.keyHeapMapper[keyToRemove]
						self.heapQueue.remove(heapItemOfKeyToRemove)
						heapq.heapify(self.heapQueue)
						del self.reqHeapMapper[heapItemOfKeyToRemove]
						del self.keyHeapMapper[keyToRemove]
					else:
						# remove an item from the last level cache
						heapItemOfKeyToRemove = heapq.heappop(self.heapQueue)
						keyToRemove = self.reqHeapMapper[heapItemOfKeyToRemove]
						del self.reqHeapMapper[heapItemOfKeyToRemove]
						del self.keyHeapMapper[keyToRemove]
						del self.secondLevelCache[keyToRemove]
						del self.latestAccessTime[keyToRemove]
				else:
					self.capacity += self.incrementSize
	
			self.firstLevelCache[key] = value
			self.keyHeapMapper[key] = self.currentReqNo
			heapq.heappush(self.heapQueue, self.currentReqNo)
			self.reqHeapMapper[self.currentReqNo] = key


class KVStoreServicer(kvstore_pb2_grpc.MultipleValuesServicer):
	"""Provides methods that implement functionality of Multiple Values Servicer."""

	def __init__(self, serverID):
		#self.cache = LRUCache(6) #key - client specific key, Value - List (value, timestamp)
		self.serverID = serverID
		self.activeSessionIDs = {}    
		with open("neighbouringEdgeServer.yaml") as file:
			self.neighboringEdgeServers = yaml.safe_load(file)
		self.centralServerConn = self.connectCentralServer()  
		self.epoch = -1
		self.changeEpochAndCollectGarbage()

	def changeEpochAndCollectGarbage(self):
		toRemove= []
		self.epoch += 1
		for sessionID, user in self.activeSessionIDs.items():
			# race conditions possible. They were also possible in the old design
			if user.epoch <= self.epoch - 2:
				toRemove.append(sessionID)
		for entry in toRemove:
			del self.activeSessionIDs[entry]
		Timer(1200, self.changeEpochAndCollectGarbage).start() #collecting garbage in background 20 mins
	
	def connectCentralServer(self):
		channel = grpc.insecure_channel(self.neighboringEdgeServers["centralServer"])         
		return centralserver_pb2_grpc.CentralServerStub(channel)
	
	def writeToCentralServer(self, key, value):
		return self.centralServerConn.setValue(centralserver_pb2.CentralServerSetRequest(key=key, value = value))
	
	def readFromCentralServer(self, key):
		return self.centralServerConn.getValue(centralserver_pb2.CentralServerValueRequest(key=key))

	def fetchFromNeighbour(self, neighbourID, clientID, sessionID):
		with grpc.insecure_channel(self.neighboringEdgeServers[neighbourID]) as channel:
			stub = kvstore_pb2_grpc.MultipleValuesStub(channel)
			response = stub.cacheMigration(kvstore_pb2.FetchRequest(clientID = clientID, sessionID = sessionID))
			newCache = LRUCache(INITIAL_CACHE_CAPACITY)
			firstLevelItems = 1
			secondLevelItems = 1
			heapQueueItems = 1
			keyHeapMapperItems = 1
			reqHeapMapperItems = 1
			latestAccessTimeItems = 1

			for i, entry in enumerate(response):
				if (i == 0):
					if(entry.key == INVALID_SESSION):
						return False
					elif entry.key == "FIRST_LEVEL_ITEMS":
						firstLevelItems = int(entry.value) + 6
				elif (i == 1):
					secondLevelItems = int(entry.value) + firstLevelItems
					newCache.capacity =  (int((secondLevelItems - 7) / \
						INITIAL_CACHE_CAPACITY) + 1) * INITIAL_CACHE_CAPACITY
					#print(newCache.capacity)
				elif (i == 2):
					heapQueueItems = int(entry.value) + secondLevelItems 
				elif (i == 3):
					keyHeapMapperItems = int(entry.value) + heapQueueItems
				elif (i == 4):
					reqHeapMapperItems = int(entry.value) + keyHeapMapperItems
				elif (i == 5):
					latestAccessTimeItems = int(entry.value) + reqHeapMapperItems
				elif (i == 6):
					newCache.currentReqNo = int(entry.value)
				elif i >= 7 and i <= firstLevelItems:
					newCache.firstLevelCache[entry.key] = \
						[entry.value, float(entry.timeStamp)]
				elif i > firstLevelItems and i <= secondLevelItems:
					newCache.secondLevelCache[entry.key] = \
						[entry.value, float(entry.timeStamp)]
				elif i > secondLevelItems and i <= heapQueueItems:
					newCache.heapQueue.append(int(entry.key))
				elif i > heapQueueItems and i <= keyHeapMapperItems:
					newCache.keyHeapMapper[entry.key] = int(entry.value)
				elif i > keyHeapMapperItems and i <= reqHeapMapperItems:
					 newCache.reqHeapMapper[int(entry.key)] = entry.value
				else:
					newCache.latestAccessTime[entry.key] = int(entry.value)
		self.activeSessionIDs[sessionID] = User(sessionID, clientID, self.epoch)
		self.activeSessionIDs[sessionID].cache = newCache #updating the cache here (linking to user cache)
		#print(newCache.firstLevelCache)
		#print(newCache.secondLevelCache)
		return True

	def cacheMigration(self, request, context):
		if(request.sessionID not in self.activeSessionIDs.keys()):
			#notify the destination server that this session has been invalidated
			yield kvstore_pb2.CacheEntry(key = INVALID_SESSION, clientID = INVALID_SESSION, value = INVALID_SESSION, timeStamp = INVALID_SESSION)
			return
		# transfer all state
		yield kvstore_pb2.CacheEntry(key = "FIRST_LEVEL_ITEMS", 
			value = str(len(self.activeSessionIDs[request.sessionID].cache.firstLevelCache)))
		yield kvstore_pb2.CacheEntry(key = "SECOND_LEVEL_ITEMS", 
			value = str(len(self.activeSessionIDs[request.sessionID].cache.secondLevelCache)))
		yield kvstore_pb2.CacheEntry(key = "HEAP_QUEUE_ITEMS", 
			value = str(len(self.activeSessionIDs[request.sessionID].cache.heapQueue)))
		yield kvstore_pb2.CacheEntry(key = "KEY_MAP_ITEMS", 
			value = str(len(self.activeSessionIDs[request.sessionID].cache.keyHeapMapper)))
		yield kvstore_pb2.CacheEntry(key = "REQ_MAP_ITEMS", 
			value = str(len(self.activeSessionIDs[request.sessionID].cache.reqHeapMapper)))
		yield kvstore_pb2.CacheEntry(key = "LATEST_TIME_ITEMS", 
			value = str(len(self.activeSessionIDs[request.sessionID].cache.latestAccessTime)))
		yield kvstore_pb2.CacheEntry(key = "CURRENT_REQUEST_NUMBER", 
			value = str(self.activeSessionIDs[request.sessionID].cache.currentReqNo))
		for key, value in self.activeSessionIDs[request.sessionID].cache.firstLevelCache.items():
			try:
				yield kvstore_pb2.CacheEntry(key = key, value = value[0], timeStamp = str(value[1]))
			except Exception as ex:
				print(ex)
				print("could not migrate entry ", key, ":", value)
		for key, value in self.activeSessionIDs[request.sessionID].cache.secondLevelCache.items():
			try:
				yield kvstore_pb2.CacheEntry(key = key, value = value[0], timeStamp = str(value[1]))
			except Exception as ex:
				print(ex)
				print("could not migrate entry ", key, ":", value)
		for value in self.activeSessionIDs[request.sessionID].cache.heapQueue:
			try:
				yield kvstore_pb2.CacheEntry(key = str(value))
			except Exception as ex:
				print(ex)
				print("could not migrate entry ", key, ":", value)
		for key, value in self.activeSessionIDs[request.sessionID].cache.keyHeapMapper.items():
			try:
				yield kvstore_pb2.CacheEntry(key = key, value = str(value))
			except Exception as ex:
				print(ex)
				print("could not migrate entry ", key, ":", value)
		for key, value in self.activeSessionIDs[request.sessionID].cache.reqHeapMapper.items():
			try:
				yield kvstore_pb2.CacheEntry(key = str(key), value = value)
			except Exception as ex:
				print(ex)
				print("could not migrate entry ", key, ":", value)
		for key, value in self.activeSessionIDs[request.sessionID].cache.latestAccessTime.items():
			try:
				yield kvstore_pb2.CacheEntry(key = key, value = str(value))
			except Exception as ex:
				print(ex)
				print("could not migrate entry ", key, ":", value)
		del self.activeSessionIDs[request.sessionID]
		#self.activeSessionIDs.remove(request.sessionID)


	def bindToServer(self, request, context): #to establish the session for the first time        
		sessionID = request.clientID + "-" + str(time.time())
		self.activeSessionIDs[sessionID] = User(sessionID, request.clientID, self.epoch)
		#self.activeSessionIDs.add(sessionID) #remove once session shift
		return kvstore_pb2.sToken(clientID = request.clientID, serverID = self.serverID, sessionID = sessionID)
	

	def setValue(self, request, context):
		if(request.token.sessionID not in self.activeSessionIDs.keys()):
			status = True
			if(request.token.serverID == self.serverID or request.token.serverID not in self.neighboringEdgeServers.keys()): #this means that it was my session, but it was garbage collected or source was not in my neighbours
				status = False
			else: #transfer from neighibouring edge node
				# TODO : fetch in another thread
				status = self.fetchFromNeighbour(request.token.serverID, request.token.clientID, request.token.sessionID)
			
			if(not status): #create a new session and token
				sessionID = request.token.clientID + "-" + str(time.time())
				self.activeSessionIDs[sessionID] = User(sessionID, request.token.clientID, self.epoch)
				request.token.serverID = self.serverID
				request.token.sessionID = sessionID
			else: #fetch was succesful from the neighbour, now update the current server to self
				request.token.serverID = self.serverID      

		self.activeSessionIDs[request.token.sessionID].epoch = self.epoch

		centralServerResponse = self.writeToCentralServer(request.key, request.value)
		if(centralServerResponse.success):
			currentTime = time.time()          
			self.activeSessionIDs[request.token.sessionID].cache.set(request.key, [request.value, currentTime])
			return kvstore_pb2.SetResponse(key=request.key, success=True, token = request.token)
		else:
			return kvstore_pb2.SetResponse(key=request.key, success=False, token = request.token)

	def getValue(self, request, context):
		if(request.token.sessionID not in self.activeSessionIDs.keys()):
			status = True
			if(request.token.serverID == self.serverID or request.token.serverID not in self.neighboringEdgeServers.keys()): #this means that it was my session, but it was garbage collected or source was not in my neighbours
				status = False
			else: #transfer from neighibouring edge node
				status = self.fetchFromNeighbour(request.token.serverID, request.token.clientID, request.token.sessionID)
			
			if(not status): #create a new session and token
				sessionID = request.token.clientID + "-" + str(time.time())
				self.activeSessionIDs[sessionID] = User(sessionID, request.token.clientID, self.epoch)
				request.token.serverID = self.serverID
				request.token.sessionID = sessionID
				
			else: #fetch was succesful from the neighbour, now update the current server to self
				request.token.serverID = self.serverID

		
		self.activeSessionIDs[request.token.sessionID].epoch = self.epoch #making the session a part of current epoch

		toReturn = self.activeSessionIDs[request.token.sessionID].cache.get(request.key)
		if(not toReturn):
			#fetch from central server
			centralServerResponse = self.readFromCentralServer(request.key)
			currentTime = time.time()
			if(centralServerResponse.value is not None):       
				self.activeSessionIDs[request.token.sessionID].cache.set(request.key, [centralServerResponse.value, currentTime])
				toReturn = [centralServerResponse.value, currentTime]
			else:
				return kvstore_pb2.ValueResponse(key=request.key, value = None, timeStamp = None, token = request.token)

		return kvstore_pb2.ValueResponse(key=request.key, value = toReturn[0], timeStamp = toReturn[1], token = request.token)



def serve(serverID):
	with open("neighbouringEdgeServer.yaml") as file:
		neighboringEdgeServers = yaml.safe_load(file)
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	kvstore_pb2_grpc.add_MultipleValuesServicer_to_server(
		KVStoreServicer(serverID), server)    
	connectionInfo = neighboringEdgeServers[serverID]
	print('Starting server. Listening on port: ', connectionInfo)
	server.add_insecure_port(connectionInfo)
	server.start()
	server.wait_for_termination()


if __name__ == '__main__':
	if(len(sys.argv) != 2):
		print("Usage: python3 edgeServer.py <serverID as defined in yaml>")
		exit(1)
	logging.basicConfig()
	serve(sys.argv[1])