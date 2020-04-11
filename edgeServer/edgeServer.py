from concurrent import futures
import time
import math
import logging

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

class User:
	def __init__(self, sessID, clientID, currentEpoch):
		self.sessionID = sessID
		# not sure why we'd need clientID, but leaving it here for the future
		self.clientID = clientID
		self.cache = LRUCache(6)
		self.epoch = currentEpoch


class LRUCache:

	# @param capacity, an integer
	def __init__(self, capacity): #capacity- number of rows allowed
		self.capacity = capacity
		self.currentCache = collections.OrderedDict()

	# @return an integer
	def get(self, key):
		if key not in self.currentCache:
			return None
		else:       
			value = self.currentCache.pop(key)
		self.currentCache[key] = value
		return value

	# @param key, an integer
	# @param value, an integer
	# @return nothing
	def set(self, key, value):
		if key in self.currentCache:
			self.currentCache.pop(key)
		elif len(self.currentCache) == self.capacity:
			self.currentCache.popitem(last=False)
		self.currentCache[key] = value

	def switchCache(self, newEntries):
		for entry in newEntries[::-1]:
			self.currentCache[entry[0]] = [entry[1], entry[2]]



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
			newEntries = []
			for i, entry in enumerate(response):
				if (i == 0):
					if(entry.key == INVALID_SESSION):
						return False
				newEntries.append([entry.key, entry.value, float(entry.timeStamp)])
		self.activeSessionIDs[sessionID] = User(sessionID, clientID, self.epoch)
		self.activeSessionIDs[sessionID].cache.switchCache(newEntries)
		return True

	def cacheMigration(self, request, context):
		if(request.sessionID not in self.activeSessionIDs.keys()):
			#notify the destination server that this session has been invalidated
			yield kvstore_pb2.CacheEntry(key = INVALID_SESSION, clientID = INVALID_SESSION, value = INVALID_SESSION, timeStamp = INVALID_SESSION)
			return    
		for key, value in self.activeSessionIDs[request.sessionID].cache.currentCache.items():
			try:
				yield kvstore_pb2.CacheEntry(key = key, value = value[0], timeStamp = str(value[1]))
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
