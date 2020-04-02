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

class LRUCache:

    # @param capacity, an integer
    def __init__(self, capacity): #capacity- number of rows allowed
        self.capacity = capacity
        self.localCache = collections.OrderedDict()

    # @return an integer
    def get(self, key):
        if not key in self.localCache:
            return None
        value = self.localCache.pop(key)
        self.localCache[key] = value
        return value

    # @param key, an integer
    # @param value, an integer
    # @return nothing
    def set(self, key, value):
        if key in self.localCache:
            self.localCache.pop(key)
        elif len(self.localCache) == self.capacity:
            self.localCache.popitem(last=False)
        self.localCache[key] = value


class KVStoreServicer(kvstore_pb2_grpc.MultipleValuesServicer):
    """Provides methods that implement functionality of Multiple Values Servicer."""

    def __init__(self, serverID):
        self.cache = LRUCache(6) #key - tuple(client specific key, clientID), Value - List (value, timestamp)
        self.serverID = serverID
        self.activeSessionIDs = {}    
        with open("neighbouringEdgeServer.yaml") as file:
            self.neighboringEdgeServers = yaml.safe_load(file)
        self.centralServerConn = self.connectCentralServer()  
        self.cacheLock = Lock()
        self.epoch = -1
        self.changeEpochAndCollectGarbage()
    
    def changeEpochAndCollectGarbage(self):
        self.epoch += 1
        if(self.epoch >= 2):
            toRemove = []
            for k,v in self.activeSessionIDs.items():
                if(v <= self.epoch - 2):
                    toRemove.append(k)
            for key in toRemove:
                del self.activeSessionIDs[key] 
                #should we clear cache here as well?  
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
                newEntries.append([entry.key, entry.clientID, entry.value, float(entry.timeStamp)])
        self.mergeCache(newEntries)
        self.activeSessionIDs[sessionID] = self.epoch
        return True
        

    def mergeCache(self, newEntries):
        entries = []
        self.cacheLock.acquire()
        for key, value in self.cache.localCache.items():
            entries.append([key[0], key[1], value[0], value[1]])
        entries.extend(newEntries)
        entries.sort(key = lambda x: x[3], reverse = True) #sorting by time stamp
        entries = entries[:self.cache.capacity]
        self.cache.localCache.clear() #clearing the cache. May need to remove it later
        for entry in entries[::-1]:
            self.cache.set((entry[0], entry[1]), [entry[2], entry[3]])
        self.cacheLock.release()


    def cacheMigration(self, request, context):
        if(request.sessionID not in self.activeSessionIDs.keys()):
            #notify the destination server that this session has been invalidated
            yield kvstore_pb2.CacheEntry(key = INVALID_SESSION, clientID = INVALID_SESSION, value = INVALID_SESSION, timeStamp = INVALID_SESSION)
            return    
        clientID = request.clientID
        toRemoveKeys = []
        for key, value in self.cache.localCache.items():
            try:
                if(key[1] == clientID):
                    toRemoveKeys.append(key)
                    yield kvstore_pb2.CacheEntry(key = key[0], clientID = key[1], value = value[0], timeStamp = str(value[1]))
            except Exception as ex:
                print(ex)
                print("could not migrate entry ", key, ":", value)
        for key in toRemoveKeys:
            del self.cache.localCache[key]
        del self.activeSessionIDs[request.sessionID]
        #self.activeSessionIDs.remove(request.sessionID)

    def bindToServer(self, request, context): #to establish the session for the first time        
        sessionID = request.clientID + "-" + str(time.time())
        self.activeSessionIDs[sessionID] = self.epoch
        #self.activeSessionIDs.add(sessionID) #remove once session shift
        return kvstore_pb2.sToken(clientID = request.clientID, serverID = self.serverID, sessionID = sessionID)
    

    def setValue(self, request, context):
        if(request.token.sessionID not in self.activeSessionIDs.keys()):
            status = True
            if(request.token.serverID == self.serverID or request.token.serverID not in self.neighboringEdgeServers.keys()): #this means that it was my session, but it was garbage collected or source was not in my neighbours
                status = False
            else: #transfer from neighibouring edge node
                status = self.fetchFromNeighbour(request.token.serverID, request.token.clientID, request.token.sessionID)
            
            if(not status): #create a new session and token
                sessionID = request.token.clientID + "-" + str(time.time())
                self.activeSessionIDs[sessionID] = self.epoch #adding this new session to active sessions
                request.token.serverID = self.serverID
                request.token.sessionID = sessionID
            else: #fetch was succesful from the neighbour, now update the current server to self
                request.token.serverID = self.serverID       

        self.activeSessionIDs[request.token.sessionID] = self.epoch

        centralServerResponse = self.writeToCentralServer(request.key, request.value)
        if(centralServerResponse.success):
            currentTime = time.time()          
            self.cache.set((request.key, request.token.clientID), [request.value, currentTime])
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
                self.activeSessionIDs[sessionID] = self.epoch #adding this new session to active sessions
                request.token.serverID = self.serverID
                request.token.sessionID = sessionID
                
            else: #fetch was succesful from the neighbour, now update the current server to self
                request.token.serverID = self.serverID

        
        self.activeSessionIDs[request.token.sessionID] = self.epoch #making the session a part of current epoch

        toReturn = self.cache.get((request.key, request.token.clientID))
        if(not toReturn):
            #fetch from central server
            centralServerResponse = self.readFromCentralServer(request.key)
            currentTime = time.time()
            if(centralServerResponse.value is not None):       
                self.cache.set((request.key, request.token.clientID), [centralServerResponse.value, currentTime])
                toReturn = self.cache.get((request.key, request.token.clientID))
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