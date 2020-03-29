from concurrent import futures
import time
import math
import logging

import grpc

import kvstore_pb2
import kvstore_pb2_grpc

import centralserver_pb2
import centralserver_pb2_grpc

import collections


class LRUCache:

    # @param capacity, an integer
    def __init__(self, capacity): #capacity- number of rows allowed
        self.capacity = capacity
        self.forwardTable = collections.OrderedDict()

    # @return an integer
    def get(self, key):
        if not key in self.forwardTable:
            return None
        value = self.forwardTable.pop(key)
        self.forwardTable[key] = value
        return value

    # @param key, an integer
    # @param value, an integer
    # @return nothing
    def set(self, key, value):
        if key in self.forwardTable:
            self.forwardTable.pop(key)
        elif len(self.forwardTable) == self.capacity:
            self.forwardTable.popitem(last=False)
        self.forwardTable[key] = value


class KVStoreServicer(kvstore_pb2_grpc.MultipleValuesServicer):
    """Provides methods that implement functionality of Multiple Values Servicer."""

    def __init__(self, serverID):
        self.cache = LRUCache(100) #key - tuple(client specific key, clientID), Value - List (value, timestamp)
        self.serverID = serverID
        self.activeSessionIDs = set()
        self.centralServerConn = self.connectCentralServer()

    
    def connectCentralServer(self):
        channel = grpc.insecure_channel('localhost:50050')         
        return centralserver_pb2_grpc.CentralServerStub(channel)
    
    def writeToCentralServer(self, key, value):
        return self.centralServerConn.setValue(centralserver_pb2.CentralServerSetRequest(key=key, value = value))
    
    def readFromCentralServer(self, key):
        return self.centralServerConn.getValue(centralserver_pb2.CentralServerValueRequest(key=key))
        

    def bindToServer(self, request, context): #to establish the session for the first time        
        sessionID = request.clientID + "-" + str(time.time())
        self.activeSessionIDs.add(sessionID) #remove once session shift
        return kvstore_pb2.sToken(clientID = request.clientID, serverID = self.serverID, sessionID = sessionID)
    

    def setValue(self, request, context):
        if(request.token.sessionID not in self.activeSessionIDs):
            #transfer from neighibouring edge node
            pass
        centralServerResponse = self.writeToCentralServer(request.key, request.value)
        if(centralServerResponse.success):
            currentTime = time.time()          
            self.cache.set((request.key, request.token.clientID), [request.value, currentTime])
            #request.token.sessionID = "server"
            return kvstore_pb2.SetResponse(key=request.key, success=True, token = request.token)
        else:
            return kvstore_pb2.SetResponse(key=request.key, success=False, token = request.token)

    def getValue(self, request, context):
        if(request.token.sessionID not in self.activeSessionIDs):
            #transfer from neighibouring edge node
            pass  
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

    
    # def getValuesForKeys(self, request_iterator, context):
    #     for request in request_iterator:
    #         yield kvstore_pb2.ValueResponse(key=request.key, value="sample_value")       

    # def setValuesForKeys(self, request_iterator, context):
    #     for request in request_iterator:
    #         yield kvstore_pb2.SetResponse(key=request.key, success=True)
            
    def acceptCache(self, request_iterator, context):
        for request in request_iterator:
            print(request.key)
        return kvstore_pb2.SetResponse(key="sample_key", success=True)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_MultipleValuesServicer_to_server(
        KVStoreServicer("server1"), server)
    print('Starting server. Listening on port 50051.')
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()