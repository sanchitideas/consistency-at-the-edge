from __future__ import print_function
import logging

import grpc
import time
import sys

import kvstore_pb2
import kvstore_pb2_grpc


def sendItems():
	dictionary = {"first_key": "first_value", "second_key": "second_value"}
	for k,v in dictionary.items():
		yield kvstore_pb2.SetRequest(key=k, value=v, timeStamp = time.time())

server1 = 'localhost:50051'
server2 = 'localhost:50052'

def run(clientID):
    server = server1
    isSet = False
    while(1):
        with grpc.insecure_channel(server) as channel:
            stub = kvstore_pb2_grpc.MultipleValuesStub(channel)
            if(not isSet):
                sToken = stub.bindToServer(kvstore_pb2.bindRequest(clientID = clientID))
                isSet = True
            while(1):
                cmd = input("Enter command: ")
                cmd = cmd.split()
                if(len(cmd) > 3):
                    print("Usage: <0/1> <key> <value(optional)>")
                    continue
                if(cmd[0] == '0'): #0 -set
                    response = stub.setValue(kvstore_pb2.SetRequest(key=cmd[1], value = cmd[2], token = sToken))
                    sToken = response.token
                    print(response.key, ":", response.success)
                elif(cmd[0] == '1'):
                    response = stub.getValue(kvstore_pb2.ValueRequest(key=cmd[1], token = sToken))
                    sToken = response.token
                    print(response.key, ":", response.value)
                elif(cmd[0] == '2'):
                    server = server2
                    print("breaking connection and starting with next edge server")
                    break



if __name__ == '__main__':
    if(len(sys.argv) != 2):
        print("Usage: python3 edgeClient.py <clientID>")
        exit(1)
    logging.basicConfig()
    run(sys.argv[1])