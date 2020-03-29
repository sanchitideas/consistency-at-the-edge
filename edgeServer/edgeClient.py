from __future__ import print_function
import logging

import grpc
import time

import kvstore_pb2
import kvstore_pb2_grpc


def sendItems():
	dictionary = {"first_key": "first_value", "second_key": "second_value"}
	for k,v in dictionary.items():
		yield kvstore_pb2.SetRequest(key=k, value=v, timeStamp = time.time())


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = kvstore_pb2_grpc.MultipleValuesStub(channel)
        sToken = stub.bindToServer(kvstore_pb2.bindRequest(clientID = "client1"))
        while(1):
            cmd = input("Enter command: ")
            cmd = cmd.split()
            if(len(cmd) > 3 or len(cmd) < 2):
                print("Usage: <0/1> <key> <value(optional)>")
                continue
            if(cmd[0] == '0'): #0 -set
                response = stub.setValue(kvstore_pb2.SetRequest(key=cmd[1], value = cmd[2], token = sToken))
                sToken = response.token
                print(response.key, ":", response.success)
            else:
                response = stub.getValue(kvstore_pb2.ValueRequest(key=cmd[1], token = sToken))
                sToken = response.token
                print(response.key, ":", response.value)


if __name__ == '__main__':
	logging.basicConfig()
	run()