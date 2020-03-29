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
        print(kvstore_pb2.ValueRequest(key="key"))
        response = stub.getValue(kvstore_pb2.ValueRequest(key="key"))
        #response = stub.acceptCache((sendItems()))
        #if response.success == True:
        #    print("Cache transferred")
        print(response.key, ":", response.value, ":", response.timeStamp)


if __name__ == '__main__':
	logging.basicConfig()
	run()