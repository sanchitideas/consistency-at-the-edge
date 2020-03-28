from __future__ import print_function
import logging

import grpc

import kvstore_pb2
import kvstore_pb2_grpc


def sendItems():
	dictionary = {"first_key": "first_value", "second_key": "second_value"}
	for k,v in dictionary.items():
		yield kvstore_pb2.SetRequest(key=k, value=v)


def run():
	with grpc.insecure_channel('localhost:50051') as channel:
		stub = kvstore_pb2_grpc.MultipleValuesStub(channel)
		#response = stub.getValue(kvstore_pb2.ValueRequest(key="key"))
		response = stub.acceptCache((sendItems()))
		if response.success == True:
			print("Cache transferred")


if __name__ == '__main__':
	logging.basicConfig()
	run()