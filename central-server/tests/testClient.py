from __future__ import print_function
import logging

import grpc

import centralserver_pb2
import centralserver_pb2_grpc


def sendItems():
	dictionary = {"foo": "bar", "loo": "poo"}
	for k,v in dictionary.items():
		yield centralserver_pb2.SetRequest(key=k, value=v)


def run():
	with grpc.insecure_channel('localhost:50051') as channel:
		stub = centralserver_pb2_grpc.CentralServerStub(channel)
		stub.setValuesForKeys((sendItems()))
		response = stub.getValue(centralserver_pb2.ValueRequest(key="foo"))
		#response = stub.acceptCache((sendItems()))
		#if response.success == True:
			#print("Cache transferred")
		print(response.value)


if __name__ == '__main__':
	logging.basicConfig()
	run()