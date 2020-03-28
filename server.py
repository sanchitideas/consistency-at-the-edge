from concurrent import futures
import time
import math
import logging

import grpc

import kvstore_pb2
import kvstore_pb2_grpc


class KVStoreServicer(kvstore_pb2_grpc.MultipleValuesServicer):
    """Provides methods that implement functionality of Multiple Values Servicer."""

    #def __init__(self):


    def setValue(self, request, context):
        return kvstore_pb2.SetResponse(key=request.key, success=True)

    def getValue(self, request, context):
        return kvstore_pb2.ValueResponse(key=request.key, value="sample_value")

    def getValuesForKeys(self, request_iterator, context):
        for request in request_iterator:
            yield kvstore_pb2.ValueResponse(key=request.key, value="sample_value")       

    def setValuesForKeys(self, request_iterator, context):
        for request in request_iterator:
            yield kvstore_pb2.SetResponse(key=request.key, success=True)
    def acceptCache(self, request_iterator, context):
        for request in request_iterator:
            print(request.key)
        return kvstore_pb2.SetResponse(key="sample_key", success=True)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_MultipleValuesServicer_to_server(
        KVStoreServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()