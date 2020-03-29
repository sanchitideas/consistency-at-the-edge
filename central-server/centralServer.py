from concurrent import futures
import logging
import redis
import grpc

import centralserver_pb2
import centralserver_pb2_grpc


class CentralServerServicer(centralserver_pb2_grpc.CentralServerServicer):
	"""Provides methods that implement functionality of CSServicer."""
	redisInstance = redis.Redis(host='localhost', port=6379, db=0)

	def setValue(self, request, context):
		try:
			self.redisInstance.set(request.key, request.value)
			return centralserver_pb2.SetResponse(success=True)
		except redis.ConnectionError:
			return centralserver_pb2.SetResponse(success=False)

	def setValuesForKeys(self, request_iterator, context):
		for request in request_iterator:
			try:
				self.redisInstance.set(request.key, request.value)
				yield centralserver_pb2.SetResponse(success=True)
			except redis.ConnectionError:
				yield centralserver_pb2.SetResponse(success=False)

	def getValue(self, request, context):
		try:
			val = self.redisInstance.get(request.key)
			if val == None:
				# we won't face this issue
				return centralserver_pb2.ValueResponse(value="sample_value")
			else:
				return centralserver_pb2.ValueResponse(value=val.decode())
		except redis.ConnectionError:
			return centralserver_pb2.ValueResponse(value="could_not_connect")

	def getValuesForKeys(self, request_iterator, context):
		for request in request_iterator:
			try:
				val = self.redisInstance.get(request.key)
				if val == None:
					yield centralserver_pb2.ValueResponse(value="sample_value")
				else:
					yield centralserver_pb2.ValueResponse(value=val.decode())
			except redis.ConnectionError:
				yield centralserver_pb2.ValueResponse(value="could_not_connect")


def serve():
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	centralserver_pb2_grpc.add_CentralServerServicer_to_server(
		CentralServerServicer(), server)
	server.add_insecure_port('[::]:50051')
	server.start()
	server.wait_for_termination()


if __name__ == '__main__':
	logging.basicConfig()
	serve()