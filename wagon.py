import asyncio
import grpc
import coster_pb2_grpc
import coster_pb2

class WagonService(coster_pb2_grpc.WagonServicer):
    async def Board(self, request, context):
        print(f"Passenger {request._id} is boarding for {request.topic}")
        return coster_pb2.Ack(value=True)

    async def Depart(self, request, context):
        print(f"Wagon {request._id} is departing for {request.topic}")
        return coster_pb2.Ack(value=True)

async def listen() -> None:
    server = grpc.aio.server()
    coster_pb2_grpc.add_WagonServicer_to_server(WagonService(), server)
    listen_addr = '[::]:54322'
    server.add_insecure_port(listen_addr)
    await server.start()
    await server.wait_for_termination()

if __name__ == '__main__':
    asyncio.run(listen())
