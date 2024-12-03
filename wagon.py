import asyncio
import grpc
import coster_pb2_grpc
import coster_pb2

class WagonService(coster_pb2_grpc.WagonServicer):
    def __init__(self):
        self.passenger_queue = asyncio.Queue()
        self.seats = 4

    async def Board(self, request, context):
        if self.passenger_queue.qsize() < self.seats:
            await self.passenger_queue.put(request._id)
            print(f"Passenger {request._id} is boarding for {request.topic}")
            return coster_pb2.Ack(value=True)
        else:
            print(f"Wagon is full, sending passenger {request._id} to next wagon")
            return coster_pb2.Ack(value=False)

    async def Depart(self, request, context):
        passengers = []
        while not self.passenger_queue.empty():
            passenger_id = await self.passenger_queue.get()
            passengers.append(passenger_id)
        print(f"Wagon {request._id} is departing with passengers: {passengers}")
        await asyncio.sleep(5)
        return coster_pb2.Ack(value=True)

async def listen(port) -> None:
    server = grpc.aio.server()
    coster_pb2_grpc.add_WagonServicer_to_server(WagonService(), server)
    listen_addr = f'[::]:{port}'
    server.add_insecure_port(listen_addr)
    await server.start()
    await server.wait_for_termination()

if __name__ == '__main__':
    asyncio.run(listen(54322))