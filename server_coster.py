import asyncio
import grpc
import coster_pb2_grpc
import coster_pb2

class RollerCoasterServer(coster_pb2_grpc.RollerCoasterServicer):
    def __init__(self):
        self.passenger_queue = asyncio.Queue()
        self.wagon_queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(1)
        self.seats_per_wagon = 4

    async def Subscribe(self, request, context):
        async with self.semaphore:
            await self.passenger_queue.put(request._id)
            print(f"{request._id} subscribed to {request.topic}")
        return coster_pb2.Ack(value=True)

    async def Unsubscribe(self, request, context):
        async with self.semaphore:
            new_queue = asyncio.Queue()
            while not self.passenger_queue.empty():
                passenger_id = await self.passenger_queue.get()
                if passenger_id != request._id:
                    await new_queue.put(passenger_id)
            self.passenger_queue = new_queue
            print(f"{request._id} unsubscribed from {request.topic}")
        return coster_pb2.Ack(value=True)

    async def Notify(self, request, context):
        async with self.semaphore:
            print(f"Notification received: {request.message}")
        return coster_pb2.Ack(value=True)

    async def Disembark(self, request, context):
        async with self.semaphore:
            print(f"{request._id} is disembarking from {request.topic}")
        return coster_pb2.Ack(value=True)
    
    # Maybe change to event based instead
    async def handle_boarding_and_departure(self):
        while True:
            async with self.semaphore:
                if not self.wagon_queue.empty() and self.passenger_queue.qsize() >= self.seats_per_wagon:
                    wagon_id = await self.wagon_queue.get()
                    passengers = []

                    for _ in range(self.seats_per_wagon):
                        passenger_id = await self.passenger_queue.get()
                        passengers.append(passenger_id)

                    print(f"Wagon {wagon_id} departing with passengers: {passengers}")
                    await asyncio.sleep(5)

                    for passenger_id in passengers:
                        response = await self.Disembark(coster_pb2.Disembark_(topic="disembarking", _id=passenger_id))
                        if response.value:
                            print(f"Passenger {passenger_id} disembarked.")
                        else:
                            print(f"Failed to disembark {passenger_id}.")

                    print(f"Wagon {wagon_id} ride completed.")
                else:
                    await asyncio.sleep(1) 

    async def Depart(self, request, context):
        async with self.semaphore:
            await self.wagon_queue.put(request._id)
            print(f"Wagon {request._id} ready to depart for {request.topic}")
        return coster_pb2.Ack(value=True)

async def listen() -> None:
    server = grpc.aio.server()
    roller_coaster_server = RollerCoasterServer()
    coster_pb2_grpc.add_RollerCoasterServicer_to_server(roller_coaster_server, server)
    listen_addr = '[::]:54321'
    server.add_insecure_port(listen_addr)
    await server.start()
    await server.wait_for_termination()

if __name__ == '__main__':
    asyncio.run(listen())

