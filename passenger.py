import asyncio
import grpc
import coster_pb2_grpc
import coster_pb2

server_address = "localhost:54321"
wagon_address = "192.168.1.73:54322"

class Client:
    def __init__(self, server_address):
        self.server_address = server_address
        self.channel = grpc.aio.insecure_channel(server_address)
        self.stub = coster_pb2_grpc.RollerCoasterStub(self.channel)
        self.seats_per_wagon = 4
        self.passenger_count = 0

    async def subscribe(self, passenger_id):
        try:
            response = await self.stub.Subscribe(coster_pb2.Subscription(topic="boarding", _id=passenger_id))
            if response.value:
                print(f"{passenger_id} subscribed successfully.")
            else:
                print(f"{passenger_id} failed to subscribe.")
        except Exception as e:
            print(f"{passenger_id} Error subscribing, error type: {e}")

    async def unsubscribe(self, passenger_id):
        try:
            response = await self.stub.Unsubscribe(coster_pb2.Subscription(topic="boarding", _id=passenger_id))
            if response.value:
                print(f"{passenger_id} unsubscribed successfully.")
            else:
                print(f"{passenger_id} failed to unsubscribe.")
        except Exception as e:
            print(f"{passenger_id} Error unsubscribing, error type: {e}")

    async def i_am_boarding(self, wagon_address, passenger_id):
        try:
            async with grpc.aio.insecure_channel(wagon_address) as channel:
                wagon_stub = coster_pb2_grpc.WagonStub(channel)
                response = await wagon_stub.Board(coster_pb2.Boarding(topic="boarding", _id=passenger_id))
                if response.value:
                    self.passenger_count += 1
                    print(f"{passenger_id} boarded wagon successfully.")
                    if self.passenger_count == self.seats_per_wagon:
                        await self.i_am_departing(wagon_address, "wagon_1") 
                        self.passenger_count = 0
                else:
                    print(f"{passenger_id} failed to board.")
        except Exception as e:
            print(f"{passenger_id} Error boarding, error type: {e}")

    async def i_am_departing(self, wagon_address, wagon_id):
        try:
            async with grpc.aio.insecure_channel(wagon_address) as channel:
                wagon_stub = coster_pb2_grpc.WagonStub(channel)
                response = await wagon_stub.Depart(coster_pb2.Departure(topic="departure", _id=wagon_id))
                if response.value:
                    print(f"Wagon {wagon_id} departed successfully.")
                else:
                    print(f"Wagon {wagon_id} failed to depart.")
        except Exception as e:
            print(f"Wagon {wagon_id} Error departing, error type: {e}")

    async def i_am_disembarking(self, passenger_id):
        try:
            response = await self.stub.Disembark(coster_pb2.Disembark_(topic="disembarking", _id=passenger_id))
            if response.value:
                print(f"{passenger_id} disembarked successfully.")
            else:
                print(f"{passenger_id} failed to disembark.")
        except Exception as e:
            print(f"{passenger_id} Error disembarking, error type: {e}")

async def main():
    passenger_client = Client(server_address)
    
    while True:
        for num in range(1, 6):
            passenger_id = f"passenger_{num}"
            await passenger_client.subscribe(passenger_id)
            await asyncio.sleep(1)
            await passenger_client.i_am_boarding(wagon_address, passenger_id)
            await asyncio.sleep(1)

        for num in range(1, 6):
            passenger_id = f"passenger_{num}"
            await passenger_client.i_am_disembarking(passenger_id)
            await asyncio.sleep(1)
            await passenger_client.unsubscribe(passenger_id)
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
