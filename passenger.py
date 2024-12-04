import asyncio
import grpc
import coster_pb2_grpc
import coster_pb2

server_address = "localhost:54321"
wagon_addresses = ["0.0.0.0:54322", "0.0.0.00:54323"] #"192.168.1.57:54322", "192.168.1.73:54323"

class Client:
    def __init__(self, server_address, wagon_addresses):
        self.server_address = server_address
        self.wagon_addresses = wagon_addresses
        self.current_wagon_index = 0

    async def subscribe(self, passenger_id):
        async with grpc.aio.insecure_channel(self.server_address) as channel:
            stub = coster_pb2_grpc.RollerCoasterStub(channel)
            try:
                response = await stub.Subscribe(coster_pb2.Subscription(topic="boarding", _id=passenger_id))
                if response.value:
                    print(f"{passenger_id} subscribed successfully.")
                else:
                    print(f"{passenger_id} failed to subscribe.")
            except Exception as e:
                print(f"{passenger_id} Error subscribing, error type: {e}")

    async def unsubscribe(self, passenger_id):
        async with grpc.aio.insecure_channel(self.server_address) as channel:
            stub = coster_pb2_grpc.RollerCoasterStub(channel)
            try:
                response = await stub.Unsubscribe(coster_pb2.Subscription(topic="boarding", _id=passenger_id))
                if response.value:
                    print(f"{passenger_id} unsubscribed successfully.")
                else:
                    print(f"{passenger_id} failed to unsubscribe.")
            except Exception as e:
                print(f"{passenger_id} Error unsubscribing, error type: {e}")

    async def i_am_boarding(self, passenger_id):
        attempts = 0
        while attempts < len(self.wagon_addresses):
            wagon_address = self.wagon_addresses[self.current_wagon_index]
            async with grpc.aio.insecure_channel(wagon_address) as channel:
                wagon_stub = coster_pb2_grpc.WagonStub(channel)
                try:
                    response = await wagon_stub.Board(coster_pb2.Boarding(topic="boarding", _id=passenger_id))
                    if response.value:
                        print(f"{passenger_id} boarded wagon at {wagon_address} successfully.")
                        return
                    else:
                        print(f"{wagon_address} is full, switching to the next wagon.")
                        self.current_wagon_index = (self.current_wagon_index + 1) % len(self.wagon_addresses)
                        attempts += 1
                except Exception as e:
                    print(f"{passenger_id} Error boarding, error type: {e}")
                    break
        print(f"All wagons are full, passenger {passenger_id} cannot board at this time.")

    async def i_am_disembarking(self, passenger_id):
        async with grpc.aio.insecure_channel(self.server_address) as channel:
            stub = coster_pb2_grpc.RollerCoasterStub(channel)
            try:
                response = await stub.Disembark(coster_pb2.Disembark_(topic="disembarking", _id=passenger_id))
                if response.value:
                    print(f"{passenger_id} disembarked successfully.")
                else:
                    print(f"{passenger_id} failed to disembark.")
            except Exception as e:
                print(f"{passenger_id} Error disembarking, error type: {e}")

async def main():
    passenger_client = Client(server_address, wagon_addresses)

    while True:
        for num in range(1, 10):
            passenger_id = f"passenger_{num}"
            await passenger_client.subscribe(passenger_id)
            await asyncio.sleep(1)
            await passenger_client.i_am_boarding(passenger_id)
            await asyncio.sleep(1)

        await asyncio.sleep(5)  # Simulate ride duration

        for num in range(1, 10):
            passenger_id = f"passenger_{num}"
            await passenger_client.i_am_disembarking(passenger_id)
            await asyncio.sleep(1)
            await passenger_client.unsubscribe(passenger_id)
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
