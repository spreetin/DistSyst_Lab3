import asyncio
import grpc
import coster_pb2_grpc
import coster_pb2

passenger_id = "passenger_1"
server_address = "192.168.1.73:54321" # change 192.168.1.73 to dedicated server
wagon_address = "localhost:54322"

class Client:
    def __init__(self, server_address):
        self.server_address = server_address
        self.channel = grpc.aio.insecure_channel(server_address)
        self.stub = coster_pb2_grpc.RollerCoasterStub(self.channel)

    async def subscribe(self, passenger_id):
        try:
            response = await self.stub.Subscribe(coster_pb2.Subscription(topic="boarding", _id=passenger_id))
            if response.value:
                print(f"{passenger_id} subscribed successfully.")
            else:
                print(f"{passenger_id} Failed to subscribe.")
        except Exception as e:
            print(f"{passenger_id} Error subscribing, error type: {e}")

    async def unsubscribe(self, passenger_id):
        try:
            response = await self.stub.Unsubscribe(coster_pb2.Subscription(topic="boarding", _id=passenger_id))
            if response.value:
                print(f"{passenger_id} unsubscribed successfully.")
            else:
                print(f"{passenger_id} Failed to unsubscribe.")
        except Exception as e:
            print(f"{passenger_id} Error unsubscribing, error type: {e}")

    async def board_wagon(self, wagon_address, passenger_id):
        try:
            async with grpc.aio.insecure_channel(wagon_address) as channel:
                wagon_stub = coster_pb2_grpc.WagonStub(channel)
                response = await wagon_stub.Board(coster_pb2.Boarding(topic="boarding", _id=passenger_id))
                if response.value:
                    print(f"{passenger_id} Boarded wagon successfully.")
                else:
                    print(f"{passenger_id} Failed to board.")
        except Exception as e:
            print(f"{passenger_id} Error boarding, error type: {e}")

    async def disembark(self, passenger_id):
        try:
            response = await self.stub.Disembark(coster_pb2.Disembark_(topic="disembarking", _id=passenger_id))
            if response.value:
                print(f"{passenger_id} Disembarked successfully.")
            else:
                print(f"{passenger_id} Failed to disembark.")
        except Exception as e:
            print(f"{passenger_id} Error disembarking, error type: {e}")

async def main():
    #server = grpc.aio.server()
    #coster_pb2_grpc.add_ReceiverServicer_to_server(Client(), server)

    passenger_client = Client(server_address)

    #testing the implementation 
    await passenger_client.subscribe(passenger_id)
    await asyncio.sleep(2)
    await passenger_client.board_wagon(wagon_address, passenger_id)
    await asyncio.sleep(5)
    await passenger_client.disembark(passenger_id)
    await passenger_client.unsubscribe(passenger_id)

if __name__ == "__main__":
    asyncio.run(main())
