import grpc
from concurrent import futures
import client_pb2
import client_pb2_grpc
import random


ip_port_list = [
    {"ip": "[::]", "port": 50051},
    {"ip": "[::]", "port": 50052},
    {"ip": "[::]", "port": 50053},
    {"ip": "[::]", "port": 50054},
    {"ip": "[::]", "port": 50055},
]

class Client():
    def __init__(self):
        self.node_list = ip_port_list
        self.curr_leader_idx = -1
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.port = self.server.add_insecure_port("[::]:0")
        self.ip = "[::]"

    def set(self, k, v):
        if self.curr_leader_idx == -1:
            self.curr_leader_idx = random_int = random.randint(0, 4)
        self.channel = grpc.insecure_channel(f"{self.node_list[self.curr_leader_idx].ip}:{self.node_list[self.curr_leader_idx].ip}")
        self.stub = client_pb2_grpc.ClientStub(self.channel)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        request = client_pb2.SetRequest(key = k, value = v)
        # response = self.stub. yaha dekhna hoga

    def get(self, k):
        if self.curr_leader_idx == -1:
            self.curr_leader_idx = random_int = random.randint(0, 4)
        self.channel = grpc.insecure_channel(f"{self.node_list[self.curr_leader_idx].ip}:{self.node_list[self.curr_leader_idx].ip}")
        self.stub = client_pb2_grpc.ClientStub(self.channel)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        request = client_pb2.SetRequest(key = k, value = v)
        # response = self.stub. yaha dekhna hoga

if __name__ == "__main__":
    client = Client()
    while(True):
        print("What do you want to do? \n 1. Set a value \n 2. Get a value \n ")
        n = int(input())
        if n == 1:
            k = input("Enter key: ")
            v = input("Enter value: ")
            client.set(k, v)
        elif n == 2:
            k = input("Enter key: ")
            client.get(k)
        else: 
            print("Invalid input")