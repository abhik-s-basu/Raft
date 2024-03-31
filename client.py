import grpc
from concurrent import futures
import raft_pb2
import raft_pb2_grpc
import random
from addr import *



class Client():
    def __init__(self, nodeList):
        self.node_list = nodeList
        self.curr_leader = -1

    def set(self, k, v):
        if self.curr_leader == -1:
            self.curr_leader = random.randint(0, 4)
        channel = grpc.insecure_channel(f"{self.node_list[self.curr_leader].ip}:{self.node_list[self.curr_leader].port}")
        print(f"{self.node_list[self.curr_leader].ip}:{self.node_list[self.curr_leader].port}")
        
        try:
            stub = raft_pb2_grpc.RaftStub(channel)
            # server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            print(self.curr_leader)
            request = raft_pb2.SetRequest(key = k, value = v)
            response = stub.SetValue(request)
            self.curr_leader = response.leaderID
            if response.status == True: 
                print(f"Key: {k} given the value {v}")
            else:
                print(f"Requested to the wrong node, resending to new leader")
                self.set(k,v)
        except Exception as e:
            print("Try again")
            self.curr_leader = -1
            print(self.curr_leader)
            pass

    def get(self, k):
        if self.curr_leader == -1:
            self.curr_leader = random_int = random.randint(0, 4)
        channel = grpc.insecure_channel(f"{self.node_list[self.curr_leader].ip}:{self.node_list[self.curr_leader].port}")
        stub = raft_pb2_grpc.RaftStub(channel)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        request = raft_pb2.GetRequest(key = k)
        # print(self.curr_leader)
        try:
            response = stub.GetValue(request)
            self.curr_leader = response.leaderID
            if response.status == True: 
                print(f"Key: {k} has the value {response.data}")
            else:
                # print(f"Requested at the wrong node")
                self.get(k)
        except Exception as e:
            print("Try again")
            self.curr_leader = -1
            # self.curr_leader = random.randint(0, 4)
            # self.get(k,v)

if __name__ == "__main__":
    nodes = []
    with open('config.conf') as conf:
        while s := conf.readline():
            n_id, *n_address = s.split()
            address = Address(int(n_id), n_address[0], int(n_address[1]))
            nodes.append(address)
    client = Client(nodes)
    while(True):
        print("What do you want to do? \n 1. Set a value \n 2. Get a value \n ")
        try:
            n = int(input())
            if n == 1:
                k = str(input("Enter key: "))
                v = str(input("Enter value: "))
                client.set(k, v)
            elif n == 2:
                k = str(input("Enter key: "))
                client.get(k)
            else:
                print("Invalid input")
        except:
            print("Invalid choice")