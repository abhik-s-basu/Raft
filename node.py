import grpc
import raft_pb2_grpc 
import raft_pb2 
import sys
import random as rnd
from enum import Enum
from concurrent import futures
from threading import Timer, Event, Lock, Thread

ip_port_list = [
    {"ip": "localhost", "port": 50051},
    {"ip": "localhost", "port": 50052},
    {"ip": "localhost", "port": 50053},
    {"ip": "localhost", "port": 50054},
    {"ip": "localhost", "port": 50055},
    # Add more entries as needed
]
class Address:
    def __init__(self, id: int, ip: str, port: int):
        self.id = id
        self.ip = ip
        self.port = port

timer_lock = Event()
term_lock = Lock()
state_lock = Lock()

class State(Enum):
    FOLLOWER = 1, 
    CANDIDATE = 2, 
    LEADER = 3

class Status(Enum):
    RUNNING = 1,
    CRASHED = 2

class Node():
    def __init__(self, id: int, address: Address, neighbours):
        self.status = Status.RUNNING
        self.state = State.FOLLOWER # need to do later 
        self.id = id
        self.address = address
        self.neighbours = neighbours
        self.last_vote_term = -1 # need to do later
        self.voted_for = -1 # need to do later
        self.leader_id = -1
        self.commitIndex = 0 # need to do later
        self.lastApplied = 0
        self.log_table = [] # need to do later
        self.nextIndex = []
        self.matchIndex = []
        self.applied_entries = {}
        self.term= 0
        self.start()
    # helper functions

    def start(self):
        self.timer_init()
        self.timer.start()
    
    def timer_init(self):
        # self.timer_interval = rnd.randint(150,300) / 1000
        self.timer_interval = rnd.randint(5,10) 
        self.timer = Timer(self.timer_interval, self.timer_follower)
    
    def timer_follower(self):
        if self.state == State.FOLLOWER and self.status == Status.RUNNING:
            if self.timer.finished:
                self.become_candidate()
                self.start_election()
    
    def timer_reinit(self):
        # self.timer_interval = rnd.randint(150,300) / 1000
        self.timer_interval = rnd.randint(5,10) 

    def timer_reset(self):
        self.timer.cancel()
        # print(f"{self.timer_interval}")
        self.timer = Timer(self.timer_interval, self.timer_follower)
        self.timer.start()

    def update_terms(self, t):
        with term_lock:    
            self.term = t
    
    def update_term(self):
        with term_lock:
            self.term += 1
    
    def update_state(self, state:State):
        with state_lock:
            self.state = state
    
    def update_vote(self, term):
        if self.last_vote_term < term:
            self.last_vote_term = term
    
    def become_candidate(self):
        self.update_state(State.CANDIDATE)
        self.update_term()
        print(f"Node {self.id} election timer timed out, Starting election")
        self.start_election()
    
    def start_election(self):
        if self.status == Status.CRASHED:
            return
        if self.state != State.CANDIDATE:
            return
        
        requests = []
        votes = [0 for i in range(len(self.neighbours))]
        for i in self.neighbours:
            if i.id == self.id:
                print("Voted for myself")
                votes[i.id] = 1
                pass
            else:
                thread = Thread(target = self.request_votes, args = (i, votes))
                requests.append(thread)
                thread.start()
                # self.request_votes(i, votes)

        for i in requests: 
            i.join()
        
        if self.state != State.CANDIDATE:
            return
        if sum(votes) > len(votes)//2:
            print(f"Node {self.id} becomes leader for term {self.term}")
            self.become_leader()
        else:
            self.update_state(State.FOLLOWER)
            self.timer_reinit()
            self.timer_reset()
    
    def request_votes(self, i:Address, votes):
        if self.status == Status.CRASHED:
            return
        
        channel = grpc.insecure_channel(f"{i.ip}:{i.port}")
        stub = raft_pb2_grpc.RaftStub(channel)
        if self.state != State.CANDIDATE:
            return
        cLogTerm = 0
        if len(self.log_table) > 0:
            cLogTerm = self.log_table[len(self.log_table)-1]['term']

        try:
            request = raft_pb2.VoteRequest(cTerm = self.term,
            cid = self.id,
            cLogLength = len(self.log_table),
            cLogTerm = cLogTerm)

            response = stub.RequestVote(request)
            if response.term > self.term:
                self.update_terms(response.term)
                # print(f"{self.term} hello")
                self.become_follower()
            elif response.status == True and self.term >= response.term:
                # print("Helllloooo")
                votes[i.id] = 1
        except Exception as e: 
            print(e)
    
    def become_follower(self):
        self.update_state(State.FOLLOWER)
        print(f"{self.id} became follower")
        self.timer_reset()
    
    def become_leader(self):
        if self.status == Status.CRASHED:
            return
        print(f"{self.id} became leader")
        self.nextIndex = [len(self.log_table)]* len(self.neighbours)
        self.matchIndex = [0]  * len(self.neighbours)
        self.leader_id = self.id

        if self.state == State.CANDIDATE:
            print("Became Leader")
            self.update_state(State.LEADER)
            # self.heartbeat_timer()
            self.leader_id = self.id

    def send_heartbeat(self, addr):
        if self.status == Status.CRASHED:
            return
        
        channel = grpc.insecure_channel(f"{addr.ip}:{addr.port}")
        stub = raft_pb2_grpc.RaftStub(channel)
        # some if conditions for log replication Akshansh tbd
        request = raft_pb2.LogRequest(leaderID = self.id,leaderTerm = self.term,
                        prefixLen = self.nextIndex[addr.id] - 2,
                        prefixTerm = self.log_table[self.nextIndex[addr.id] - 2]['term'],
                        entries = [],
                    leaderCommit = self.commitIndex)
        response = stub.AppendEntries(request)
        # akshansh tbd more code here
    
    def heartbeat_timer(self):
        if self.status == Status.CRASHED:
            return
        if self.state != State.LEADER:
            return
        pool = []
        for n in self.neighbours:
            if n.id != NODE_ADDR.id:
                thread = Thread(target = self.send_heartbeat, args = (n,))
                thread.start()
                pool.append(thread)
        
        for t in pool:
            t.join()

        self.leader_timer = Timer(1, self.heartbeat_timer)
        self.leader_timer.start() # leader lease comes here

class RaftHandler(raft_pb2_grpc.RaftServicer, Node):
    def __init__(self, id:int, address: Address, neighbours):
        super().__init__(id, address,  neighbours)
        print(f"The server starts at {address.ip}:{address.port}")

    def RequestVote(self, request, context):
        cTerm = request.cTerm
        cid = request.cid
        cLogLength = request.cLogLength
        cLogTerm = request.cLogTerm

        if self.status == Status.CRASHED:
            return raft_pb2.VoteResponse(status = False, term = self.term, nodeID = cid)
        
        if cTerm > self.term:
            self.update_terms(cTerm)
            self.become_follower()
            self.voted_for = -1
        
        lastTerm = 0
        if len(self.log_table) > 0:
            lastTerm = self.log_table[len(self.log_table)-1]['term']
        logOk = (cLogTerm > lastTerm) or (cLogTerm == lastTerm and cLogLength >= len(self.log_table))
        # print(self.voted_for)
        if cTerm == self.term and logOk and (self.voted_for == -1 or self.voted_for == cid):
            self.voted_for = cid
            print(f"Voted for {cid} in term {cTerm}")
            return raft_pb2.VoteResponse(status = True, term = self.term, nodeID = cid)
        else:
            print(f"Voted denied for {cid} in term {cTerm}")
            return raft_pb2.VoteResponse(status = False, term = self.term, nodeID = cid)


        

def run(handler: RaftHandler):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(
        handler, server
    )
    server.add_insecure_port(f'[::]:{handler.address.port}')
    # print(f"Server has been started with address {handler.address}")
    server.start()
    server.wait_for_termination()

# def run2():
#     with grpc.insecure_channel('localhost:50051') as channel:
#         stub = raft_pb2_grpc.RaftStub(channel)
#         # print("test")
#         response = stub.RequestVote(raft_pb2.VoteRequest(cid=1, cTerm=2, cLogLength=3, cLogTerm=4))
#         print("Greeter client received: " + str(response.cid) + " ")

if __name__ == "__main__":
    global NODE_ADDR
    neighbours = []
    id = int(sys.argv[1])
    address = None
    with open('config.conf') as conf:
        while s := conf.readline():
            n_id, *n_address = s.split()
            if int(n_id) == id:
                address = Address(int(n_id), n_address[0], int(n_address[1]))
                NODE_ADDR = address

            n_ip = n_address[0]
            n_port = int(n_address[1])
            neighbours.append(Address(int(n_id), n_ip, n_port))
    run(RaftHandler(id, address, neighbours))
