import grpc
import raft_pb2_grpc 
import raft_pb2 
import sys
import math
import random as rnd
from addr import *
from enum import Enum
from concurrent import futures
from threading import Timer, Event, Lock, Thread
import os 

ip_port_list = [
    {"ip": "localhost", "port": 50051},
    {"ip": "localhost", "port": 50052},
    {"ip": "localhost", "port": 50053},
    {"ip": "localhost", "port": 50054},
    {"ip": "localhost", "port": 50055},
    # Add more entries as needed
]

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
        self.dump_file_path = f"./logs_node_{self.id}/dump.txt"
        self.log_file_path = f"./logs_node_{self.id}/logs.txt"
        self.term= 0
        self.start()
    # helper functions
    def write_to_dump(self, message):
        with open(self.dump_file_path, 'a') as dump_file:
            dump_file.write(message + '\n')
            dump_file.flush()
    
    def write_to_logs(self, term, op, key, value):
        dump_text = f"{op} {key} {value} {term}"
        with open(self.log_file_path, 'a') as log_file: 
            log_file.write(dump_text + '\n')
            log_file.flush()
    def print_and_write(self, message):
        print(message)
        self.write_to_dump(message)

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
        # print(f"{self.id} timer got reset")
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
        # print(f"Node {self.id} election timer timed out, Starting election")
        self.print_and_write(f"Node {self.id} election timer timed out, Starting election")
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
                self.print_and_write("Voted for myself")
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
            self.print_and_write(f"Node {self.id} becomes leader for term {self.term}")
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
            pass
            # print(e)
    
    def become_follower(self):
        self.update_state(State.FOLLOWER)
        # print(f"{self.id} became follower")
        self.timer_reset()
    
    def become_leader(self):
        if self.status == Status.CRASHED:
            return
        # self.nextIndex = [len(self.log_table)]* len(self.neighbours)
        # self.matchIndex = [0]  * len(self.neighbours)
        # self.leader_id = self.id

        if self.state == State.CANDIDATE:
            self.print_and_write(f"Node {self.id} Became Leader")
            self.update_state(State.LEADER)
            print(self.term)
            entry = {
                'term': self.term,
                'update': ['NO OP', "", ""]
            }
            self.log_table.append(entry)
            self.nextIndex = [len(self.log_table)]* len(self.neighbours)
            self.matchIndex = [0]  * len(self.neighbours)
            self.leader_id = self.id
            # print("Test1")
            self.heartbeat_timer()
            # print("Test2")

    def send_heartbeat(self, addr):
        if self.status == Status.CRASHED:
            return
        
        channel = grpc.insecure_channel(f"{addr.ip}:{addr.port}")
        stub = raft_pb2_grpc.RaftStub(channel)
        request= {}

        # Log Replication

        prefixLen= self.nextIndex[addr.id]
        prefixTerm= 0
        print(prefixLen, prefixTerm)
        if prefixLen>0:
            prefixTerm= self.log_table[prefixLen-1]['term']
        print(prefixTerm)

        appending_entries= self.log_table[prefixLen:]

        if self.nextIndex[addr.id]<=len(self.log_table):
            request = raft_pb2.LogRequest(leaderID = self.id,leaderTerm = self.term,
                            prefixLen = prefixLen,
                            prefixTerm = prefixTerm,
                            entries = appending_entries,
                            leaderCommit = self.commitIndex)

        else:
            request = raft_pb2.LogRequest(leaderID = self.id,leaderTerm = self.term,
                            prefixLen = prefixLen,
                            prefixTerm = prefixTerm,
                            entries = [],
                            leaderCommit = self.commitIndex)

        try:
            # self.print_and_write(f"Sending heartbeat for {self.term}")
            response = stub.AppendEntries(request)
            self.print_and_write(f"Term of {response.id}is {response.term}")
            if response.term==self.term and self.state==State.LEADER:
                if response.status==True and response.ack>=self.matchIndex[response.nodeID]:
                    self.nextIndex[response.nodeID]= response.ack
                    self.matchIndex= response.ack
                
                elif self.nextIndex[response.nodeID]>0:
                    self.nextIndex[response.nodeID]= self.nextIndex[response.nodeID]-1
                    self.send_heartbeat(addr)
            elif response.term>self.term:
                self.update_term= response.term
                self.voted_for= -1
                self.become_follower()
        except:
            pass
    
    def heartbeat_timer(self):
        if self.status == Status.CRASHED:
            return
        if self.state != State.LEADER:
            return
        pool = []
        # print("Test3")
        majority= len(self.neighbours)//2
        counter= 0

        for n in self.neighbours:
            if n.id != NODE_ADDR.id:
                thread = Thread(target = self.send_heartbeat, args = (n,))
                thread.start()
                pool.append(thread)
        
        for t in pool:
            t.join()
        
        acks = [0]* len(self.log_table)
        print(acks)
        print(self.matchIndex)
        for i in range(len(acks)):
            for j in self.neighbours:
                if self.matchIndex[j.id] >= i:
                    acks[i] += 1
        print(acks)
        ready = -1
        for i in range(len(acks)):
            if acks[i] > majority:
                ready += 1
        if ready != -1 and ready > self.commitIndex:
            for i in range(self.commitIndex, ready,1):
                key = self.log_table[i]['update'][1]
                value = self.log_table[i]['update'][2]
                self.applied_entries[key] = value
                self.write_to_logs(self.log_table[i]['term'], self.log_table[i]['update'][0], 
                                    self.log_table[i]['update'][1], self.log_table[i]['update'][2])
        
        self.commitIndex = ready
        self.lastApplied = ready-1


        # self.leader_timer.cancel()
        self.leader_timer = Timer(3, self.heartbeat_timer)
        self.leader_timer.start() # leader lease comes here

class RaftHandler(raft_pb2_grpc.RaftServicer, Node):
    def __init__(self, id:int, address: Address, neighbours):
        super().__init__(id, address,  neighbours)
        self.print_and_write(f"The server starts at {address.ip}:{address.port}")

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
            self.print_and_write(f"Voted for {cid} in term {cTerm}")
            return raft_pb2.VoteResponse(status = True, term = self.term, nodeID = cid)
        else:
            self.print_and_write(f"Voted denied for {cid} in term {cTerm}")
            return raft_pb2.VoteResponse(status = False, term = self.term, nodeID = cid)

    def AppendEntries(self, request, context):
        if self.status== Status.CRASHED:
            return raft_pb2.AppendEntriesResponse(status= False, ack = 0, nodeID = self.id, term=self.term)
        # print("here 1")
        leaderID= request.leaderID
        term= request.leaderTerm
        prefixLen= request.prefixLen
        prefixTerm= request.prefixTerm
        leaderCommit= request.leaderCommit
        suffix= request.entries
        self.print_and_write(f"{term} and {self.term}")
        if term>self.term:
            self.update_term(term)
            self.voted_for= -1
            self.become_follower()
            # self.timer_reset()  # check it once
        elif term==self.term:
            # self.state = State.FOLLOWER
            self.become_follower()
            # self.timer_reset()  # check it once
            self.leader_id= leaderID
        
        # print("here 2")
        
        logOk= (len(self.log_table)>prefixLen) and (prefixLen==0 or self.log_table[prefixLen-1]['term']==prefixTerm)
        print(logOk)
        if term==self.term and logOk:
            self.actualAppendEntries(prefixLen, leaderCommit, suffix)
            ack= prefixLen+len(suffix)
            # print("Hello")
            return raft_pb2.AppendEntriesResponse(status= True, ack= ack, nodeID= self.id, term= self.term)
        else:
            # print("hellllloooo")
            return raft_pb2.AppendEntriesResponse(status= False, ack= 0, nodeID= self.id, term= self.term)
        

    def actualAppendEntries(self, prefixLen, leaderCommit, suffix):
        suffixLen= len(suffix)
        logTableLen= len(self.log_table)

        if suffixLen>0 and logTableLen>prefixLen:
            index= math.min(logTableLen, prefixLen+suffixLen)-1

            if self.log_table[index]['term']!=suffix[index-prefixLen]['term']:
                self.log_table= self.log_table[:prefixLen]
        
        if prefixLen+suffixLen>logTableLen:
            for i in range(logTableLen-prefixLen, suffixLen):
                print("appending to followers")
                self.log_table.append(suffix[i])
        
        if leaderCommit>self.commitIndex:
            self.commitIndex= math.min(leaderCommit, len(self.log_table)-1)
            while self.commitIndex>self.lastApplied:
                key= self.log_table[self.lastApplied]['update'][1]
                value= self.log_table[self.lastApplied]['update'][2]
                self.applied_entries[key]= value
                self.lastApplied+= 1
            self.commitIndex= leaderCommit

    def SetValue(self, request, context):
        key = request.key
        value = request.value
        if self.state == State.LEADER:
            entry = {
                'term': self.term,
                'update': ['set', key, value]
            }
            self.log_table.append(entry)
            print(len(self.log_table))
            response = {
                'success': True
            }
            return raft_pb2.ClientResponse(status = True, leaderID = self.id, data = key)
        else:
            return raft_pb2.ClientResponse(status = False, leaderID = self.leader_id, data = key)


    def GetValue(self, request, context):
        key = request.key
        print(key)
        print(self.state)
        print(self.applied_entries)
        if self.state == State.LEADER:
            if self.applied_entries[key]:
                response = {
                    'success': True,
                    'value': self.applied_entries[key]
                }
                return raft_pb2.ClientResponse(status = True, leaderID = self.id, data = self.applied_entries[key])
            else:
                print("bhola")
                return raft_pb2.ClientResponse(status = True, leaderID = self.id, data = 'None')
        else:
            return raft_pb2.ClientResponse(status = False, leaderID = self.leader_id, data = key)
        

def run(handler: RaftHandler):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(
        handler, server
    )
    server.add_insecure_port(f'[::]:{handler.address.port}')
    # print(f"Server has been started with address {handler.address}")
    server.start()
    server.wait_for_termination()

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
