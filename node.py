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
        # print("In write_to_logs function")
        dump_text = f"{op} {key} {value} {term}"
        with open(self.log_file_path, 'a') as log_file:
            # print("file opened to write logs")
            # print(f"dump text: {dump_text}")
            log_file.write(dump_text + '\n')
            log_file.flush()
    
    def last_len_helper(self):
        with open(self.log_file_path, 'r') as log_file:
            line_count = 0
            for line in log_file:
                line_count += 1
        return line_count

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
            self.nextIndex = [len(self.log_table)]* len(self.neighbours)
            self.matchIndex = [0]  * len(self.neighbours)
            # print("new leader match index: ", self.matchIndex)
            entry = {
                'term': self.term,
                'update': ['NO OP', "", ""]
            }
            self.log_table.append(entry)
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

        if prefixLen>0:
            prefixTerm= self.log_table[prefixLen-1]['term']

        appending_entries= self.log_table[prefixLen:]
        if self.nextIndex[addr.id]<=len(self.log_table):
            # print("went in this request line 230")
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
            print(f"Sent commit index: {self.commitIndex}")
            response = stub.AppendEntries(request)
            if response.term==self.term and self.state==State.LEADER:
                if response.status==True and response.ack>=self.matchIndex[response.nodeID]:
                    self.nextIndex[response.nodeID]= response.ack
                    self.matchIndex[response.nodeID]= response.ack #BIG CHANGE HERE
                    print("WILL COMMIT HERE")
                    self.commit_function(response.term)
                    # COMMIT HERE
                
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
        majority= len(self.neighbours)//2

        self.matchIndex[self.id] = len(self.log_table)
        for n in self.neighbours:
            if n.id != NODE_ADDR.id:
                thread = Thread(target = self.send_heartbeat, args = (n,))
                thread.start()
                pool.append(thread)
        
        for t in pool:
            t.join()
        
        print(self.matchIndex)
        # YEH SECTION THEEK KARNA HAI
        # acks = [0]* len(self.log_table)
        

        # print("Abhiks print 1")
        # for i in range(len(acks)):
        #     for j in self.neighbours:
        #         if self.matchIndex[j.id] >= i:
        #             acks[i] += 1
        # ready = -1
        # for i in range(len(acks)):
        #     if acks[i] > majority:
        #         ready += 1

        # print("Abhiks print 2")
        # if ready != -1 and ready > self.commitIndex:
        #     for i in range(self.commitIndex, ready,1):
        #         key = self.log_table[i]['update'][1]
        #         value = self.log_table[i]['update'][2]
        #         self.applied_entries[key] = value
        #         print("Going to write logs")
        #         self.write_to_logs(self.log_table[i]['term'], self.log_table[i]['update'][0], 
        #                             self.log_table[i]['update'][1], self.log_table[i]['update'][2])
        
        # self.commitIndex = ready
        # self.lastApplied = ready-1

        # PROBABLY YAHA TAK

        
        # self.leader_timer.cancel()
        self.leader_timer = Timer(3, self.heartbeat_timer)
        self.leader_timer.start() # leader lease comes here
    
    def acks(self, len):
        count= 0
        print(f"in acks function {self.matchIndex} and input len {len}")
        for i in self.matchIndex:
            if i>=len:
                count+= 1
        
        return count
    
    def commit_function(self, term):
        print("Akshansh 1")
        minAcks= (len(self.neighbours)+1)//2
        ready= -1
        print(f"log table len in commit function: {len(self.log_table)}")
        for i in range(1, len(self.log_table)+1):
            print("test")
            if self.acks(i)>=minAcks:
                ready= i
        
        print("Comparison1: ", ready, self.commitIndex)
        print("Comparison2: ", self.log_table[ready-1], term)
        if ready!=-1 and ready>self.commitIndex and self.log_table[ready-1]['term']==term:
            for i in range(self.commitIndex, ready):
                print("do something here to complete the code")
                self.write_to_logs(self.log_table[i]['term'], self.log_table[i]['update'][0], 
                                    self.log_table[i]['update'][1], self.log_table[i]['update'][2])

            self.commitIndex= ready
            # self.lastApplied


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
        print(f"in append entries leader commit jo aaya {leaderCommit}")
        suffix= request.entries
        self.print_and_write(f"{term} and {self.term}")
        if term>self.term:
            self.update_terms(term)
            self.voted_for= -1
            self.become_follower()
            # self.timer_reset()  # check it once
        elif term==self.term:
            # self.state = State.FOLLOWER
            self.become_follower()
            # self.timer_reset()  # check it once
            self.leader_id= leaderID
        
        # print("here 2")
        # print(f"Prefix len {prefixLen}")
        # print(f"Prefix term {prefixTerm}")
        # print(f"Log index term {self.log_table[prefixLen-1]['term']}")
        # print(f"Length Log table {len(self.log_table)}")
        logOk= (len(self.log_table)>=prefixLen) and (prefixLen==0 or self.log_table[prefixLen-1]['term']==prefixTerm)
        # print(logOk)
        if term==self.term and logOk:
            self.actualAppendEntries(prefixLen, leaderCommit, suffix)
            ack= prefixLen+len(suffix)
            print(f"ack: {ack}, sending back true response to heartbeat")
            return raft_pb2.AppendEntriesResponse(status= True, ack= ack, nodeID= self.id, term= self.term)
        else:
            # print("hellllloooo")
            return raft_pb2.AppendEntriesResponse(status= False, ack= 0, nodeID= self.id, term= self.term)
        

    def actualAppendEntries(self, prefixLen, leaderCommit, suffix):
        suffixLen= len(suffix)
        logTableLen= len(self.log_table)

        # print("Reached actual append entry")
        # print(f"suffix length {suffixLen}")
        # print(f"prefix length {prefixLen}")
        # print(f"logtable length {logTableLen}")

        if suffixLen>0 and logTableLen>prefixLen:
            # print("min1")
            index= min(logTableLen, prefixLen+suffixLen)-1

            # print(index)
            # print(f"log table {self.log_table[index]['term']}")
            # print(f"suffix table {suffix[index-prefixLen].term}")

            if self.log_table[index]['term']!=suffix[index-prefixLen].term:
                self.log_table= self.log_table[:prefixLen]
        print(f"Prefix Length: {prefixLen}")
        if prefixLen+suffixLen>logTableLen:
            for i in range(logTableLen-prefixLen, suffixLen):
                entry = {
                    'term': suffix[i].term,
                    'update': suffix[i].update
                }
                self.log_table.append(entry)
        # print()
        print(f"leader commit: {leaderCommit}")
        print(f"self commit: {self.commitIndex}")

        last_len= self.last_len_helper()
        if leaderCommit>self.commitIndex:
            print("Reached leader commit")
            self.commitIndex= min(leaderCommit, last_len-1)
            while leaderCommit>self.commitIndex:
                key= self.log_table[self.commitIndex]['update'][1]
                value= self.log_table[self.commitIndex]['update'][2]
                self.applied_entries[key]= value
                self.write_to_logs(self.log_table[self.commitIndex]['term'], self.log_table[self.commitIndex]['update'][0], key, value)
                print(f"At Node: {self.id}")
                self.commitIndex+= 1
            self.commitIndex= leaderCommit
        print(f"my log table {self.log_table}")

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
        print("reached get value")
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
