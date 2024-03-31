import grpc
import raft_pb2_grpc 
import raft_pb2 
import pytz
import time
from datetime import datetime, timedelta
import sys
import math
import random as rnd
from addr import *
from enum import Enum
from concurrent import futures
import threading
from threading import Timer, Event, Lock, Thread
import os 
import signal
import sys
lock = threading.Lock()

utc_timezone = pytz.utc


timer_lock = Event()
term_lock = threading.Lock()
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
        self.lease_acquired = False
        self.lease_interval = 0 # see if needed here or not
        self.lease_duration = 0
        self.dump_file_path = f"./logs_node_{self.id}/dump.txt"
        self.log_file_path = f"./logs_node_{self.id}/logs.txt"
        self.meta_file_path = f"./logs_node_{self.id}/metadata.txt"
        self.last_len= self.last_len_helper()
        self.flag_count= 1
        self.applied_entries_helper()
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

    def metadata_writer(self):
        dump_text= f"{self.commitIndex} {self.term} {self.voted_for}"
        with open(self.meta_file_path, 'a') as meta_file:
            meta_file.write(dump_text + '\n')
            meta_file.flush()
    
    def last_len_helper(self):
        with open(self.log_file_path, 'r') as log_file:
            line_count = 0
            for line in log_file:
                line_count += 1
        return line_count

    def print_and_write(self, message):
        print(message)
        self.write_to_dump(message)

    def applied_entries_helper(self):
        with open(self.log_file_path, 'r') as log_file:
            for line in log_file:
                words = line.split()
                if len(words) >= 3:
                    self.applied_entries[words[1]] = words[2]

    def start(self):
        self.timer_init()
        self.timer.start()

    def timer_init(self):
        # self.timer_interval = rnd.randint(150,300) / 1000
        self.timer_interval = rnd.randint(5,9) 
        self.timer = Timer(self.timer_interval, self.timer_follower)
    
    def lease_timer_init(self):
        self.lease_timer_interval = 10
        self.lease_over_time = datetime.now(utc_timezone) + timedelta(seconds = self.lease_timer_interval)
        self.lease_timer = Timer(self.lease_timer_interval, self.lease_over)

    def follower_lease_timer(self, interval):
        self.lease_timer = Timer(interval, self.lease_over)

    def lease_over(self):
        if self.state == State.FOLLOWER and self.status == Status.RUNNING:

            if self.lease_timer.finished:
                self.lease_interval = 0
                self.become_follower()
        elif self.state == State.LEADER and self.status == Status.RUNNING:
            if self.lease_timer.finished:
                self.print_and_write(f"Leader {self.id} lease renewal failed. Stepping Down") #dumpline 2
                self.become_follower()

    
    def lease_timer_reset(self):
        self.lease_timer.cancel()
        self.lease_timer = Timer(self.lease_timer_interval, self.lease_over)
        self.lease_over_time = datetime.now(utc_timezone) + timedelta(seconds = self.lease_timer_interval)
        self.lease_timer.start()

    
    def timer_follower(self):
        if self.state == State.FOLLOWER and self.status == Status.RUNNING:
            if self.timer.finished:
                self.become_candidate()
                self.start_election()
    
    def timer_reinit(self):
        self.timer_interval = rnd.randint(5,9) 

    def timer_reset(self):
        self.timer.cancel()
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
        self.print_and_write(f"Node {self.id} election timer timed out, Starting election.") #dumpline 4
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
                votes[i.id] = 1
                pass
            else:
                thread = Thread(target = self.request_votes, args = (i, votes))
                requests.append(thread)
                thread.start()

        for i in requests: 
            i.join()
        if self.state != State.CANDIDATE:
            return
        if sum(votes) > len(votes)//2:

            self.print_and_write("New Leader waiting for Old Leader Lease to timeout.") #dumpline 3
            time.sleep(self.lease_interval)
            self.lease_interval= 0
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
                self.lease_interval = max(self.lease_interval, request.interval)
                self.update_terms(response.term)
                self.become_follower()
            elif response.status == True and self.term >= response.term:
                self.lease_interval = max(self.lease_interval, response.interval)
                votes[i.id] = 1
        except Exception as e: 
            pass
    
    def become_follower(self):
        self.lease_acquired = False
        self.update_state(State.FOLLOWER)
        self.timer_reset()
    
    def become_leader(self):
        if self.status == Status.CRASHED:
            return

        if self.state == State.CANDIDATE:
            self.print_and_write(f"Node {self.id} became the leader for term {self.term}.") #dumpline 5
            self.lease_acquired = True
            self.update_state(State.LEADER)
            self.voted_for= self.id
            self.nextIndex = [len(self.log_table)]* len(self.neighbours)
            self.matchIndex = [0]  * len(self.neighbours)
            entry = {
                'term': self.term,
                'update': ['NO OP', "", ""]
            }
            self.log_table.append(entry)
            self.leader_id = self.id
            self.heartbeat_timer()

    def send_heartbeat(self, addr):
        if self.status == Status.CRASHED:
            return
        
        self.print_and_write(f"Leader {self.id} sending heartbeat & Renewing Lease") #dumpline 1
        channel = grpc.insecure_channel(f"{addr.ip}:{addr.port}")
        stub = raft_pb2_grpc.RaftStub(channel)
        request= {}
        self.lease_timer_init()
        propInterval = self.lease_timer_interval
        # Log Replication

        prefixLen= self.nextIndex[addr.id]
        prefixTerm= 0

        if prefixLen>0:
            prefixTerm= self.log_table[prefixLen-1]['term']

        appending_entries= self.log_table[prefixLen:]
        if self.nextIndex[addr.id]<=len(self.log_table):
            request = raft_pb2.LogRequest(leaderID = self.id,leaderTerm = self.term,
                            prefixLen = prefixLen,
                            prefixTerm = prefixTerm,
                            entries = appending_entries,
                            leaderCommit = self.commitIndex,
                            lease_over_time = propInterval)

        else:
            request = raft_pb2.LogRequest(leaderID = self.id,leaderTerm = self.term,
                            prefixLen = prefixLen,
                            prefixTerm = prefixTerm,
                            entries = [],
                            leaderCommit = self.commitIndex,
                            lease_over_time = propInterval)

        try:
            response = stub.AppendEntries(request)
            if response.term==self.term and self.state==State.LEADER:
                if response.status==True:
                    self.flag_count+= 1
                if response.status==True and response.ack>=self.matchIndex[response.nodeID]:
                    self.nextIndex[response.nodeID]= response.ack
                    self.matchIndex[response.nodeID]= response.ack
                    self.commit_function(response.term)
                
                elif self.nextIndex[response.nodeID]>0:
                    self.nextIndex[response.nodeID]= self.nextIndex[response.nodeID]-1
                    self.send_heartbeat(addr)
            elif response.term>self.term:
                self.update_term= response.term
                self.voted_for= -1
                self.print_and_write(f"{self.id} Stepping down") #dumpline 14
                self.become_follower()
        except:
            self.print_and_write(f"Error occurred while sending RPC to Node {addr.id}.") #dumpline 6
            pass
    
    def heartbeat_timer(self):
        if self.status == Status.CRASHED:
            return
        if self.state != State.LEADER:
            return
        pool = []
        majority= (len(self.neighbours)+1)//2
        self.matchIndex[self.id] = len(self.log_table)
        for n in self.neighbours:
            if n.id != NODE_ADDR.id:
                thread = Thread(target = self.send_heartbeat, args = (n,))
                thread.start()
                pool.append(thread)
        
        for t in pool:
            t.join()

        if self.flag_count<majority:
            pass
        else:
            self.flag_count= 1
        
            self.leader_timer = Timer(3, self.heartbeat_timer)
            self.leader_timer.start() # leader lease comes here
            self.lease_timer_init()
    
    def acks(self, len):
        count= 0
        for i in self.matchIndex:
            if i>=len:
                count+= 1
        
        return count
    
    def commit_function(self, term):
        with lock:
            minAcks= (len(self.neighbours)+1)//2
            ready= -1
            for i in range(1, len(self.log_table)+1):
                if self.acks(i)>=minAcks:
                    ready= i
            
            if ready!=-1 and ready>self.commitIndex and self.log_table[ready-1]['term']==term:
                for i in range(self.commitIndex, ready):
                    operation= self.log_table[i]['update'][0]
                    key= self.log_table[i]['update'][1]
                    value= self.log_table[i]['update'][2]
                    self.applied_entries[key]= value
                    self.write_to_logs(self.log_table[i]['term'], operation, key, value)
                    self.print_and_write(f"Node {self.id} (Leader) committed {operation} {key} {value}  to state machine") #dumpline 9
                self.commitIndex= ready
    def signal_handler(self,sig, frame):
        self.metadata_writer()
        os._exit(0)

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
        if cTerm == self.term and logOk and (self.voted_for == -1 or self.voted_for == cid):
            self.voted_for = cid
            self.print_and_write(f"Vote granted for Node {cid} in term {cTerm}.") #dumpline 12
            return raft_pb2.VoteResponse(status = True, term = self.term, nodeID = cid, interval = self.lease_interval)
        else:
            self.print_and_write(f"Vote denied for Node {cid} in term {cTerm}.") #dumpline 13
            return raft_pb2.VoteResponse(status = False, term = self.term, nodeID = cid, interval = self.lease_interval)

    def AppendEntries(self, request, context):
        if self.status== Status.CRASHED:
            return raft_pb2.AppendEntriesResponse(status= False, ack = 0, nodeID = self.id, term=self.term)
        leaderID= request.leaderID
        term= request.leaderTerm
        prefixLen= request.prefixLen
        prefixTerm= request.prefixTerm
        leaderCommit= request.leaderCommit
        suffix= request.entries
        lease_interval = request.lease_over_time
        self.follower_lease_timer(lease_interval)
        if term>self.term:
            self.update_terms(term)
            self.voted_for= -1
            self.become_follower()
        elif term==self.term:
            self.become_follower()
            self.leader_id= leaderID
        
        logOk= (len(self.log_table)>=prefixLen) and (prefixLen==0 or self.log_table[prefixLen-1]['term']==prefixTerm)
        if term==self.term and logOk:
            self.actualAppendEntries(prefixLen, leaderCommit, suffix)
            ack= prefixLen+len(suffix)
            return raft_pb2.AppendEntriesResponse(status= True, ack= ack, nodeID= self.id, term= self.term)
        else:
            self.print_and_write(f"Node {self.id} rejected AppendEntries RPC from {self.leader_id}.") #dumpline 11 
            return raft_pb2.AppendEntriesResponse(status= False, ack= 0, nodeID= self.id, term= self.term)
        

    def actualAppendEntries(self, prefixLen, leaderCommit, suffix):
        suffixLen= len(suffix)
        logTableLen= len(self.log_table)


        if suffixLen>0 and logTableLen>prefixLen:
            index= min(logTableLen, prefixLen+suffixLen)-1

            if self.log_table[index]['term']!=suffix[index-prefixLen].term:
                self.log_table= self.log_table[:prefixLen]
        if prefixLen+suffixLen>logTableLen:
            for i in range(logTableLen-prefixLen, suffixLen):
                entry = {
                    'term': suffix[i].term,
                    'update': suffix[i].update
                }
                self.print_and_write(f"Node {self.id} accepted AppendEntries RPC from {self.leader_id}.") #dumpline 10
                self.log_table.append(entry)

        if leaderCommit>self.commitIndex:
            self.commitIndex= min(leaderCommit, self.last_len)
            while leaderCommit>self.commitIndex:
                operation = self.log_table[self.commitIndex]['update'][0]
                key= self.log_table[self.commitIndex]['update'][1]
                value= self.log_table[self.commitIndex]['update'][2]
                self.applied_entries[key]= value
                self.write_to_logs(self.log_table[self.commitIndex]['term'], operation, key, value)
                self.print_and_write(f"Node {self.id} (follower) committed the entry {operation} {key} {value} to the state machine.") #dumpline 7
                self.commitIndex+= 1
            self.commitIndex= leaderCommit
            self.last_len = leaderCommit

    def SetValue(self, request, context):
        try:
            key = request.key
            value = request.value
            if self.state == State.LEADER and self.lease_acquired == True:
                self.print_and_write(f"Node {self.id} (leader) received a Set {key} {value} request.") #dumpline 8
                entry = {
                    'term': self.term,
                    'update': ['set', key, value]
                }
                self.log_table.append(entry)
                response = {
                    'success': True
                }
                return raft_pb2.ClientResponse(status = True, leaderID = self.id, data = key)
            else:
                return raft_pb2.ClientResponse(status = False, leaderID = self.leader_id, data = key)
        except: 
            return raft_pb2.ClientResponse(status = False, leaderID = self.leader_id, data = key)


    def GetValue(self, request, context):
        key = request.key
        if self.state == State.LEADER and self.lease_acquired == True:
            self.print_and_write(f"Node {self.id} (leader) received a Get {key} request.") #dumpline 8
            if self.applied_entries[key]:
                response = {
                    'success': True,
                    'value': self.applied_entries[key]
                }
                return raft_pb2.ClientResponse(status = True, leaderID = self.id, data = self.applied_entries[key])
            else:
                return raft_pb2.ClientResponse(status = True, leaderID = self.id, data = 'None')
        else:
            return raft_pb2.ClientResponse(status = False, leaderID = self.leader_id, data = key)
        

def run(handler: RaftHandler):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(
        handler, server
    )
    server.add_insecure_port(f'[::]:{handler.address.port}')
    server.start()
    signal.signal(signal.SIGINT, handler.signal_handler)
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
    

