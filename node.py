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
        # self.lease_timer2 = 
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
        # print("In write_to_logs function")
        dump_text = f"{op} {key} {value} {term}"
        with open(self.log_file_path, 'a') as log_file:
            # print("file opened to write logs")
            # print(f"dump text: {dump_text}")
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
    
    # def timer_lease(self):
    #     self.lease_time2 = rnd.randint(4,8)
    #     self.lease_timer2 = Timer(self.lease_timer_interval, self.lease_finish)
    
    # def lease_finish(self):


    def timer_init(self):
        # self.timer_interval = rnd.randint(150,300) / 1000
        self.timer_interval = rnd.randint(5,9) 
        self.timer = Timer(self.timer_interval, self.timer_follower)
    
    def lease_timer_init(self):
        self.lease_timer_interval = 10
        self.lease_over_time = datetime.now(utc_timezone) + timedelta(seconds = self.lease_timer_interval)
        self.lease_timer = Timer(self.lease_timer_interval, self.lease_over)

    def follower_lease_timer(self, interval):
        # self.lease_timer_interval = rnd.randint(4,8)
        # self.lease_over_time = datetime.now(utc_timezone) + timedelta(seconds = self.lease_timer_interval)
        self.lease_timer = Timer(interval, self.lease_over)

    def lease_over(self):
        print("entered lease over")
        if self.state == State.FOLLOWER and self.status == Status.RUNNING:
            print("Entered first if condition")

            if self.lease_timer.finished:
                self.lease_interval = 0
                print(f"Leader lease renewal failed, stepping down as leader")
                self.become_follower()
        elif self.state == State.LEADER and self.status == Status.RUNNING:
            if self.lease_timer.finished:
                print("Leader lease timed out stepping down")
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
        # self.timer_interval = rnd.randint(150,300) / 1000
        self.timer_interval = rnd.randint(5,9) 

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
                # self.print_and_write("Voted for myself")
                votes[i.id] = 1
                pass
            else:
                thread = Thread(target = self.request_votes, args = (i, votes))
                requests.append(thread)
                thread.start()
                # self.request_votes(i, votes)

        for i in requests: 
            i.join()
        print(votes)
        if self.state != State.CANDIDATE:
            return
        if sum(votes) > len(votes)//2:

            self.print_and_write(f"Node {self.id} becomes leader for term {self.term}, waiting for old lease to run out")
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
            # print("here")
            if response.term > self.term:
                print(self.lease_interval)
                self.lease_interval = max(self.lease_interval, request.interval) # need to see if this is correct but should not matter
                self.update_terms(response.term)
                self.become_follower()
            elif response.status == True and self.term >= response.term:
                print(self.lease_interval, "hello")
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
        # self.nextIndex = [len(self.log_table)]* len(self.neighbours)
        # self.matchIndex = [0]  * len(self.neighbours)
        # self.leader_id = self.id

        if self.state == State.CANDIDATE:
            self.print_and_write(f"Node {self.id} Became Leader and renewing lease")
            self.lease_acquired = True
            self.update_state(State.LEADER)
            self.voted_for= self.id
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
            # self.lease_timer_init()
            self.heartbeat_timer()
            # print("Test2")

    def send_heartbeat(self, addr):
        if self.status == Status.CRASHED:
            return
        
        self.print_and_write("Leader sending heartbeat and renewing lease")
        channel = grpc.insecure_channel(f"{addr.ip}:{addr.port}")
        stub = raft_pb2_grpc.RaftStub(channel)
        request= {}
        self.lease_timer_init()
        propInterval = self.lease_timer_interval
        # lease_time_str = self.lease_over_time.isoformat()
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
            # print(f"Sent commit index: {self.commitIndex}")
            response = stub.AppendEntries(request)
            if response.term==self.term and self.state==State.LEADER:
                if response.status==True:
                    self.flag_count+= 1
                if response.status==True and response.ack>=self.matchIndex[response.nodeID]:
                    self.nextIndex[response.nodeID]= response.ack
                    self.matchIndex[response.nodeID]= response.ack #BIG CHANGE HERE
                    # print("WILL COMMIT HERE")
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
            self.print_and_write(f"Error while sending RPC to {addr.id} ")
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
            print("aaaaaaaaaa")
            pass
            # print("Stepping down due to less followers")
            # self.become_follower()
        else:
            print("Going good")
            self.flag_count= 1
        
            # self.leader_timer.cancel()
            self.leader_timer = Timer(3, self.heartbeat_timer)
            self.leader_timer.start() # leader lease comes here
            self.lease_timer_init()
    
    def acks(self, len):
        count= 0
        # print(f"in acks function {self.matchIndex} and input len {len}")
        for i in self.matchIndex:
            if i>=len:
                count+= 1
        
        return count
    
    def commit_function(self, term):
        with lock:
            # print("Akshansh 1")
            minAcks= (len(self.neighbours)+1)//2
            ready= -1
            # print(f"log table len in commit function: {len(self.log_table)}")
            for i in range(1, len(self.log_table)+1):
                # print("test")
                if self.acks(i)>=minAcks:
                    ready= i
                # kush akshansh if this is not true should we fail to renew leader lease?
            
            if ready!=-1 and ready>self.commitIndex and self.log_table[ready-1]['term']==term:
                for i in range(self.commitIndex, ready):
                    # print("do something here to complete the code")
                    key= self.log_table[i]['update'][1]
                    value= self.log_table[i]['update'][2]
                    self.applied_entries[key]= value
                    self.write_to_logs(self.log_table[i]['term'], self.log_table[i]['update'][0], 
                                        key, value)
                    if self.state == State.LEADER:
                        self.print_and_write(f"Node {self.id} (Leader) committed {self.log_table[i]['update'][0]} {self.log_table[i]['update'][1]} {self.log_table[i]['update'][2]}  to state machine")
                    else:
                        self.print_and_write(f"Node {self.id} (Follower) committed {self.log_table[i]['update'][0]} {self.log_table[i]['update'][1]} {self.log_table[i]['update'][2]} entry to state machine")
                self.commitIndex= ready
                # self.lastApplied            
    def signal_handler(self,sig, frame):
        print('You pressed Ctrl+C! Node iD:',self.id)
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
            self.print_and_write(f"Voted for {cid} in term {cTerm}")
            return raft_pb2.VoteResponse(status = True, term = self.term, nodeID = cid, interval = self.lease_interval)
        else:
            self.print_and_write(f"Voted denied for {cid} in term {cTerm}")
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
        # lease_over_time = datetime.fromisoformat(lease_over_time_str)
        # self.lease_interval = (lease_over_time - datetime.now(utc_timezone)).total_seconds()
        # self.print_and_write(f"{term} and {self.term}")
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
            self.print_and_write(f"Node {self.id} accepted append entries RPC")
            return raft_pb2.AppendEntriesResponse(status= True, ack= ack, nodeID= self.id, term= self.term)
        else:
            self.print_and_write(f"Node {self.id} rejected append entries RPC")
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
                self.log_table.append(entry)

        if leaderCommit>self.commitIndex:
            self.commitIndex= min(leaderCommit, self.last_len)
            while leaderCommit>self.commitIndex:
                key= self.log_table[self.commitIndex]['update'][1]
                value= self.log_table[self.commitIndex]['update'][2]
                self.applied_entries[key]= value
                self.write_to_logs(self.log_table[self.commitIndex]['term'], self.log_table[self.commitIndex]['update'][0], key, value)
                self.commitIndex+= 1
            self.commitIndex= leaderCommit
            self.last_len = leaderCommit

    def SetValue(self, request, context):
        try:
            key = request.key
            value = request.value
            if self.state == State.LEADER and self.lease_acquired == True:
                self.print_and_write(f"Node {self.id} received Set Request")
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
        except: 
            return raft_pb2.ClientResponse(status = False, leaderID = self.leader_id, data = key)


    def GetValue(self, request, context):
        key = request.key
        if self.state == State.LEADER and self.lease_acquired == True:
            self.print_and_write(f"Node {self.id} received Get Request")
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
    print('Press Ctrl+C')
    # signal.pause()
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
    

