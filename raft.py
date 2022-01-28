import json
import random
import sys
import time
import threading
import secrets

state = ["FOLLOWER", "CANDIDATE", "LEADER"]


# msg format:SEND src_command_term_idxOrTrue_log; log can be empty(new leader's first msg)


class Node:
    def __init__(self, pid, num):
        self.pid = pid
        self.node_num = num
        self.term = 0
        self.currentLeader = "null"
        self.state = state[0]
        self.log = [0]
        self.lastIndex = 0  # the last log's idx
        self.commitIndex = 0  # the last committed log's idx
        self.vote_cnt = 0
        self.commit_cnt = 0  # count for commit
        random.seed(secrets.randbits(4))
        self.time_bound = 0
        self.basetime = 0
        self.lastCommitted = True
        self.reset_timeout()
        receiver = threading.Thread(target=self.receiver)
        receiver.start()

    def receiver(self):
        while True:
            line = sys.stdin.readline()
            if line is None:
                continue
            self.update(line)

    def update(self, line):
        type, msg = line.strip().split(' ', 1)
        if type == "LOG":
            if self.state == state[2]:
                entry = [self.term, msg]
                self.log.append(entry)
                self.lastIndex += 1
                print(
                    f"\nSTATE log[{self.lastIndex}]=[{self.log[self.lastIndex][0]}, \"{self.log[self.lastIndex][1]}\"]",
                    flush=True)
        else:
            src, command, tail = msg.strip().split(' ', 2)
            if command == "RequestVotes":
                term = int(tail)
                if term > self.term:
                    self.currentLeader = "null"
                    self.term = term
                    self.state = state[0]
                    print(f"\nSEND {src} RequestVotesResponse {term} true", flush=True)
                    self.reset_timeout()
                    self.printState()
                else:
                    print(f"\nSEND {src} RequestVotesResponse {self.term} false", flush=True)
            elif command == "AppendEntries":
                term, idx, log = tail.split(' ', 2)
                term = int(term)
                idx = int(idx)
                entries = json.loads(log)
                if term >= self.term:
                    self.currentLeader = src
                    self.term = term
                    self.state = state[0]
                    self.reset_timeout()
                    self.printState()
                    if idx <= self.lastIndex or len(entries) > 1:  # new log or need repair
                        for entry in entries:  # add all coming entries to self log
                            entry = [self.term, entry]
                            self.log.append(entry)
                            self.lastIndex += 1
                            print(
                                f"\nSTATE log[{self.lastIndex}]=[{self.log[self.lastIndex][0]}, \"{self.log[self.lastIndex][1]}\"]",
                                flush=True)
                        self.commitIndex = max(self.commitIndex, idx)
                        print(f"\nSEND {src} AppendEntriesResponse {self.term} {self.commitIndex} true", flush=True)
                    else:  # coming committed idx > self's log, need repair
                        self.log = self.log[:self.commitIndex + 1]
                        self.lastIndex = self.commitIndex
                        print(f"\nSEND {src} AppendEntriesResponse {self.term} {self.commitIndex} false", flush=True)
                else:
                    print(f"\nSEND {src} AppendEntriesResponse {self.term} {self.commitIndex} false", flush=True)
            elif command == "RequestVotesResponse":
                if self.state == state[1]:
                    term, granted = tail.split(' ', 1)
                    term = int(term)
                    if term == self.term and granted == "true":
                        self.vote_cnt += 1
                        if self.vote_cnt > self.node_num / 2:
                            self.state = state[2]
                            self.currentLeader = self.pid
                            self.printState()
                    else:
                        # discover higher term
                        if term > self.term:
                            self.term = term
                            self.currentLeader = "null"
                            self.state = state[0]
                            self.reset_timeout()
                            self.printState()
            else:  # AppendEntriesResponse
                if self.state == state[2]:
                    term, commitIdx, success = tail.split(' ', 2)
                    term = int(term)
                    commitIdx = int(commitIdx)
                    if term > self.term:
                        self.term = term
                        self.state = state[0]
                        self.currentLeader = "null"
                        self.reset_timeout()
                        self.printState()
                    else:  # the commit ack for not empty entry
                        if (not self.lastCommitted) and (
                                term == self.term and success == "true" and commitIdx == self.commitIndex):  # accept entry
                            self.commit_cnt += 1
                            if self.commit_cnt > self.node_num / 2:
                                self.commitIndex += 1
                                for i in range(self.node_num):
                                    if i != self.pid:
                                        print(
                                            f"\nSEND {i} AppendEntries {self.term} {self.commitIndex} {json.dumps([])}",
                                            flush=True)
                                self.lastCommitted = True
                                print(f"\nSTATE commitIndex={self.commitIndex}", flush=True)
                                print(f"\nCOMMITTED {self.log[self.commitIndex][1]} {self.commitIndex}", flush=True)
                        elif success == "false" and commitIdx < self.commitIndex:  # find partition, send miss logs
                            data = []
                            for entry in self.log[commitIdx + 1:self.commitIndex + 1]:
                                data.append(entry[1])
                            print(f"\nSEND {src} AppendEntries {self.term} {self.commitIndex} {json.dumps(data)}",
                                  flush=True)
                            if not self.lastCommitted:
                                data = [self.log[self.commitIndex + 1][1]]
                                print(f"\nSEND {src} AppendEntries {self.term} {self.commitIndex} {json.dumps(data)}",
                                      flush=True)

    def printState(self):
        print(f"\nSTATE term={self.term}", flush=True)
        print(f"\nSTATE state=\"{self.state}\"", flush=True)
        print(f"\nSTATE leader={self.currentLeader}", flush=True)
        if self.commitIndex > 0:
            print(
                f"\nSTATE log[{self.commitIndex}]=[{self.log[self.commitIndex][0]}, \"{self.log[self.commitIndex][1]}\"]",
                flush=True)
            print(f"\nSTATE commitIndex={self.commitIndex}", flush=True)

    def follower(self):
        while (time.time() - self.basetime) * 1000 <= self.time_bound:
            time.sleep(0.01)
        # timeout
        self.state = state[1]

    def candidate(self):
        self.term += 1
        self.currentLeader = "null"
        self.printState()
        self.vote_cnt = 1
        for i in range(self.node_num):
            if i != self.pid:
                print(f"\nSEND {i} RequestVotes {self.term}", flush=True)
        self.reset_timeout()
        while (time.time() - self.basetime) * 1000 <= self.time_bound and self.state == state[1]:
            time.sleep(0.01)
        self.vote_cnt = 0
        # timeout

    def leader(self):
        # trim old logs
        self.lastIndex = self.commitIndex
        self.log = self.log[:self.commitIndex + 1]
        while self.state == state[2]:
            data = []
            if self.commitIndex < self.lastIndex:
                data = [self.log[self.commitIndex + 1][1]]
                self.lastCommitted = False
            self.commit_cnt = 1
            for i in range(self.node_num):
                if i != self.pid:
                    print(f"\nSEND {i} AppendEntries {self.term} {self.commitIndex} {json.dumps(data)}", flush=True)
            time.sleep(0.3)
        self.commit_cnt = 0

    def running(self):
        while True:
            if self.state == state[0]:
                self.follower()
            if self.state == state[1]:
                self.candidate()
            if self.state == state[2]:
                self.leader()

    def reset_timeout(self):
        self.time_bound = random.randint(1500, 3000)
        self.basetime = time.time()


if __name__ == "__main__":
    node = Node(int(sys.argv[1]), int(sys.argv[2]))
    node.running()
