package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// peer's role
type Status int

type VoteState int

type AppendEntriesState int

type InstallSnapshotState int

const (
	Follower Status = iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	MoreVoteTime   = 50
	MinVoteTime    = 37
	HeartBeatSleep = 17
	AppliedSleep   = 7
)

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyChan chan ApplyMsg

	status Status
	//how many people vote to me
	voteNum   int
	VoteTimer time.Time

	lastIncludeIndex int
	lastIncludeTerm  int
}

// ------------------------------Part RPC----------------------------------------

type RequestVoteArgs struct {
	Term         int
	CondidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	UpNextIndex int // if conflict, where the peer wants its nextindex is
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

// make new raft peer
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.mu.Lock()

	rf.status = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteNum = 0

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	rf.lastApplied = 0 //apply means the command is in log but not use, applied means command is used
	rf.commitIndex = 0

	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})
	rf.applyChan = applyCh
	rf.mu.Unlock()

	rf.readPersist(persister.ReadRaftState())

	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}

	go rf.electionTicker()

	go rf.appendTicker()

	go rf.committedTicker()

	return rf
}

// ------------------------------Part ticker----------------------------------------

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		nowTime := time.Now()
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)

		rf.mu.Lock()

		if rf.VoteTimer.Before(nowTime) && rf.status != Leader {
			rf.status = Candidate
			rf.votedFor = rf.me
			rf.voteNum = 1
			rf.currentTerm += 1
			rf.persist()

			rf.sendElection()
			rf.VoteTimer = time.Now()
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) appendTicker() {
	for rf.killed() == false {
		time.Sleep(HeartBeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) committedTicker() {
	for rf.killed() == false {
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)

		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied += 1
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				Command:       rf.restoreLog(rf.lastApplied).Command,
				CommandIndex:  rf.lastApplied,
				SnapshotValid: false,
			})
		}

		rf.mu.Unlock()
		for _, message := range Messages {
			rf.applyChan <- message
		}
	}
}

// ------------------------------Part ticker----------------------------------------

func (rf *Raft) sendElection() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.getLastIndex(),
				rf.getLastTerm(),
			}

			reply := RequestVoteReply{}
			rf.mu.Unlock()
			res := rf.sendRequestVote(server, &args, &reply)

			if res == true {
				rf.mu.Lock()

				if rf.status != Candidate || args.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				if reply.Term > args.Term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}

					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted == true && rf.currentTerm == args.Term {
					rf.voteNum += 1
					if rf.voteNum >= len(rf.peers)/2+1 {
						rf.status = Leader
						rf.votedFor = -1
						rf.voteNum = 0
						rf.persist()

						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.getLastIndex() + 1
						}

						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.getLastIndex()

						rf.VoteTimer = time.Now()
						rf.mu.Unlock()
						return
					}

					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				return
			}
		}(i)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.voteNum = 0
		rf.persist()
	}

	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) || rf.votedFor != -1 && rf.votedFor != args.CondidateId && args.Term == reply.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CondidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.VoteTimer = time.Now()
		rf.persist()
		return
	}
}

// ------------------------------Part append log----------------------------------------

func (rf *Raft) leaderAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				return
			}

			if rf.nextIndex[server]-1 < rf.lastIncludeIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PreLogIndex:  prevLogIndex,
				PreLogTerm:   prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{}
			}

			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			re := rf.sendAppendEntries(server, &args, &reply)

			if re == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.status != Leader {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.VoteTimer = time.Now()
					return
				}

				if reply.Success {
					rf.commitIndex = rf.lastIncludeIndex
					rf.matchIndex[server] = args.PreLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					for index := rf.getLastIndex(); index >= rf.lastIncludeIndex+1; index-- {
						sum := 0
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								sum += 1
								continue
							}
							if rf.matchIndex[i] >= index {
								sum += 1
							}
						}

						if sum >= len(rf.peers)/2+1 && rf.restoreLogTerm(index) == rf.currentTerm {
							rf.commitIndex = index
							break
						}
					}
				} else {
					if reply.UpNextIndex != -1 {
						rf.nextIndex[server] = reply.UpNextIndex
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = -1
		return
	}

	reply.Success = true
	reply.Term = args.Term
	reply.UpNextIndex = -1

	rf.status = Follower
	rf.currentTerm = args.Term
	rf.voteNum = 0
	rf.votedFor = -1
	rf.persist()
	rf.VoteTimer = time.Now()

	if rf.lastIncludeIndex > args.PreLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() + 1
		return
	}

	if rf.getLastIndex() < args.PreLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() //problem?
		return
	} else {
		if rf.restoreLogTerm(args.PreLogIndex) != args.PreLogTerm {
			reply.Success = false
			tempTerm := rf.restoreLogTerm(args.PreLogIndex)
			for index := args.PreLogIndex; index >= rf.lastIncludeIndex; index-- {
				if rf.restoreLogTerm(index) != tempTerm {
					reply.UpNextIndex = index + 1
					break
				}
			}
			return
		}
	}

	rf.logs = append(rf.logs[:args.PreLogIndex+1-rf.lastIncludeIndex], args.Entries...)
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}

	return
}

// ------------------------------Part snapshot----------------------------------------

func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludeIndex,
		rf.lastIncludeTerm,
		rf.persister.ReadSnapshot(),
	}

	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	res := rf.sendSnapShot(server, &args, &reply)

	if res == true {
		rf.mu.Lock()
		if rf.status != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Term > rf.currentTerm {
			rf.status = Follower
			rf.votedFor = -1
			rf.voteNum = 0
			rf.persist()
			rf.VoteTimer = time.Now()
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	rf.status = Follower
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	rf.VoteTimer = time.Now()

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}

	index := args.LastIncludeIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.restoreLog(i))
	}

	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.lastIncludeIndex = args.LastIncludeIndex

	rf.logs = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}

	rf.mu.Unlock()
	rf.applyChan <- msg
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastIncludeIndex >= index || index > rf.commitIndex {
		return
	}

	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLogs = append(sLogs, rf.restoreLog(i))
	}

	if index == rf.getLastIndex()+1 {
		rf.lastIncludeTerm = rf.getLastTerm()
	} else {
		rf.lastIncludeTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludeIndex = index
	rf.logs = sLogs

	if index > rf.commitIndex {
		rf.commitIndex = index
	}

	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

// ------------------------------Part persist----------------------------------------
func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() == true {
		return -1, -1, false
	}
	if rf.status != Leader {
		return -1, -1, false
	} else {
		index := rf.getLastIndex() + 1
		term := rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{term, command})
		rf.persist()
		return index, term, true
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
