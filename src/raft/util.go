package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(num1, num2 int) int {
	if num1 < num2 {
		return num1
	} else {
		return num2
	}
}

func generateOverTime(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(MoreVoteTime) + MinVoteTime
}

func (rf *Raft) UpToDate(index, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) restoreLog(curIndex int) LogEntry {
	return rf.logs[curIndex-rf.lastIncludeIndex]
}

func (rf *Raft) restoreLogTerm(curIndex int) int {
	if curIndex-rf.lastIncludeIndex == 0 {
		return rf.lastIncludeTerm
	}

	return rf.logs[curIndex-rf.lastIncludeIndex].Term
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1 + rf.lastIncludeIndex
}

func (rf *Raft) getLastTerm() int {
	if len(rf.logs) == 1 {
		return rf.lastIncludeTerm
	}

	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	lastIndex := rf.getLastIndex()

	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}

	return newEntryBeginIndex, rf.restoreLogTerm(newEntryBeginIndex)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}
