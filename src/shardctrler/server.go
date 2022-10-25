package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"time"
)
import "sync"

const (
	JoinType  = "join"
	LeaveType = "leave"
	MoveType  = "move"
	QueryType = "query"

	JoinOverTime  = 100
	LeaveOverTime = 100
	MoveOverTime  = 100
	QueryOverTime = 100

	// InvalidGid all shards should be assigned to GID zero (an invalid GID).
	InvalidGid = 0
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	seqMap    map[int64]int
	waitChMap map[int]chan Op
}

type Op struct {
	// Your data here.
	OpType      string
	ClientId    int64
	SeqId       int
	QueryNum    int
	JoinServers map[int][]string
	LeaveGids   []int
	MoveShard   int
	MoveGid     int
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)

	go sc.applyMsgHandlerLoop()

	return sc
}

// ------------------------------Part Rpc----------------------------------------

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:      JoinType,
		ClientId:    args.ClientId,
		SeqId:       args.SeqId,
		JoinServers: args.Servers,
	}

	lastIndex, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(lastIndex)

	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(JoinOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:    LeaveType,
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		LeaveGids: args.GIDs,
	}

	lastIndex, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(lastIndex)

	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(LeaveOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:    MoveType,
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		MoveShard: args.Shard,
		MoveGid:   args.GID,
	}

	lastIndex, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(lastIndex)

	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(MoveOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:   QueryType,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		QueryNum: args.Num,
	}

	lastIndex, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(lastIndex)

	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(QueryOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			sc.mu.Lock()
			reply.Err = OK
			sc.seqMap[op.ClientId] = op.SeqId
			if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[op.QueryNum]
			}
			sc.mu.Unlock()
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

// ------------------------------Part Loop----------------------------------------

func (sc *ShardCtrler) applyMsgHandlerLoop() {
	for {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				index := msg.CommandIndex

				if !sc.ifDuplicate(op.ClientId, op.SeqId) {
					sc.mu.Lock()
					switch op.OpType {
					case JoinType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.JoinHandler(op.JoinServers))
					case LeaveType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.LeaveHandler(op.LeaveGids))
					case MoveType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.MoveHandler(op.MoveGid, op.MoveShard))
					}
					sc.seqMap[op.ClientId] = op.SeqId
					sc.mu.Unlock()
				}
				sc.getWaitCh(index) <- op
			}
		}
	}
}

// ------------------------------Part handler----------------------------------------

func (sc *ShardCtrler) JoinHandler(servers map[int][]string) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroup := make(map[int][]string)

	for gid, serverList := range lastConfig.Groups {
		newGroup[gid] = serverList
	}

	for gid, serverList := range servers {
		newGroup[gid] = serverList
	}

	GroupMap := make(map[int]int)
	for gid := range newGroup {
		GroupMap[gid] = 0
	}

	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GroupMap[gid]++
		}
	}

	if len(GroupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroup,
		}
	}

	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, lastConfig.Shards), //because new group have no shard, so need re loadbalance
		Groups: newGroup,
	}
}

func (sc *ShardCtrler) LeaveHandler(gids []int) *Config {
	leaveMap := make(map[int]bool)
	for _, gid := range gids {
		leaveMap[gid] = true
	}

	lastConfig := sc.configs[len(sc.configs)-1]
	newGroup := make(map[int][]string)

	for gid, serverList := range lastConfig.Groups {
		newGroup[gid] = serverList
	}

	for _, leaveGid := range gids {
		delete(newGroup, leaveGid)
	}

	GroupMap := make(map[int]int)
	newShard := lastConfig.Shards

	for gid := range newGroup {
		if !leaveMap[gid] {
			GroupMap[gid] = 0
		}
	}

	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			if leaveMap[gid] {
				newShard[shard] = 0
			} else {
				GroupMap[gid]++
			}
		}
	}

	if len(GroupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroup,
		}
	}

	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, newShard),
		Groups: newGroup,
	}
}

func (sc *ShardCtrler) MoveHandler(gid int, shard int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: [10]int{},
		Groups: map[int][]string{},
	}

	for shards, gids := range lastConfig.Shards {
		newConfig.Shards[shards] = gids
	}

	newConfig.Shards[shard] = gid

	for gids, servers := range lastConfig.Groups {
		newConfig.Groups[gids] = servers
	}

	return &newConfig
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}

func (sc *ShardCtrler) ifDuplicate(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastSeqId, exist := sc.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (sc *ShardCtrler) loadBalance(GroupMap map[int]int, lastShards [NShards]int) [NShards]int {
	length := len(GroupMap)
	ave := NShards / length
	remainder := NShards % length
	sortGid := sortGroupShard(GroupMap)

	for i := 0; i < length; i++ {
		target := ave

		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}

		if GroupMap[sortGid[i]] > target {
			overLoadGid := sortGid[i]
			changeNum := GroupMap[overLoadGid] - target

			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == overLoadGid {
					lastShards[shard] = InvalidGid
					changeNum--
				}
			}
			GroupMap[overLoadGid] = target
		}
	}

	for i := 0; i < length; i++ {
		target := ave
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}

		if GroupMap[sortGid[i]] < target {
			freeGid := sortGid[i]
			changeNum := target - GroupMap[freeGid]
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}

				if gid == InvalidGid {
					lastShards[shard] = freeGid
					changeNum--
				}
			}
			GroupMap[freeGid] = target
		}
	}
	return lastShards
}

func sortGroupShard(GroupMap map[int]int) []int {
	length := len(GroupMap)
	gidSlice := make([]int, 0, length)

	for gid, _ := range GroupMap {
		gidSlice = append(gidSlice, gid)
	}

	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if GroupMap[gidSlice[j]] < GroupMap[gidSlice[j-1]] || (GroupMap[gidSlice[j]] == GroupMap[gidSlice[j-1]] && gidSlice[j] < gidSlice[j-1]) {
				gidSlice[j], gidSlice[j-1] = gidSlice[j-1], gidSlice[j]
			}
		}
	}
	return gidSlice
}

func moreAllocations(length int, remainder int, i int) bool {
	if i < length-remainder {
		return true
	} else {
		return false
	}
}
