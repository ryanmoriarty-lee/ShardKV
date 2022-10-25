package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"

const (
	UpConfigLoopInterval = 100 * time.Millisecond
	GetTimeout           = 500 * time.Millisecond
	AppOrPutTimeout      = 500 * time.Millisecond
	UpConfigTimeout      = 500 * time.Millisecond
	AddShardsTimeout     = 500 * time.Millisecond
	RemoveShardsTimeout  = 500 * time.Millisecond
)

type Shard struct {
	KvMap     map[string]string
	ConfigNum int
}

type Op struct {
	ClientId int64
	SeqId    int
	OpType   Operation
	Key      string
	Value    string
	UpConfig shardctrler.Config
	ShardId  int
	Shard    Shard
	SeqMap   map[int64]int
}

type OpReply struct {
	ClientId int64
	SeqId    int
	Err      Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32

	Config     shardctrler.Config
	LastConfig shardctrler.Config

	shardsPersist []Shard
	waitChMap     map[int]chan OpReply
	SeqMap        map[int64]int
	sck           *shardctrler.Clerk
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = make_end
	kv.gid = gid
	kv.masters = ctrlers

	kv.shardsPersist = make([]Shard, shardctrler.NShards)
	kv.SeqMap = make(map[int64]int)

	kv.sck = shardctrler.MakeClerk(kv.masters)
	kv.waitChMap = make(map[int]chan OpReply)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyMsgHandlerLoop()
	go kv.ConfigDetectedLoop()

	return kv
}

// ------------------------------Part Loop----------------------------------------
func (kv *ShardKV) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}

		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid == true {
				kv.mu.Lock()
				op := msg.Command.(Op)
				reply := OpReply{
					ClientId: op.ClientId,
					SeqId:    op.SeqId,
					Err:      OK,
				}

				if op.OpType == PutType || op.OpType == GetType || op.OpType == AppendType {
					shardId := key2shard(op.Key)

					if kv.Config.Shards[shardId] != kv.gid {
						reply.Err = ErrWrongGroup
					} else if kv.shardsPersist[shardId].KvMap == nil {
						reply.Err = ShardNotArrived
					} else {
						if !kv.ifDuplicate(op.ClientId, op.SeqId) {
							kv.SeqMap[op.ClientId] = op.SeqId
							switch op.OpType {
							case PutType:
								kv.shardsPersist[shardId].KvMap[op.Key] = op.Value
							case AppendType:
								kv.shardsPersist[shardId].KvMap[op.Key] += op.Value
							case GetType:

							default:
								log.Fatalf("invalid command type: %v.", op.OpType)
							}
						}
					}
				} else {
					switch op.OpType {
					case UpConfigType:
						kv.upConfigHandler(op)
					case AddShardType:
						if kv.Config.Num < op.SeqId {
							reply.Err = ConfigNotArrived
							break
						}
						kv.addShardHandler(op)
					case RemoveShardType:
						kv.removeShardHandler(op)
					default:
						log.Fatalf("invalid command type: %v.", op.OpType)
					}
				}
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}

				ch := kv.getWaitCh(msg.CommandIndex)
				ch <- reply
				kv.mu.Unlock()
			}

			if msg.SnapshotValid == true {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.mu.Lock()
					kv.DecodeSnapShot(msg.Snapshot)
					kv.mu.Unlock()
				}
				continue
			}
		}
	}
}

func (kv *ShardKV) ConfigDetectedLoop() {
	kv.mu.Lock()

	curConfig := kv.Config
	rf := kv.rf
	kv.mu.Unlock()

	for !kv.killed() {
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Lock()

		if !kv.allSent() {
			SeqMap := make(map[int64]int)
			for k, v := range kv.SeqMap {
				SeqMap[k] = v
			}

			for shardId, gid := range kv.LastConfig.Shards {
				if gid == kv.gid && kv.Config.Shards[shardId] != kv.gid && kv.shardsPersist[shardId].ConfigNum < kv.Config.Num {
					sendDate := kv.cloneShard(kv.Config.Num, kv.shardsPersist[shardId].KvMap)

					args := SendShardArgs{
						LastAppliedRequestId: SeqMap,
						ShardId:              shardId,
						Shard:                sendDate,
						ClientId:             int64(gid),
						RequestId:            kv.Config.Num,
					}

					serverList := kv.Config.Groups[kv.Config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serverList))

					for i, name := range serverList {
						servers[i] = kv.makeEnd(name)
					}

					go func(servers []*labrpc.ClientEnd, args *SendShardArgs) {
						index := 0
						start := time.Now()
						for {
							var reply AddShardReply
							ok := servers[index].Call("ShardKV.AddShard", args, &reply)

							if ok && reply.Err == OK || time.Now().Sub(start) >= 2*time.Second {
								kv.mu.Lock()
								command := Op{
									OpType:   RemoveShardType,
									ClientId: int64(kv.gid),
									SeqId:    kv.Config.Num,
									ShardId:  args.ShardId,
								}
								kv.mu.Unlock()
								kv.startCommand(command, RemoveShardsTimeout)
								break
							}
							index = (index + 1) % len(servers)
							if index == 0 {
								time.Sleep(UpConfigLoopInterval)
							}
						}
					}(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		curConfig = kv.Config
		sck := kv.sck
		kv.mu.Unlock()

		newConfig := sck.Query(curConfig.Num + 1)
		if newConfig.Num != curConfig.Num+1 {
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		command := Op{
			OpType:   UpConfigType,
			ClientId: int64(kv.gid),
			SeqId:    newConfig.Num,
			UpConfig: newConfig,
		}
		kv.startCommand(command, UpConfigTimeout)
	}
}

// ------------------------------Part RPC----------------------------------------

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	command := Op{
		OpType:   GetType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
	}

	err := kv.startCommand(command, GetTimeout)
	if err != OK {
		reply.Err = err
		return
	}

	kv.mu.Lock()

	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	} else {
		reply.Err = OK
		reply.Value = kv.shardsPersist[shardId].KvMap[args.Key]
	}
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}

	command := Op{
		OpType:   args.Op,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
		Value:    args.Value,
	}

	reply.Err = kv.startCommand(command, AppOrPutTimeout)
	return
}

func (kv *ShardKV) AddShard(args *SendShardArgs, reply *AddShardReply) {
	command := Op{
		OpType:   AddShardType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		ShardId:  args.ShardId,
		Shard:    args.Shard,
		SeqMap:   args.LastAppliedRequestId,
	}

	reply.Err = kv.startCommand(command, AddShardsTimeout)
	return
}

// ------------------------------Part handler----------------------------------------

//use to add new shard tp leader to help leader to update config in configloop
func (kv *ShardKV) upConfigHandler(op Op) {
	curConfig := kv.Config
	upConfig := op.UpConfig
	if curConfig.Num >= upConfig.Num {
		return
	}

	for shard, gid := range upConfig.Shards {
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			kv.shardsPersist[shard].KvMap = make(map[string]string)
			kv.shardsPersist[shard].ConfigNum = upConfig.Num
		}
	}
	kv.LastConfig = curConfig
	kv.Config = upConfig
}

func (kv *ShardKV) addShardHandler(op Op) {
	if kv.shardsPersist[op.ShardId].KvMap != nil || op.Shard.ConfigNum < kv.Config.Num {
		return
	}

	kv.shardsPersist[op.ShardId] = kv.cloneShard(op.Shard.ConfigNum, op.Shard.KvMap)

	for clientId, SeqId := range op.SeqMap {
		if r, ok := kv.SeqMap[clientId]; !ok || r < SeqId {
			kv.SeqMap[clientId] = SeqId
		}
	}
}

func (kv *ShardKV) removeShardHandler(op Op) {
	if op.SeqId < kv.Config.Num {
		return
	}

	kv.shardsPersist[op.ShardId].KvMap = nil
	kv.shardsPersist[op.ShardId].ConfigNum = op.SeqId
}

// ------------------------------Part persist----------------------------------------

func (kv *ShardKV) PersistSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.shardsPersist)
	err = e.Encode(kv.SeqMap)
	err = e.Encode(kv.maxraftstate)
	err = e.Encode(kv.Config)
	err = e.Encode(kv.LastConfig)

	if err != nil {
		log.Fatalf("[%d-%d] fails to take snapshot.", kv.gid, kv.me)
	}

	return w.Bytes()
}

func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shardPersist []Shard
	var SeqMap map[int64]int
	var MaxRaftState int
	var Config, LastConfig shardctrler.Config

	if d.Decode(&shardPersist) != nil || d.Decode(&SeqMap) != nil ||
		d.Decode(&MaxRaftState) != nil || d.Decode(&Config) != nil || d.Decode(&LastConfig) != nil {
		log.Fatalf("[Server(%v)] Failed to decode snapshot！！！", kv.me)
	} else {
		kv.shardsPersist = shardPersist
		kv.SeqMap = SeqMap
		kv.maxraftstate = MaxRaftState
		kv.Config = Config
		kv.LastConfig = LastConfig
	}
}

// ------------------------------Part utils----------------------------------------

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) ifDuplicate(clientId int64, seqId int) bool {
	lastSeqId, exist := kv.SeqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *ShardKV) getWaitCh(index int) chan OpReply {
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan OpReply, 1)
		ch = kv.waitChMap[index]
	}

	return ch
}

func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.LastConfig.Shards {
		if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.LastConfig.Shards {
		if gid != kv.gid && kv.Config.Shards[shard] == kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) startCommand(command Op, timeoutPeriod time.Duration) Err {
	kv.mu.Lock()
	index, _, isLeadler := kv.rf.Start(command)
	if !isLeadler {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	ch := kv.getWaitCh(index)
	kv.mu.Unlock()

	timer := time.NewTimer(timeoutPeriod)
	defer timer.Stop()

	select {
	case re := <-ch:
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		if re.SeqId != command.SeqId || re.ClientId != command.ClientId {
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return re.Err

	case <-timer.C:
		return ErrOverTime
	}
}

func (kv *ShardKV) cloneShard(ConfigNum int, kvMap map[string]string) Shard {
	migrateShard := Shard{
		KvMap:     make(map[string]string),
		ConfigNum: ConfigNum,
	}

	for k, v := range kvMap {
		migrateShard.KvMap[k] = v
	}

	return migrateShard
}
