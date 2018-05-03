package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "log"
import "time"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
    requestMap map[int64]int64
    applyCommandIndex int
    applyIndex int
    killed bool

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
    Type string
    // Join
	Servers map[int][]string // new GID -> servers mappings
    // Leave
	GIDs []int
    // Move
	Shard int
	GID   int
    ClientId int64
    SerialNumber int64
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
    reply.Err = OK
    reply.WrongLeader = false
    if _, isleader := sm.rf.GetState(); isleader == false {
        reply.WrongLeader = true
        return
    }
    sm.mu.Lock()
    if val, ok := sm.requestMap[args.ClientId]; ok {
        if val >= args.SerialNumber {
            sm.mu.Unlock()
            reply.WrongLeader = false
            reply.Err = OK
            return
        }
    }
    sm.mu.Unlock()
    index, term, exist := sm.get(args.ClientId, args.SerialNumber)
    var isLeader bool = false
    if exist == false {
        var op Op
        op.Type = "Join"
        op.Servers = args.Servers
        op.ClientId = args.ClientId
        op.SerialNumber = args.SerialNumber
        index, term, isLeader = sm.rf.Start(op)
        if isLeader == false {
            reply.WrongLeader = true
            return
        }
    }
    for sm.applyCommandIndex < index {
        log.Printf("Join me:%d, sm.applyCommandIndex:%d, index:%d", sm.me, sm.applyCommandIndex, index)
        time.Sleep(5 * time.Millisecond)
        if _, isleader := sm.rf.GetState(); isleader == false {
            reply.WrongLeader = true
            return
        }
    }
    l, exist := sm.rf.GetLog(index)
    if exist == false || l.Term != term {
        reply.WrongLeader = true
        return
    }

    reply.WrongLeader = false
    reply.Err = OK
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
    reply.Err = OK
    reply.WrongLeader = false
    if _, isleader := sm.rf.GetState(); isleader == false {
        reply.WrongLeader = true
        return
    }
    sm.mu.Lock()
    if val, ok := sm.requestMap[args.ClientId]; ok {
        if val >= args.SerialNumber {
            sm.mu.Unlock()
            reply.WrongLeader = false
            reply.Err = OK
            return
        }
    }
    sm.mu.Unlock()
    index, term, exist := sm.get(args.ClientId, args.SerialNumber)
    var isLeader bool = false
    if exist == false {
        var op Op
        op.Type = "Leave"
        op.GIDs = args.GIDs
        op.ClientId = args.ClientId
        op.SerialNumber = args.SerialNumber
        index, term, isLeader = sm.rf.Start(op)
        if isLeader == false {
            reply.WrongLeader = true
            return
        }
    }
    for sm.applyCommandIndex < index {
        log.Printf("Join me:%d, sm.applyCommandIndex:%d, index:%d", sm.me, sm.applyCommandIndex, index)
        time.Sleep(5 * time.Millisecond)
        if _, isleader := sm.rf.GetState(); isleader == false {
            reply.WrongLeader = true
            return
        }
    }
    l, exist := sm.rf.GetLog(index)
    if exist == false || l.Term != term {
        reply.WrongLeader = true
        return
    }

    reply.WrongLeader = false
    reply.Err = OK
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
    reply.Err = OK
    reply.WrongLeader = false
    if _, isleader := sm.rf.GetState(); isleader == false {
        reply.WrongLeader = true
        return
    }
    sm.mu.Lock()
    if val, ok := sm.requestMap[args.ClientId]; ok {
        if val >= args.SerialNumber {
            sm.mu.Unlock()
            reply.WrongLeader = false
            reply.Err = OK
            return
        }
    }
    sm.mu.Unlock()
    index, term, exist := sm.get(args.ClientId, args.SerialNumber)
    var isLeader bool = false
    if exist == false {
        var op Op
        op.Type = "Move"
        op.Shard = args.Shard
        op.GID = args.GID
        op.ClientId = args.ClientId
        op.SerialNumber = args.SerialNumber
        index, term, isLeader = sm.rf.Start(op)
        if isLeader == false {
            reply.WrongLeader = true
            return
        }
    }
    for sm.applyCommandIndex < index {
        log.Printf("Move me:%d, sm.applyCommandIndex:%d, index:%d", sm.me, sm.applyCommandIndex, index)
        time.Sleep(5 * time.Millisecond)
        if _, isleader := sm.rf.GetState(); isleader == false {
            reply.WrongLeader = true
            return
        }
    }
    l, exist := sm.rf.GetLog(index)
    if exist == false || l.Term != term {
        reply.WrongLeader = true
        return
    }

    reply.WrongLeader = false
    reply.Err = OK
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
    reply.WrongLeader = false
    reply.Err = OK
    if sm.rf.IsReadable() == false {
        reply.WrongLeader = true
        return
    }
    //defer log.Printf("Query end, args:%+v, reply:%+v, me:%d, sm.requestMap:%+v", args, reply, sm.me, sm.requestMap)
    sm.mu.Lock()
    defer sm.mu.Unlock()
    if args.Num == -1 {
        reply.Config = sm.configs[len(sm.configs) - 1]
        return
    }
    if args.Num > len(sm.configs) {
        reply.Err = INVALIDARG
        return
    }
    reply.Config = sm.configs[args.Num]
    return
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
    sm.killed = true
    log.Printf("end sm kill, me:%d", sm.me)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
    sm.requestMap = make(map[int64]int64)
    go sm.apply()

	return sm
}

func (sm *ShardMaster) join(servers map[int][]string) {
    log.Printf("join, servers:%+v", servers)
    config := sm.configs[len(sm.configs)-1]
    var newConfig Config
    newConfig.Num = config.Num + 1
    newConfig.Shards = config.Shards
    newConfig.Groups = make(map[int][]string) // gid -> servers[]
    for k1, v1 := range config.Groups {
        newConfig.Groups[k1] = v1
    }
    // need to load balance
    for g, s := range servers {
        needed := NShards / (len(newConfig.Groups) + 1)
        remain := needed
        log.Printf("join, needed:%d, remain:%d, newConfig.Shards:%+v", needed, remain, newConfig.Shards)
        if needed > 0 {
            countMap := make(map[int]int) // gid -> times
            for index, gid := range newConfig.Shards {
                if remain <= 0 {
                    break
                }
                if _, ok := countMap[gid]; ok {
                    countMap[gid] += 1
                } else {
                    countMap[gid] = 1
                }
                if countMap[gid] > needed || gid == 0{
                    newConfig.Shards[index] = g
                    log.Printf("join, shard:%d, g:%d", index, g)
                    remain -= 1
                }
            }
        }
        newConfig.Groups[g] = s
    }
    log.Printf("join, newConfig:%+v", newConfig)
    sm.configs = append(sm.configs, newConfig)
}

func InArray(array [NShards]int, search int) bool{
    for _, val := range array {
        if val == search {
            return true
        }
    }
    return false
}

func FindLeastShardsOfGid(shards [NShards]int, except int) int{
    var minShardsNum int = MaxInt
    var minGid int = 0
    countMap := make(map[int]int) // gid -> times
    for _, gid := range shards {
        if _, ok := countMap[gid]; ok {
            countMap[gid] += 1
        } else {
            countMap[gid] = 1
        }
    }
    for gid, count := range countMap {
        if count < minShardsNum && gid != except{
            minGid = gid
            minShardsNum = count
        }
    }
    return minGid
}

func (sm *ShardMaster) move(shard int, gid int) {
    log.Printf("move, shard:%d, gid:%d", shard, gid)
    config := sm.configs[len(sm.configs)-1]
    var newConfig Config
    newConfig.Num = config.Num + 1
    newConfig.Shards = config.Shards
    newConfig.Groups = make(map[int][]string) // gid -> servers[]
    for k1, v1 := range config.Groups {
        newConfig.Groups[k1] = v1
    }
    // move
    newConfig.Shards[shard] = gid
    log.Printf("leave, newConfig:%+v", newConfig)
    sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) leave(GIDs []int) {
    log.Printf("leave, GIDs:%+v", GIDs)
    config := sm.configs[len(sm.configs)-1]
    var newConfig Config
    newConfig.Num = config.Num + 1
    newConfig.Shards = config.Shards
    newConfig.Groups = make(map[int][]string) // gid -> servers[]
    for k1, v1 := range config.Groups {
        newConfig.Groups[k1] = v1
    }
    // clear group and Shards where gid in GIDs
    for _, gid := range GIDs {
        delete(newConfig.Groups, gid)
        for shard, gid1 := range newConfig.Shards {
            if gid1 == gid {
                // has enough groups
                if len(newConfig.Groups) >= NShards {
                    // find not used group to replace it
                    for gid2, _ := range newConfig.Groups {
                        if !InArray(newConfig.Shards, gid2) {
                            newConfig.Shards[shard] = gid2
                        }
                    }
                } else {
                    // rebalance the shards
                    newConfig.Shards[shard] = FindLeastShardsOfGid(newConfig.Shards, gid)
                }
            }
        }
    }
    log.Printf("leave, newConfig:%+v", newConfig)
    sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) apply() {
    for sm.killed == false{
        select {
        case msg := <-sm.applyCh:
            if msg.CommandValid == false {
                continue
            }
            op, ok := msg.Command.(Op)
            sm.mu.Lock()
            if ok {
                if op.Type == "Join" {
                    sm.join(op.Servers)
                } else if op.Type == "Leave" {
                    sm.leave(op.GIDs)
                } else if op.Type == "Move" {
                    sm.move(op.Shard, op.GID)
                }
                sm.requestMap[op.ClientId] = op.SerialNumber
            }
            log.Printf("apply, msg:%+v, me:%d", msg, sm.me)
            sm.applyCommandIndex = msg.CommandIndex
            sm.applyIndex = msg.Index
            sm.mu.Unlock()
        }
    }
}

func (sm *ShardMaster) get(clientId int64, serialNumber int64) (int, int, bool) {
    var index int = -1
    var term int = -1
    var exist bool = false
    logs := sm.rf.GetLogs()
    for i := 0; i < len(logs); i++ {
        op, ok := logs[i].Command.(Op)
        if ok {
            if op.SerialNumber == serialNumber && op.ClientId == clientId {
                index = logs[i].CommandIndex
                term = logs[i].Term
                exist = true
            }
        }
    }
    return index, term, exist
}
