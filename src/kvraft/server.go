package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
    "time"
    "bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Key string
    Value string
    Type string
    ClientId int64
    SerialNumber int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
    persister *raft.Persister
	// Your definitions here.
    requestMap map[int64]int64
    db map[string]string
    applyCommandIndex int
    applyIndex int
    killed bool
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    defer log.Printf("end Get, args:%+v, reply:%+v, me:%d", args, reply, kv.me)
    reply.WrongLeader = false
    if kv.rf.CanGet() == false {
        reply.WrongLeader = true
        return
    }
    kv.mu.Lock()
    defer kv.mu.Unlock()
    val, ok := kv.db[args.Key]
    if ok {
        reply.Value = val
        reply.Err = OK
    } else {
        reply.Value = ""
        reply.Err = ErrNoKey
    }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    //defer log.Printf("PutAppend, args:%+v, reply:%+v, me:%d,kv.requestMap:%+v", args, reply, kv.me, kv.requestMap)
    reply.Err = OK
    reply.WrongLeader = false
    //defer log.Printf("end PutAppend, args:%+v", args)
    if _, isleader := kv.rf.GetState(); isleader == false {
        reply.WrongLeader = true
        return
    }
    kv.mu.Lock()
    if val, ok := kv.requestMap[args.ClientId]; ok {
        if val >= args.SerialNumber {
            kv.mu.Unlock()
            reply.WrongLeader = false
            reply.Err = OK
            return
        }
    }
    kv.mu.Unlock()
    //kv.requestMap[args.ClientId] = args.LogId
    index, term, exist := kv.get(args.ClientId, args.SerialNumber)
    var isLeader bool = false
    if exist == false {
        var op Op
        op.Key = args.Key
        op.Value = args.Value
        op.Type = args.Op
        op.ClientId = args.ClientId
        op.SerialNumber = args.SerialNumber
        index, term, isLeader = kv.rf.Start(op)
        if isLeader == false {
            reply.WrongLeader = true
            return
        }
    }
    defer log.Printf("me:%d, kv.applyCommandIndex:%d, index:%d, reply:%+v", kv.me, kv.applyCommandIndex, index, reply)
    for kv.applyCommandIndex < index {
        //log.Printf("me:%d, kv.applyCommandIndex:%d, index:%d", kv.me, kv.applyCommandIndex, index)
        time.Sleep(5 * time.Millisecond)
        if _, isleader := kv.rf.GetState(); isleader == false {
            reply.WrongLeader = true
            return
        }
    }
    l, exist := kv.rf.GetLog(index)
    if exist == false || l.Term != term {
        reply.WrongLeader = true
        return
    }

    reply.WrongLeader = false
    reply.Err = OK
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
    kv.killed = true
    log.Printf("end kv kill, me:%d", kv.me)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
    kv.applyIndex = 0
    kv.applyCommandIndex = 0

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
    kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.db = make(map[string]string)
    kv.requestMap = make(map[int64]int64)
    go kv.apply()
    if maxraftstate > -1 {
        go kv.doSnapshot()
    }


	// You may need initialization code here.

	return kv
}

func (kv *KVServer) get(clientId int64, serialNumber int64) (int, int, bool) {
    var index int = -1
    var term int = -1
    var exist bool = false
    logs := kv.rf.GetLogs()
    //log.Printf("get logs:%+v", logs)
    for i := 0; i < len(logs); i++ {
        op, ok := logs[i].Command.(Op)
        if ok {
            if (op.Type == "Put" || op.Type == "Append") && op.SerialNumber == serialNumber && op.ClientId == clientId {
                index = logs[i].CommandIndex
                term = logs[i].Term
                exist = true
            }
        }
    }
    return index, term, exist
}

func (kv *KVServer) apply() {
    for kv.killed == false{
        select {
        case msg := <-kv.applyCh:
            if msg.CommandValid == false {
                snapshot, ok := msg.Command.([]byte)
                if ok && len(snapshot) > 0{
                    kv.mu.Lock()
                    r := bytes.NewBuffer(snapshot)
                    d := labgob.NewDecoder(r)
                    if d.Decode(&kv.applyIndex) != nil ||
                        d.Decode(&kv.applyCommandIndex) != nil ||
                        d.Decode(&kv.requestMap) != nil ||
                        d.Decode(&kv.db) != nil {
                        panic("decode error")
                    }
                    kv.mu.Unlock()
                }
                continue
            }
            op, ok := msg.Command.(Op)
            kv.mu.Lock()
            if ok {
                if op.Type == "Put" {
                    kv.db[op.Key] = op.Value
                    kv.requestMap[op.ClientId] = op.SerialNumber
                } else if op.Type == "Append" {
                    val, ok := kv.db[op.Key]
                    if ok {
                        kv.db[op.Key] = val + op.Value
                    } else {
                        kv.db[op.Key] = op.Value
                    }
                    kv.requestMap[op.ClientId] = op.SerialNumber
                }
            }
            log.Printf("apply, msg:%+v, me:%d", msg, kv.me)
            kv.applyCommandIndex = msg.CommandIndex
            kv.applyIndex = msg.Index
            kv.mu.Unlock()
        }
    }
}

func (kv *KVServer) doSnapshot() {
    for kv.killed == false{
        if kv.persister.RaftStateSize() > kv.maxraftstate {
            kv.mu.Lock()
            w := new(bytes.Buffer)
            e := labgob.NewEncoder(w)
            e.Encode(kv.applyIndex)
            e.Encode(kv.applyCommandIndex)
            e.Encode(kv.requestMap)
            e.Encode(kv.db)
            data := w.Bytes()
            applyIndex := kv.applyIndex
            kv.mu.Unlock()
            kv.rf.Snapshot(applyIndex, data)
        }
        time.Sleep(5 * time.Millisecond)
    }
}
