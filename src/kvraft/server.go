package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
    Key string
    Value string
    Type string
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
    requestMap map[int64]int64

	maxraftstate int // snapshot if log grows this big
    db map[string]string
    applyIndex int
    killed bool
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    //log.Printf("Get, args:%+v", args)
    defer log.Printf("end Get, args:%+v, reply:%+v, me:%d", args, reply, kv.me)
    if _, isleader := kv.rf.GetState(); isleader == false {
        reply.WrongLeader = true
        return
    }
    var op Op
    op.Key = args.Key
    op.Type = "Get"
    index, _, isLeader := kv.rf.Start(op)
    if isLeader == false {
        reply.WrongLeader = true
        return
    }
    reply.WrongLeader = false
    for kv.applyIndex < index {
        if _, isleader := kv.rf.GetState(); isleader == false {
            reply.WrongLeader = true
            return
        }
        //log.Printf("me:%d, kv.applyIndex:%d, index:%d", kv.me, kv.applyIndex, index)
        time.Sleep(5 * time.Millisecond)
    }
    reply.Value, reply.Err = kv.get(args.Key)
    //if val, ok := kv.db[args.Key]; ok {
    //    if ok {
    //        reply.Err = OK
    //        reply.Value = val
    //        return
    //    }
    //    reply.Err = ErrNoKey
    //}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    defer log.Printf("PutAppend, args:%+v, reply:%+v, me:%d", args, reply, kv.me)
    reply.Err = OK
    //defer log.Printf("end PutAppend, args:%+v", args)
    reply.Term = args.Term
    reply.Index = args.Index
    if _, isleader := kv.rf.GetState(); isleader == false {
        reply.WrongLeader = true
        return
    }
    kv.mu.Lock()
    val := kv.requestMap[args.ClientId]
    if val == args.LogId {
        reply.WrongLeader = false
        kv.mu.Unlock()
        return
    }
    kv.requestMap[args.ClientId] = args.LogId
    if args.Index > -1 {
        l, exist := kv.rf.GetLog(args.Index)
        if exist == true && l.Term == args.Term {
            if kv.applyIndex >= args.Index {
                reply.WrongLeader = false
            } else {
                reply.WrongLeader = true
            }
            return
        }
    }
    kv.mu.Unlock()
    var op Op
    op.Key = args.Key
    op.Value = args.Value
    op.Type = args.Op
    index, term, isLeader := kv.rf.Start(op)
    defer log.Printf("me:%d, kv.applyIndex:%d, index:%d, reply:%+v", kv.me, kv.applyIndex, index, reply)
    if isLeader == false {
        reply.WrongLeader = true
        return
    }
    for kv.applyIndex < index {
        //log.Printf("me:%d, kv.applyIndex:%d, index:%d", kv.me, kv.applyIndex, index)
        time.Sleep(5 * time.Millisecond)
        l, exist := kv.rf.GetLog(index)
        if exist == false || l.Term != term {
            reply.Term = term
            reply.Index = index
            reply.WrongLeader = true
            return
        }
        if _, isleader := kv.rf.GetState(); isleader == false {
            reply.Term = term
            reply.Index = index
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
    log.Printf("kv kill, me:%d", kv.me)
	kv.rf.Kill()
    kv.killed = true
    log.Printf("end kv kill, me:%d", kv.me)
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
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

	// You may need initialization code here.

    kv.requestMap = make(map[int64]int64)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    go kv.apply()
	// You may need initialization code here.

	return kv
}

func (kv *KVServer) get(key string) (string, Err){
    var val string
    var err Err
    err = ErrNoKey
    logs := kv.rf.GetAppliedLogs()
    //log.Printf("get logs:%+v", logs)
    for i := 0;i < len(logs);i++ {
        op := logs[i].Command.(Op)
        if op.Type == "Put" && op.Key == key{
            val = op.Value
            err = OK
        } else if op.Type == "Append" && op.Key == key{
            val = val + op.Value
            err = OK
        }
    }
    return val, err
}

func (kv *KVServer) apply() {
    for kv.killed == false{
        select {
        case msg := <-kv.applyCh:
            if msg.CommandValid == false {
                continue
            }
            //op := msg.Command.(Op)
            //if op.Type == "Put" {
            //    kv.db[op.Key] = op.Value
            //} else if op.Type == "Append" {
            //    if val, ok := kv.db[op.Key]; ok {
            //        if ok {
            //            kv.db[op.Key] = val + op.Value
            //        } else {
            //            kv.db[op.Key] = op.Value
            //        }
            //    }
            //}
            //log.Printf("apply, msg:%+v, me:%d", msg, kv.me)
            kv.applyIndex = msg.CommandIndex
        }
    }
}
