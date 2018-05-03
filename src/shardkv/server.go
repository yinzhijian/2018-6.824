package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "log"
import "time"
import "bytes"
import "strconv"
import "os"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Key string
    Value string
    Type string
    ClientId int64
    SerialNumber int64
    Shard int
    Gid int
	DB   map[string]string
	RequestMap   map[int64]int64
	Logs []raft.Log
}

type ShardKV struct {
	mu           sync.Mutex
	writeMu      sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    shards map[int]bool // shard => writable
    //migrateTask map[int]int // shard => new gid
    receiveTask map[int]bool // shard => ok
	mck       *shardmaster.Clerk
	config   shardmaster.Config
    nextConfigNum int // need process
    persister *raft.Persister
    requestMap map[int64]int64
    db map[string]string
    applyCommandIndex int
    applyIndex int
    applyCommandTerm int
    killed bool
    started bool
}

func Min(x, y int) int {
    if x < y {
        return x
    }
    return y
}
func DeepCopy(value interface{}) interface{} {
    if valueMap, ok := value.(map[string]interface{}); ok {
        newMap := make(map[string]interface{})
        for k, v := range valueMap {
            newMap[k] = DeepCopy(v)
        }

        return newMap
    } else if valueSlice, ok := value.([]interface{}); ok {
        newSlice := make([]interface{}, len(valueSlice))
        for k, v := range valueSlice {
            newSlice[k] = DeepCopy(v)
        }

        return newSlice
    }

    return value
}

func (kv *ShardKV) CommitMigrate(args *CommitMigrateArgs, reply *CommitMigrateReply) {
	// Your code here.
    defer log.Printf("end CommitMigrate, args:%+v, reply:%+v, me:%d, kv.gid:%d", args, reply, kv.me, kv.gid)
    reply.WrongLeader = false
    reply.Err = OK
    if _, isleader := kv.rf.GetState(); isleader == false {
        reply.WrongLeader = true
        return
    }
    if kv.started == false {
        reply.WrongLeader = true
        return
    }
    if args.Gid != kv.gid {
        reply.Err = ErrWrongGroup
        return
    }
    if _, ok := kv.shards[args.Shard]; ok {
        reply.Err = OK
        return
    }
    var op Op
    op.Type = "CommitMigrateArgs"
    op.Shard = args.Shard
    op.Gid = args.Gid
    reply.WrongLeader = !kv.write(op)
}

func (kv *ShardKV) PrepareMigrate(args *PrepareMigrateArgs, reply *PrepareMigrateReply) {
	// Your code here.
    defer log.Printf("end PrepareMigrate, args:%+v, reply:%+v, me:%d, kv.gid:%d", args, reply, kv.me, kv.gid)
    reply.WrongLeader = false
    reply.Err = OK
    //defer log.Printf("end PutAppend, args:%+v", args)
    if _, isleader := kv.rf.GetState(); isleader == false {
        reply.WrongLeader = true
        return
    }
    if kv.started == false {
        reply.WrongLeader = true
        return
    }
    if args.Gid != kv.gid {
        reply.Err = ErrWrongGroup
        return
    }
    //kv.mu.Lock()
    //defer kv.mu.Unlock()
    if _, ok := kv.shards[args.Shard]; ok {
        reply.Err = ErrMigrated
        return
    }
    if _, ok := kv.receiveTask[args.Shard]; ok {
        reply.Err = ErrPrepareMigrated
        return
    }
    var op Op
    op.Type = "PrepareMigrateArgs"
    op.Shard = args.Shard
    op.Gid = args.Gid
    op.DB = args.DB
    op.RequestMap = args.RequestMap
    op.Logs = args.Logs
    reply.WrongLeader = !kv.write(op)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    reply.WrongLeader = false
    if kv.started == false || kv.rf.IsReadable() == false {
        reply.WrongLeader = true
        return
    }
    kv.mu.Lock()
    defer kv.mu.Unlock()
    shard := key2shard(args.Key)
    defer log.Printf("end Get, args:%+v, shard:%d, reply:%+v, me:%d, gid:%d, kv.shards:%+v", args, shard, reply, kv.me, kv.gid, kv.shards)
    if access, ok := kv.shards[shard]; !ok || !access {
        reply.Err = ErrWrongGroup
        return
    }
    val, ok := kv.db[args.Key]
    if ok {
        reply.Value = val
        reply.Err = OK
    } else {
        reply.Value = ""
        reply.Err = ErrNoKey
    }
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    reply.Err = OK
    reply.WrongLeader = false
    if _, isleader := kv.rf.GetState(); isleader == false || kv.started == false {
        reply.WrongLeader = true
        return
    }
    kv.mu.Lock()
    shard := key2shard(args.Key)
    defer log.Printf("end PutAppend, args:%+v, shard:%d, kv.gid:%d", args, shard, kv.gid)
    if writable, ok := kv.shards[shard]; !ok || !writable {
        kv.mu.Unlock()
        reply.Err = ErrWrongGroup
        return
    }
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
    var op Op
    op.Key = args.Key
    op.Value = args.Value
    op.Type = args.Op
    op.ClientId = args.ClientId
    op.SerialNumber = args.SerialNumber

    _, _, exist := kv.get(op.ClientId, op.SerialNumber)
    if exist == false {
        reply.WrongLeader = !kv.write(op)
    }
    reply.Err = OK
}

func (kv *ShardKV) writeToRaft(command interface{}) bool {
    index, term, isLeader := kv.rf.Start(command)
    if isLeader == false {
        log.Printf("writeToRart not leader, me:%d, kv.gid:%d, shards:%+v", kv.me, kv.gid, kv.shards)
        return false
    }
    for kv.applyCommandIndex < index {
        if kv.killed == true {
            return false
        }
        rfterm, isleader := kv.rf.GetState()
        if isleader == false {
            log.Printf("writeToRart not leader, me:%d, kv.gid:%d, shards:%+v, rfterm:%d", kv.me, kv.gid, kv.shards, rfterm)
            return false
        }
        log.Printf("gid:%d, me:%d, kv.applyCommandIndex:%d, index:%d, isleader:%d, rfterm:%d, command:%+v", kv.gid, kv.me, kv.applyCommandIndex, index, isleader, rfterm, command)
        time.Sleep(5 * time.Millisecond)
    }
    if term != kv.applyCommandTerm {
        return false
    }
    rfterm, isleader := kv.rf.GetState()
    if isleader == false {
        log.Printf("writeToRart not leader, me:%d, kv.gid:%d, shards:%+v, rfterm:%d", kv.me, kv.gid, kv.shards, rfterm)
        return false
    }
    //l, exist := kv.rf.GetLog(index)
    //if exist == false || l.Term != term {
    //    log.Printf("writeToRart not exist, me:%d, kv.gid:%d, shards:%+v, exist:%+v, l:%+v", kv.me, kv.gid, kv.shards, exist, l)
    //    return false
    //}
    return true
}

func (kv *ShardKV) write(op Op) bool {
    kv.writeMu.Lock()
    defer kv.writeMu.Unlock()
    if op.Type == "Put" || op.Type == "Append" {
        shard := key2shard(op.Key)
        if writable, ok := kv.shards[shard]; !ok || !writable {
            return false
        }
    }
    return kv.writeToRaft(op)
}


//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
    kv.killed = true
	// Your code here, if desired.
}
func (kv *ShardKV) startUntilAllLogApplied() {
    for kv.killed == false {
        if kv.rf.IsAllLogApplied() {
            kv.started = true
            return
        }
        time.Sleep(1 * time.Millisecond)
    }
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(raft.Log{})
	//labgob.Register(PrepareMigrateArgs{})
	//labgob.Register(CommitMigrateArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.mck = shardmaster.MakeClerk(masters)
    kv.nextConfigNum = 1
    kv.applyCommandTerm = -1

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.persister = persister
    kv.db = make(map[string]string)
    kv.requestMap = make(map[int64]int64)
    //kv.migrateTask = make(map[int]int)
    kv.receiveTask = make(map[int]bool)
    kv.shards = make(map[int]bool)
    go kv.startUntilAllLogApplied()
    go kv.apply()
    go kv.updateConfig()
    if maxraftstate > -1 {
        go kv.doSnapshot()
    }

	return kv
}

func Remove(array []int, unwanted int) []int{
    var result []int
    for _, val := range array {
        if val != unwanted {
            result = append(result, val)
        }
    }
    return result
}

func InArray(array []int, search int) bool{
    for _, val := range array {
        if val == search {
            return true
        }
    }
    return false
}

func (kv *ShardKV) getServers(gid int) []string {
    var servers []string
    config := kv.mck.Query(-1)
    configNum := config.Num
    for ; configNum > 0; configNum-- {
        if servers, ok := config.Groups[gid]; ok {
            return servers
        }
        config = kv.mck.Query(configNum)
    }
    return servers
}

func (kv *ShardKV) commitMigrate(shard int, newGid int, servers []string) bool{
    log.Printf("start commitMigrate to servers:%+v, shard:%d, newGid:%d, me:%d, gid:%d", servers, shard, newGid, kv.me, kv.gid)
    if kv.commitMigrateToGid(shard, newGid, servers) {
        var op Op
        op.Key = strconv.Itoa(shard)
        op.Value = strconv.Itoa(newGid)
        op.Type = "CommitMigrateFinish"
        op.ClientId = 0
        op.SerialNumber = 0
        if kv.write(op) == false {
            return false
        }
        return true
    }
    return false
}

func (kv *ShardKV) commitMigrateToGid(shard int, newGid int, servers []string) bool{
    args := CommitMigrateArgs{}
    args.Gid = newGid
    args.Shard = shard
    if _, isleader := kv.rf.GetState(); isleader == false {
        return false
    }
    for si := 0; si < len(servers); si++ {
        srv := kv.make_end(servers[si])
        reply := new(CommitMigrateReply)
        ok := srv.Call("ShardKV.CommitMigrate", &args, reply)
        if ok && reply.WrongLeader == false {
            if reply.Err == OK {
                // success
                return true
            }
            return false
        }
    }
    return false
}

func (kv *ShardKV) migrate(shard int, newGid int, servers []string) bool {
    if _, isleader := kv.rf.GetState(); isleader == false {
        return false
    }
    log.Printf("start migrate to servers:%+v, shard:%d, newGid:%d, me:%d, kv.killed:%d, kv.gid", servers, shard, newGid, kv.me, kv.killed, kv.gid)
    // disable write first
    kv.mu.Lock()
    _, ok := kv.shards[shard];
    if !ok {
        log.Printf("not exist, shard:%d", shard)
        os.Exit(1)
        return false
    }
    kv.shards[shard] = false
    kv.mu.Unlock()
    // wait all write log applied
    var op Op
    op.Key = "shard"
    op.Value = strconv.Itoa(shard)
    op.Type = "StopWrite"
    op.ClientId = 0
    op.SerialNumber = 0
    if kv.write(op) == false {
        log.Printf("failed stop, migrate to servers:%+v, shard:%d, newGid:%d, me:%d, kv.killed:%d, kv.gid:%d", servers, shard, newGid, kv.me, kv.killed, kv.gid)
        return false
    }
    kv.mu.Lock()
    args := new(PrepareMigrateArgs)
    args.Gid = newGid
    args.Shard = shard
    args.DB = DeepCopy(kv.db).(map[string]string)
    args.RequestMap = DeepCopy(kv.requestMap).(map[int64]int64)
    //args.Logs = kv.rf.GetLogsAfter(kv.applyIndex)
    //if len(args.Logs) > 0 {
    //    log.Printf("args.Logs:%+v", args.Logs)
    //    os.Exit(1)
    //}
    kv.mu.Unlock()
    if _, isleader := kv.rf.GetState(); isleader == false {
        log.Printf("not leader, migrate to servers:%+v, shard:%d, newGid:%d, me:%d, kv.killed:%d, kv.gid:%d", servers, shard, newGid, kv.me, kv.killed, kv.gid)
        return false
    }
    for si := 0; si < len(servers); si++ {
        srv := kv.make_end(servers[si])
        reply := new(PrepareMigrateReply)
        log.Printf("send PrepareMigrate to server:%s, shard:%d, newGid:%d, me:%d, kv.gid:%d", servers[si], shard, newGid, kv.me, kv.gid)
        ok := srv.Call("ShardKV.PrepareMigrate", args, reply)
        if ok && reply.WrongLeader == false {
            // success
            if reply.Err == OK || reply.Err == ErrPrepareMigrated || reply.Err == ErrMigrated {
                // to commmit
                return kv.commitMigrate(shard, newGid, servers)
            }
            return false
        }
    }
    return false
}

func (kv *ShardKV) removeShard(shard int) bool {
    var op Op
    op.Key = "Remove"
    op.Value = strconv.Itoa(shard)
    op.Type = "Shard"
    return kv.write(op)
}

func (kv *ShardKV) addShard(shard int) bool {
    log.Printf("addShard :%d", shard)
    var op Op
    op.Key = "Add"
    op.Value = strconv.Itoa(shard)
    op.Type = "Shard"
    return kv.write(op)
}

func (kv *ShardKV) updateConfig() {
    for kv.killed == false {
        time.Sleep(20 * time.Millisecond)
        if kv.started == false {
            continue
        }
        if _, isleader := kv.rf.GetState(); isleader == false {
            continue
        }

        config := kv.mck.Query(-1)
        if config.Num > kv.nextConfigNum {
            config = kv.mck.Query(kv.nextConfigNum)
        }
        log.Printf("updateConfig config:%+v, kv.nextConfigNum:%d, kv.gid:%d, kv.shards:%+v", config, kv.nextConfigNum, kv.gid, kv.shards)
        canUpdateConfigNum := true
        if config.Num > 0 && config.Num <= kv.nextConfigNum {
            prev_config := kv.mck.Query(config.Num - 1)
            log.Printf("updateConfig prev_config:%+v, kv.nextConfigNum:%d, kv.gid:%d, kv.shards:%+v ", prev_config, kv.nextConfigNum, kv.gid, kv.shards)
            // need migrate ?
            kv.mu.Lock()
            shards := DeepCopy(kv.shards).(map[int]bool)
            //migrateTask := DeepCopy(kv.migrateTask).(map[int]int)
            kv.mu.Unlock()
            for shard, _ := range shards {
                if config.Shards[shard] == 0 {
                    kv.removeShard(shard)
                    log.Printf("kv.removeShard should not happen")
                    os.Exit(1)
                }
                if config.Shards[shard] != kv.gid && prev_config.Shards[shard] == kv.gid {
                    gid := config.Shards[shard]
                    // prepare to migrate to new gid
                    if !kv.migrate(shard, gid, config.Groups[gid]) {
                        canUpdateConfigNum = false
                    }
                }
            }
            // need add responsible for brand new shared
            for shard, gid := range config.Shards {
                if gid == kv.gid {
                    if _, ok := shards[shard]; !ok {
                        // query the prev config to know whether the shard is brand new
                        if prev_config.Shards[shard] == 0 {
                            // brand new
                            kv.addShard(shard)
                        } else {
                            // wait the prev group to migrate
                            canUpdateConfigNum = false
                            log.Printf("wait prev group:%d to migrate shard:%d, kv.gid:%d, kv.me:%d", prev_config.Shards[shard], shard, kv.gid, kv.me)
                        }
                    }
                }
            }
            log.Printf("finish updateConfig kv.nextConfigNum:%d, kv.shards:%+v, kv.gid:%d, kv.me:%d, ", kv.nextConfigNum, shards, kv.gid, kv.me)
            if canUpdateConfigNum && kv.nextConfigNum <= config.Num{
                var op Op
                op.Key = "num"
                op.Value = strconv.Itoa(kv.nextConfigNum + 1)
                op.Type = "NextConfig"
                op.ClientId = 0
                op.SerialNumber = 0
                kv.write(op)
            }
        }
    }
        // persist migrate task, shard -> new gid, status{Normal, Prepared, Committed}
        // persist responsible shards
        // compare to pre config to get the diff
        // migrate step
        // 0. stop write for not belong to me, waiting for all committed log has been applied, start migrating when every prepare work is done
        // 1. pass the db and the requestMap to every server who belongs to new group
        // 2. new group receive the info, first clean the db which key belong to new shard, then iterate the db| requestMap rewrite to new group's db| requestMap
        // 3. the migrater commit PrepareMigrate to self's raft, wait unit applied
        // 4. send FinishMigrate to new group's leader, leader commit it through raft, return ok after applied
        // 5. old group must clean not belong to themselves's shard data
}
func (kv *ShardKV) applyPrepareMigrate(op Op) {
    // clear db's data which key belong to args.shard
    log.Printf("applyPrepareMigrate, me:%d, kv.gid:%d, shards:%+v, op:%+v", kv.me, kv.gid, kv.shards, op)
    var newDB map[string]string = make(map[string]string)
    for key, value := range kv.db {
        shard := key2shard(key)
        if shard != op.Shard {
            newDB[key] = value
        }
    }
    kv.db = newDB
    // merge args.DB's data which key belongs to args.shard to kv.db
    for key, value := range op.DB {
        shard := key2shard(key)
        if shard == op.Shard {
            kv.db[key] = value
        }
    }
    // merge args.RequestMap to kv.RequestMap
    for clientId, serialNumber := range op.RequestMap {
        if kv.requestMap[clientId] < serialNumber {
            kv.requestMap[clientId] = serialNumber
        }
    }
    kv.receiveTask[op.Shard] = true
    // apply args.Logs to kv.db and kv.requestMap
    //logs := op.Logs
    //for _, log := range logs {
    //    if log.CommandValid == true {
    //        op, ok := log.Command.(Op)
    //        if ok {
    //            if op.Type == "Put" || op.Type == "Append" {
    //                shard := key2shard(op.Key)
    //                if shard == op.Shard {
    //                    kv.applyOp(op)
    //                }
    //            }
    //        }
    //    }
    //}
}

func (kv *ShardKV) removeShardData(shard int) {
    for key, _ := range kv.db {
        key_shard := key2shard(key)
        if shard == key_shard {
            delete(kv.db, key)
        }
    }
}

func (kv *ShardKV) applyCommitMigrate(op Op) {
    log.Printf("applyCommitMigrate, me:%d, kv.gid:%d, shards:%+v, op:%+v", kv.me, kv.gid, kv.shards, op)
    // delete receive task
    log.Printf("add shards before, kv.shards:%+v, shardId:%d, kv.gid:%d, kv.nextConfigNum:%d", kv.shards, op.Shard, kv.gid, kv.nextConfigNum)
    delete(kv.receiveTask, op.Shard)
    kv.shards[op.Shard] = true
    log.Printf("add shards after, kv.shards:%+v, shardId:%d, kv.gid:%d, kv.nextConfigNum:%d", kv.shards, op.Shard, kv.gid, kv.nextConfigNum)
}

func (kv *ShardKV) applyOp(op Op) {
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
    } else if op.Type == "Shard" {
        shardId, _ := strconv.Atoi(op.Value)
        if op.Key == "Add" {
            log.Printf("add shards before, kv.shards:%+v, shardId:%d, kv.gid:%d, kv.nextConfigNum:%d", kv.shards, shardId, kv.gid, kv.nextConfigNum)
            kv.shards[shardId] = true
            log.Printf("add shards after, kv.shards:%+v, shardId:%d, kv.gid:%d, kv.nextConfigNum:%d", kv.shards, shardId, kv.gid, kv.nextConfigNum)
        } else {
            log.Printf("delete shards before, kv.shards:%+v, shardId:%d, kv.gid:%d, kv.nextConfigNum:%d", kv.shards, shardId, kv.gid, kv.nextConfigNum)
            delete(kv.shards, shardId)
            kv.removeShardData(shardId)
            log.Printf("delete shards after, kv.shards:%+v, shardId:%d, kv.gid:%d, kv.nextConfigNum:%d", kv.shards, shardId, kv.gid, kv.nextConfigNum)
        }
    } else if op.Type == "StopWrite" {
        shardId, _ := strconv.Atoi(op.Value)
        if _, ok := kv.shards[shardId]; ok {
            kv.shards[shardId] = false
        }
    } else if op.Type == "CommitMigrateFinish" {
        shardId, _ := strconv.Atoi(op.Key)
        log.Printf("delete shards before, kv.shards:%+v, shardId:%d, kv.gid:%d, kv.nextConfigNum:%d", kv.shards, shardId, kv.gid, kv.nextConfigNum)
        //delete(kv.migrateTask, shardId)
        delete(kv.shards, shardId)
        kv.removeShardData(shardId)
        log.Printf("delete shards after, kv.shards:%+v, shardId:%d, kv.gid:%d, kv.nextConfigNum:%d", kv.shards, shardId, kv.gid, kv.nextConfigNum)
    } else if op.Type == "NextConfig" {
        kv.nextConfigNum, _ = strconv.Atoi(op.Value)
    } else if op.Type == "PrepareMigrateArgs" {
        kv.applyPrepareMigrate(op)
    } else if op.Type == "CommitMigrateArgs" {
        kv.applyCommitMigrate(op)
    }
}

func (kv *ShardKV) apply() {
    for kv.killed == false {
        select {
        case msg := <-kv.applyCh:
            if msg.CommandValid == false {
                snapshot, ok := msg.Command.([]byte)
                if ok && len(snapshot) > 0{
                    kv.mu.Lock()
                    log.Printf("snapshot before kv:%+v", kv)
                    r := bytes.NewBuffer(snapshot)
                    d := labgob.NewDecoder(r)
                    var applyIndex int
                    if d.Decode(&applyIndex) == nil {
                        if applyIndex <= kv.applyIndex {
                            kv.mu.Unlock()
                            continue
                        }
                    }
                    kv.applyIndex = applyIndex
                    if d.Decode(&kv.applyCommandIndex) != nil ||
                        d.Decode(&kv.applyCommandTerm) != nil ||
                        d.Decode(&kv.nextConfigNum) != nil ||
                        d.Decode(&kv.shards) != nil ||
                        //d.Decode(&kv.migrateTask) != nil ||
                        d.Decode(&kv.receiveTask) != nil ||
                        d.Decode(&kv.requestMap) != nil ||
                        d.Decode(&kv.db) != nil {
                        panic("decode error")
                    }
                    log.Printf("snapshot kv:%+v", kv)
                    kv.mu.Unlock()
                }
                continue
            }
            kv.mu.Lock()
            op, ok := msg.Command.(Op)
            if ok {
                kv.applyOp(op)
            }
            log.Printf("apply, msg:%+v, me:%d, kv.gid:%d, kv.shards:%+v", msg, kv.me, kv.gid, kv.shards)
            kv.applyCommandIndex = msg.CommandIndex
            kv.applyIndex = msg.Index
            kv.applyCommandTerm = msg.CommandTerm
            kv.mu.Unlock()
        }
    }
}

func (kv *ShardKV) get(clientId int64, serialNumber int64) (int, int, bool) {
    var index int = -1
    var term int = -1
    var exist bool = false
    logs := kv.rf.GetLogs()
    //log.Printf("get logs:%+v", logs)
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

func (kv *ShardKV) doSnapshot() {
    for kv.killed == false {
        time.Sleep(5 * time.Millisecond)
        if kv.started == false {
            continue
        }
        if kv.persister.RaftStateSize() > kv.maxraftstate {
            kv.mu.Lock()
            w := new(bytes.Buffer)
            e := labgob.NewEncoder(w)
            e.Encode(kv.applyIndex)
            e.Encode(kv.applyCommandIndex)
            e.Encode(kv.applyCommandTerm)
            e.Encode(kv.nextConfigNum)
            e.Encode(kv.shards)
            //e.Encode(kv.migrateTask)
            e.Encode(kv.receiveTask)
            e.Encode(kv.requestMap)
            e.Encode(kv.db)
            data := w.Bytes()
            applyIndex := kv.applyIndex
            kv.mu.Unlock()
            kv.rf.Snapshot(applyIndex, data)
        }
    }
}
