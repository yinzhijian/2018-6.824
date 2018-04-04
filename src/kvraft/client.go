package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "log"
import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
    leader int
    clientId int64
    serialNumber int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
    ck.leader = int(nrand()) % len(servers)
    ck.clientId = nrand()
    ck.serialNumber = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
    args := new(GetArgs)
    args.Key = key
    for {
        reply := new(GetReply)
        ck.leader = ck.leader % len(ck.servers)
        ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
        if ok == false {
            ck.leader++
            continue
        }
        if reply.WrongLeader {
            ck.leader++
            time.Sleep(1 * time.Millisecond)
            continue
        }
        if reply.Err == ErrNoKey {
            return ""
        } else {
            return reply.Value
        }
    }
    return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
    log.Printf("client PutAppend, key:%+v, value:%+v, op:%d", key, value, op)
    ck.serialNumber += 1
    args := new(PutAppendArgs)
    args.Key = key
    args.Value = value
    args.Op = op
    args.ClientId = ck.clientId
    args.SerialNumber = ck.serialNumber
    for {
        reply := new(PutAppendReply)
        ck.leader = ck.leader % len(ck.servers)
        ok := ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
        if ok == false {
            ck.leader++
            continue
        }
        if reply.WrongLeader {
            ck.leader++
            continue
        }
        return
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
