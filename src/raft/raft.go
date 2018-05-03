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
    "os"
    "sync"
    "labrpc"
    "math/rand"
    "time"
    "log"
    "sort"
    "bytes"
    "labgob"
    "sync/atomic"
)

// import "bytes"
// import "labgob"


type Role int
// iota 初始化后会自动递增
const (
    Follower Role = iota // value --> 0
    Candidate              // value --> 1
    Leader            // value --> 2
)



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm int
    Index int
}

type Noop struct {
    Noop string
}

type Log struct {
    Term int
    Index int
    Command interface{}
    CommandIndex int
    CommandValid bool
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    killed bool
    started bool
    role Role
    currentTerm int
    votedFor int
    log []Log
    timeoutElapses int
    commitIndex int
    lastApplied int
    applyCh chan ApplyMsg
    noopTerm int
    lastHeartbeat int64

    nextIndex []int
    matchIndex []int

    lastIncludedIndex int
    lastIncludedTerm int
    lastIncludedCommandIndex int
}
func Min(x, y int) int {
    if x < y {
        return x
    }
    return y
}

func Max(x, y int) int {
    if x > y {
        return x
    }
    return y
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool = false
	// Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    term = rf.currentTerm
    if rf.role == Leader {
        isleader = true
    }
    //log.Printf("GetState me:%+v\n",rf)
	return term, isleader
}

func (rf *Raft) getLog(index int) (Log, bool) {
    if len(rf.log) > 0 && rf.log[0].Index < index && rf.log[len(rf.log) - 1].Index >= index {
        // maybe change to binary search
        for i := len(rf.log) - 1; i >= 0; i-- {
            if rf.log[i].Index == index {
                return rf.log[i], true
            }
        }
    }
    var log Log
    return log, false
}

func (rf *Raft) GetLog(index int) (Log, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.getLog(index)
}


func (rf *Raft) Snapshot(index int, snapshot []byte) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if len(rf.log) <= 0 || index <= rf.lastIncludedIndex {
        return
    }
    log.Printf("Snapshot index(%d) rf.lastApplied(%d) rf.lastIncludedIndex(%d) len(rf.log)(%d) (index - 1 - rf.lastIncludedIndex)(%d)", index, rf.lastApplied, rf.lastIncludedIndex, len(rf.log), index - 1 - rf.lastIncludedIndex)
    if index > rf.lastApplied {
        log.Printf("Snapshot index(%d) should not greater than rf.lastApplied(%d)", index, rf.lastApplied)
        os.Exit(1)
    }
    if rf.log[index - 1 - rf.lastIncludedIndex].Index != index {
        log.Printf("Snapshot rf.log.index(%d) not equal to index(%d), rf.log:%+v", rf.log[index - 1 - rf.lastIncludedIndex].Index, index, rf.log)
        os.Exit(1)
    }
    log.Printf("Snapshot rf.log[index - 1 - rf.lastIncludedIndex]:%+v", rf.log[index - 1 - rf.lastIncludedIndex])

    rf.lastIncludedTerm = rf.log[index - 1 - rf.lastIncludedIndex].Term
    rf.lastIncludedCommandIndex = rf.log[index - 1 - rf.lastIncludedIndex].CommandIndex
    if len(rf.log) == (index - rf.lastIncludedIndex) {
        rf.log = rf.log[:0]
    } else if len(rf.log) > (index - rf.lastIncludedIndex) {
        rf.log = rf.log[index - rf.lastIncludedIndex:]
    } else {
        log.Printf("Snapshot len(rf.log):%s < (index - rf.lastIncludedIndex):%d, rf.log:%+v", len(rf.log), index - rf.lastIncludedIndex, rf.log)
        os.Exit(1)
    }
    rf.lastIncludedIndex = index
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
    log.Printf("Snapshot end")
}

func (rf *Raft) encodeRaftState() []byte {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedCommandIndex)
	 return w.Bytes()
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	 rf.persister.SaveRaftState(rf.encodeRaftState())
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
	   d.Decode(&rf.votedFor) != nil ||
	   d.Decode(&rf.log) != nil ||
	   d.Decode(&rf.lastIncludedIndex) != nil ||
	   d.Decode(&rf.lastIncludedTerm) != nil ||
	   d.Decode(&rf.lastIncludedCommandIndex) != nil {
	   panic("decode error")
	}
    rf.lastApplied = rf.lastIncludedIndex
    rf.commitIndex = rf.lastIncludedIndex
}

type InstallSnapshotArgs struct {
    Term int
    LeaderId int
    LastIncludedIndex int
    LastIncludedTerm int
    LastIncludedCommandIndex int
    Data []byte
}

type InstallSnapshotReply struct {
    Term int
}

type AppendEntriesArgs struct {
    Term int
    LeaderId int
    PrevLogIndex int
    PrevLogTerm int
    Entries []Log
    LeaderCommit int
}
type AppendEntriesReply struct {
    Term int
    Success bool
    NextLogIndex int
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term int
    VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    //log.Printf("RequestVote: args:%+v, rf:%+v\n", args, rf)
    defer rf.mu.Unlock()
    reply.Term = rf.currentTerm
    reply.VoteGranted = false
    if args.Term < rf.currentTerm {
        return
    }
    if rf.started == false {
        return
    }
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.toFollower()
        rf.votedFor = -1
        rf.persist()
    }
    if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
        return
    }
    // up-to-date log
    if rf.getLastLogIndex() > 0 {
        logTerm := rf.getLastLogTerm()
        if args.LastLogTerm < logTerm {
            return
        }
        if args.LastLogTerm == logTerm && args.LastLogIndex < rf.getLastLogIndex() {
            return
        }
    }
    reply.VoteGranted = true
    rf.votedFor = args.CandidateId
    rf.toFollower()
    //log.Printf("RequestVote: args:%+v, reply:%+v, me:%+v\n",args,reply,rf)
    return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    //log.Printf("sendAppendEntries: args:%+v, server:%+v\n", args, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    log.Printf("send InstallSnapshot server:%d, args:%+v", server, args)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}


func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    //log.Printf("recevie InstallSnapshot args:%+v", args)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    reply.Term = rf.currentTerm
    defer log.Printf("InstallSnapshot args:%+v, rf:%+v, reply:%+v", args, rf, reply)
    if args.Term < rf.currentTerm {
        return
    }
    if rf.started == false {
        return
    }
    rf.currentTerm = args.Term
    rf.toFollower()
    rf.persist()
    if rf.lastApplied > args.LastIncludedIndex {
        return
    }
    //if rf.commitIndex > args.LastIncludedIndex {
    //    return
    //}
    if rf.lastIncludedIndex >= args.LastIncludedIndex {
        return
    }
    if rf.getLastLogIndex() < args.LastIncludedIndex {
        rf.log = rf.log[:0]
    } else {
        includedLog := rf.log[args.LastIncludedIndex - 1 - rf.lastIncludedIndex]
        if includedLog.Index == args.LastIncludedIndex && includedLog.Term == args.LastIncludedTerm {
            rf.log = rf.log[args.LastIncludedIndex - rf.lastIncludedIndex:]
        } else {
            rf.log = rf.log[:0]
            //log.Printf("InstallSnapshot includedLog.Index(%d) != args.LastIncludedIndex(%d) || includedLog.Term(%d) == args.LastIncludedTerm(%d)", includedLog.Index, args.LastIncludedIndex, includedLog.Term, args.LastIncludedTerm)
            //os.Exit(1)
        }
    }
    rf.lastIncludedIndex = args.LastIncludedIndex
    rf.lastIncludedTerm = args.LastIncludedTerm
    rf.lastIncludedCommandIndex = args.LastIncludedCommandIndex
    rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
    rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
    rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), args.Data)
    var msg ApplyMsg
    msg.CommandValid = false
    msg.Command = args.Data
    msg.CommandIndex = rf.lastIncludedCommandIndex
    msg.Index = rf.lastIncludedIndex
    msg.CommandTerm = rf.lastIncludedTerm
    rf.applyCh <-msg
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    //log.Printf("begin AppendEntries: args:%+v, rf:%+v\n", args, rf)
    defer rf.mu.Unlock()
    reply.Term = rf.currentTerm
    reply.Success = false
    if args.Term < rf.currentTerm {
        log.Printf("AppendEntries: args:%+v, reply:%+v, rf:%+v\n", args, reply, rf)
        return
    }
    if rf.started == false {
        return
    }
    rf.currentTerm = args.Term
    rf.toFollower()
    rf.persist()
    if rf.getLastLogIndex() < args.PrevLogIndex {
        reply.NextLogIndex = rf.getLastLogIndex() + 1
        log.Printf("AppendEntries: args:%+v, reply:%+v, rf:%+v\n", args, reply, rf)
        return
    }
    if args.PrevLogIndex > 0 && rf.lastIncludedIndex > args.PrevLogIndex {
        reply.NextLogIndex = rf.lastIncludedIndex + 1
        log.Printf("AppendEntries: args:%+v, reply:%+v, rf:%+v\n", args, reply, rf)
        return
    }
    if args.PrevLogIndex > 0 && rf.getLastLogIndex() >= args.PrevLogIndex {
        if rf.lastIncludedIndex == args.PrevLogIndex {
            if rf.lastIncludedTerm != args.PrevLogTerm {
                log.Printf("AppendEntries: args:%+v, rf:%+v, rf.lastIncludedTerm != args.PrevLogTerm should not happened\n", args, rf)
                //os.Exit(1)
                // sender will send a snapshot
                return
            }
        } else if rf.log[args.PrevLogIndex - 1 - rf.lastIncludedIndex].Term != args.PrevLogTerm {
            for i:= args.PrevLogIndex;i > rf.lastIncludedIndex;i-- {
                if rf.log[i - 1 - rf.lastIncludedIndex].Term != rf.log[args.PrevLogIndex - 1 - rf.lastIncludedIndex].Term {
                    reply.NextLogIndex = Max(i, 1)
                    return
                }
            }
            reply.NextLogIndex = Max(reply.NextLogIndex, 1)
            log.Printf("AppendEntries: args:%+v, reply:%+v, rf:%+v\n", args, reply, rf)
            return
        }
    }
    for i := 0; i < len(args.Entries); i++ {
        if rf.getLastLogIndex() > args.PrevLogIndex + i {
            if rf.log[args.PrevLogIndex + i - rf.lastIncludedIndex].Term != args.Entries[i].Term {
                rf.log = rf.log[:args.PrevLogIndex + i - rf.lastIncludedIndex]
                rf.log = append(rf.log, args.Entries[i:] ...)
                break
            }
        } else {
            rf.log = append(rf.log, args.Entries[i])
        }
    }
    rf.persist()
    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
    }
    reply.Success = true
    for rf.lastApplied < rf.commitIndex {
        rf.lastApplied += 1
        var msg ApplyMsg
        msg.CommandValid = rf.log[rf.lastApplied - 1 - rf.lastIncludedIndex].CommandValid
        msg.Command = rf.log[rf.lastApplied - 1 - rf.lastIncludedIndex].Command
        msg.CommandIndex = rf.log[rf.lastApplied - 1 - rf.lastIncludedIndex].CommandIndex
        msg.Index = rf.log[rf.lastApplied - 1 - rf.lastIncludedIndex].Index
        msg.CommandTerm = rf.log[rf.lastApplied - 1 - rf.lastIncludedIndex].Term
        if msg.CommandValid == false {
            val, ok := msg.Command.(string)
            if ok && val == "noop"{
                rf.noopTerm = rf.log[rf.lastApplied - 1 - rf.lastIncludedIndex].Term
            }
        }
        rf.applyCh <-msg
    }
    //log.Printf("AppendEntries: args:%+v, reply:%+v, rf:%+v\n", args, reply, rf)
}

func (rf *Raft) GetLogsAfter(index int) ([]Log){
    rf.mu.Lock()
    defer rf.mu.Unlock()
    var logs []Log
    if len(rf.log) > 0 && rf.log[len(rf.log) - 1].Index > index {
        return rf.log[index - rf.lastIncludedIndex:]
    }
    return logs
}

func (rf *Raft) GetLogs() ([]Log){
    rf.mu.Lock()
    defer rf.mu.Unlock()
    var logs []Log
    if len(rf.log) > 0 {
        return rf.log[0:]
    }
    return logs
}

func (rf *Raft) GetAppliedLogs() ([]Log){
    rf.mu.Lock()
    defer rf.mu.Unlock()
    var logs []Log
    if rf.getLastLogIndex() > 0 && rf.lastApplied > 0 {
        return rf.log[0:rf.lastApplied - 1]
    }
    return logs
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).


	return rf.start(command, true)
}

func (rf *Raft) start(command interface{}, commandVaild bool) (int, int, bool) {
    rf.mu.Lock()
	index := rf.getLastLogIndex() + 1
    var l Log
	term := rf.currentTerm
	isLeader := true
    if rf.role != Leader {
        isLeader = false
        rf.mu.Unlock()
        return index, term, isLeader
    }
    l.Term = rf.currentTerm
    if commandVaild {
        l.CommandIndex = rf.getLastCommandIndex() + 1
    } else {
        l.CommandIndex = rf.getLastCommandIndex()
    }
    l.Index = index
    l.Command = command
    l.CommandValid = commandVaild
    rf.log = append(rf.log, l)
    rf.persist()
    rf.mu.Unlock()
    log.Printf("Start: me:%d, command:%+v,index:%d, term:%d, isLeader:%+v", rf.me, command, index, term, isLeader)

	// Your code here (2B).


	return l.CommandIndex, term, isLeader
}

func (rf *Raft) IsAllLogApplied() bool {
    if len(rf.log) > 0 {
        return rf.lastApplied == rf.log[len(rf.log) -1].Index
    }
    return rf.lastApplied == rf.lastIncludedIndex
}

func (rf *Raft) getLastCommandIndex() int {
    for i := len(rf.log) - 1; i >= 0; i-- {
        if rf.log[i].CommandValid {
            return rf.log[i].CommandIndex
        }
    }
    return rf.lastIncludedCommandIndex
}
//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
    //rf.mu.Lock()
    //defer rf.mu.Unlock()
    rf.killed = true
}

func (rf *Raft) IsReadable() bool {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.role != Leader {
        return false
    }
    if rf.currentTerm != rf.noopTerm {
        return false
    }
    now := time.Now().UnixNano()/1e6
    if now - rf.lastHeartbeat > 100 {
        return false
    }
    return true
}

func GenerateRangeNum(min, max int) int {
    rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	randNum := rand.Intn(max - min)
	randNum = randNum + min
	return randNum
}

func (rf *Raft) toFollower() {
    rf.resetElectionTimeout();
    rf.role = Follower
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
    log.SetFlags(log.LstdFlags | log.Lshortfile)
	rf := &Raft{}
    rf.started = false
	rf.peers = peers
	rf.persister = persister
	rf.me = me
    rf.killed = false
    rf.toFollower()
    rf.votedFor = -1
    rf.commitIndex = 0
    rf.lastApplied = 0
    rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
    // for Snapshot
    rf.lastIncludedIndex = 0
    rf.lastIncludedTerm = 0
    rf.lastIncludedCommandIndex = 0
	rf.readPersist(persister.ReadRaftState())
    for i := 0;i < len(rf.peers); i++ {
        rf.nextIndex = append(rf.nextIndex ,rf.getLastLogIndex() + 1)
        rf.matchIndex = append(rf.matchIndex, 0)
    }
    snapshot := rf.persister.ReadSnapshot()
    go func(snapshot []byte) {
        rf.mu.Lock()
        var msg ApplyMsg
        msg.CommandValid = false
        msg.Command = snapshot
        msg.CommandIndex = rf.lastIncludedCommandIndex
        msg.Index = rf.lastIncludedIndex
        msg.CommandTerm = rf.lastIncludedTerm
        rf.applyCh <-msg
        rf.started = true
        rf.mu.Unlock()
    }(snapshot)
    go rf.startElection()
	return rf
}

func (rf *Raft) getLastLogIndex() int {
    if len(rf.log) > 0 {
        return rf.log[len(rf.log) - 1].Index
    }
    return rf.lastIncludedIndex
}

func (rf *Raft) getLastLogTerm() int {
    if len(rf.log) > 0 {
        return rf.log[len(rf.log) - 1].Term
    }
    return rf.lastIncludedTerm
}

func (rf *Raft) installSnapshot(server int) {
    rf.mu.Lock()
    if rf.role != Leader {
        rf.mu.Unlock()
        return
    }
    log.Printf("prepare send InstallSnapshot server:%d, rf:%+v", server, rf)
    args := new(InstallSnapshotArgs)
    reply := new(InstallSnapshotReply)
    args.Term = rf.currentTerm
    args.LeaderId = rf.me
    args.LastIncludedIndex = rf.lastIncludedIndex
    args.LastIncludedTerm = rf.lastIncludedTerm
    args.LastIncludedCommandIndex = rf.lastIncludedCommandIndex
    log.Printf("prepare read snapshot")
    args.Data = rf.persister.ReadSnapshot()
    rf.mu.Unlock()
    if len(args.Data) <= 0 {
        return
    }
    ok := rf.sendInstallSnapshot(server, args, reply)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if ok {
        if reply.Term > rf.currentTerm {
            rf.currentTerm = reply.Term
            rf.toFollower()
            rf.persist()
            return
        }
        if args.LastIncludedIndex != rf.lastIncludedIndex {
            return
        }
        if args.LastIncludedTerm != rf.lastIncludedTerm {
            return
        }
        rf.nextIndex[server] = args.LastIncludedIndex + 1
    }
}

func (rf *Raft) doHeartbeat(peers []*labrpc.ClientEnd) {
    var success int32 = 0
    var replied int32 = 0
    for server := range peers {
        if server == rf.me {
            continue
        }
        go func(server int) {
            canDoSnapshot := false
            defer atomic.AddInt32(&replied, 1)
            rf.mu.Lock()
            args := new(AppendEntriesArgs)
            args.Term = rf.currentTerm
            args.LeaderId = rf.me
            args.PrevLogIndex = rf.nextIndex[server] - 1
            args.PrevLogTerm = rf.lastIncludedTerm
            //log.Printf("doHeartbeat rf.me(%d) rf.lastIncludedIndex(%d) > args.PrevLogIndex(%d), rf.getLastLogIndex()(%d)", rf.me, rf.lastIncludedIndex, args.PrevLogIndex, rf.getLastLogIndex())
            if rf.lastIncludedIndex > args.PrevLogIndex {
                // TODO snapshot
                canDoSnapshot = true
            } else if rf.getLastLogIndex() > 0 {
                if rf.lastIncludedIndex == args.PrevLogIndex {
                    args.PrevLogTerm = rf.lastIncludedTerm
                } else if args.PrevLogIndex > 0 && rf.getLastLogIndex() >= args.PrevLogIndex {
                    args.PrevLogTerm = rf.log[args.PrevLogIndex - 1 - rf.lastIncludedIndex].Term
                }
                if rf.getLastLogIndex() >= rf.nextIndex[server] {
                    //log.Printf("rf.log:%+v, nextIndex[server]:%d", rf.log, rf.nextIndex[server])
                    args.Entries = rf.log[rf.nextIndex[server] - 1 - rf.lastIncludedIndex:]
                }
            }
            args.LeaderCommit = rf.commitIndex
            rf.mu.Unlock()
            reply := new(AppendEntriesReply)
            ok := rf.sendAppendEntries(server, args, reply)
            if ok == true {
                rf.mu.Lock()
                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.toFollower()
                    rf.persist()
                    rf.mu.Unlock()
                    return
                }
                if rf.role != Leader {
                    rf.mu.Unlock()
                    return
                }
                if args.PrevLogIndex == rf.nextIndex[server] - 1 {
                    if reply.Success == true && len(args.Entries) > 0{
                        rf.nextIndex[server] = args.Entries[len(args.Entries) -1].Index + 1
                        rf.matchIndex[server] = rf.nextIndex[server] - 1
                    } else if rf.nextIndex[server] > reply.NextLogIndex && reply.NextLogIndex > 0{
                        rf.nextIndex[server] = reply.NextLogIndex
                    } else if len(args.Entries) > 0 {
                        rf.nextIndex[server] -= 1
                    }
                }
                rf.mu.Unlock()
                rf.updateCommitIndex()
                atomic.AddInt32(&success, 1)
            }
            if canDoSnapshot {
                rf.installSnapshot(server)
            }
        }(server)
    }
    for atomic.LoadInt32(&replied) < int32(len(peers) - 1) {
        //log.Printf("replied:%d,peers:%d",  atomic.LoadInt32(&replied), len(peers))
        time.Sleep(time.Millisecond * 2)
    }
    if atomic.LoadInt32(&success) >= int32(len(peers) / 2) {
        rf.lastHeartbeat = time.Now().UnixNano()/1e6
    }
}

func (rf *Raft) startHeartbeat() {
    for rf.role == Leader && rf.killed == false{
        rf.mu.Lock()
        peers := rf.peers
        if rf.role != Leader {
            rf.mu.Unlock()
            return
        }
        rf.mu.Unlock()
        go rf.doHeartbeat(peers)

        time.Sleep(50 * time.Millisecond)
    }
}
func (rf *Raft) updateCommitIndex() {
    rf.mu.Lock()
    var matchIndexs []int
    for server := range rf.peers {
        if rf.me == server {
            continue
        }
        matchIndexs = append(matchIndexs, rf.matchIndex[server])
    }
    sort.Ints(matchIndexs)
    matchIndex := matchIndexs[len(matchIndexs)/2]
    //log.Printf("rf.me:%d, rf.lastApplied:%d, rf.commitIndex:%d, matchIndex:%d\n", rf.me ,rf.lastApplied, rf.commitIndex, matchIndex)
    if matchIndex > rf.commitIndex && rf.log[matchIndex - 1 - rf.lastIncludedIndex].Term == rf.currentTerm {
        rf.commitIndex = matchIndex
    }
    for rf.lastApplied < rf.commitIndex {
        //log.Printf("rf.me:%d, rf.lastApplied:%d, rf.commitIndex:%d\n", rf.me ,rf.lastApplied, rf.commitIndex)
        rf.lastApplied += 1
        var msg ApplyMsg
        msg.CommandValid = rf.log[rf.lastApplied - 1 - rf.lastIncludedIndex].CommandValid
        msg.Command = rf.log[rf.lastApplied - 1 - rf.lastIncludedIndex].Command
        msg.CommandIndex = rf.log[rf.lastApplied - 1 - rf.lastIncludedIndex].CommandIndex
        msg.Index = rf.log[rf.lastApplied - 1 - rf.lastIncludedIndex].Index
        msg.CommandTerm = rf.log[rf.lastApplied - 1 - rf.lastIncludedIndex].Term
        if msg.CommandValid == false {
            val, ok := msg.Command.(string)
            if ok && val == "noop"{
                rf.noopTerm = rf.log[rf.lastApplied - 1 - rf.lastIncludedIndex].Term
            }
        }
        //log.Printf("rf.me:%d, rf.lastApplied:%d, rf.commitIndex:%d, msg.CommandIndex:%d\n", rf.me ,rf.lastApplied, rf.commitIndex, msg.CommandIndex)
        rf.applyCh <-msg
    }
    rf.mu.Unlock()
}

func (rf *Raft) toLeader() {
    {
        rf.mu.Lock()
        if rf.role == Leader {
            rf.mu.Unlock()
            return
        }
        if rf.role == Follower {
            panic("to leader from follower!")
        }
        rf.role = Leader
        for server := range rf.peers {
            rf.nextIndex[server] = rf.getLastLogIndex() + 1
            rf.matchIndex[server] = 0
        }
        rf.mu.Unlock()
    }
    rf.start("noop", false)
    go rf.startHeartbeat()
}

func (rf *Raft) sendRequestVoteToAll(currentTerm int) {
    rf.mu.Lock()
    peers := rf.peers
    term := currentTerm
    candidateId := rf.me
    lastLogIndex := rf.getLastLogIndex()
    lastLogTerm := 0
    if lastLogIndex > 0 {
        lastLogTerm = rf.getLastLogTerm()
    }
    rf.mu.Unlock()
    votedNum := 1
    neededVoted := len(peers)/2 + 1
    c := make(chan *RequestVoteReply)
    for server := range peers {
        if server == rf.me {
            continue
        }
        go func(server int) {
            args := new(RequestVoteArgs)
            args.Term = term
            args.CandidateId = candidateId
            args.LastLogIndex = lastLogIndex
            args.LastLogTerm = lastLogTerm

            reply := new(RequestVoteReply)
            ok := rf.sendRequestVote(server, args, reply)
            if ok == true {
                c <-reply
            } else {
                c <-nil
            }
        }(server)
    }
    for i := 1; i < len(peers); i++ {
        reply := <-c
        if reply != nil {
            if reply.VoteGranted == true {
                votedNum += 1
                if votedNum >= neededVoted && rf.role == Candidate {
                    rf.toLeader()
                    return
                }
            } else {
                rf.mu.Lock()
                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.toFollower()
                    rf.persist()
                }
                rf.mu.Unlock()
            }
        }
    }
}

func (rf *Raft) resetElectionTimeout() {
    rf.timeoutElapses = GenerateRangeNum(150,300)
}

func (rf *Raft) startElection() {
    for rf.killed == false {
        time.Sleep(time.Millisecond * 5)
        if rf.role == Leader || rf.started == false{
            continue
        }
        rf.mu.Lock()
        if rf.role != Leader {
            rf.timeoutElapses -= 5
            if rf.timeoutElapses <= 0 {
                if rf.role == Leader {
                    rf.mu.Unlock()
                    continue
                }
                rf.role = Candidate
                rf.currentTerm += 1
                rf.votedFor = rf.me
                rf.persist()
                //log.Printf("startElection: rf:%+v\n", rf)
                rf.mu.Unlock()
                rf.sendRequestVoteToAll(rf.currentTerm)
                rf.mu.Lock()
                rf.resetElectionTimeout();
                rf.mu.Unlock()
                continue
            }
        }
        rf.mu.Unlock()
    }
}

func GetTimestampInMilli() int64 {
   return int64(time.Now().UnixNano() / (1000 * 1000)) // ms
}
