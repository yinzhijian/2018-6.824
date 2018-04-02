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
    "sync"
    "labrpc"
    "math/rand"
    "time"
    "log"
    "sort"
    "bytes"
    "labgob"
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
}

type Log struct {
    Term int
    Command interface{}
    CommandIndex int
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
    role Role
    currentTerm int
    votedFor int
    log []Log
    timeoutElapses int
    commitIndex int
    lastApplied int
    applyCh chan ApplyMsg

    nextIndex []int
    matchIndex []int
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
	var isleader bool
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

func (rf *Raft) GetLog(index int) (Log, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if len(rf.log) >= index {
        return rf.log[index-1], true
    }
    var log Log
    return log, false
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	 w := new(bytes.Buffer)
	 e := labgob.NewEncoder(w)
	 e.Encode(rf.currentTerm)
	 e.Encode(rf.votedFor)
	 e.Encode(rf.log)
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)
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
	var currentTerm int
	var votedFor int
	var l []Log
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&l) != nil {
	   panic("decode error")
	} else {
	   rf.currentTerm = currentTerm
	   rf.votedFor = votedFor
	   rf.log = l
       //log.Printf("recover from persister, currentTerm:%d, votedFor:%d, log:%+v, rf.me:%d",currentTerm, votedFor, l, rf.me)
	}
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
    if len(rf.log) > 0 {
        logTerm := rf.log[len(rf.log)-1].Term
        if args.LastLogTerm < logTerm {
            return
        }
        if args.LastLogTerm == logTerm && args.LastLogIndex < len(rf.log) {
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
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    //log.Printf("AppendEntries: args:%+v, rf:%+v\n", args, rf)
    defer rf.mu.Unlock()
    reply.Term = rf.currentTerm
    reply.Success = false
    if args.Term < rf.currentTerm {
        log.Printf("AppendEntries: args:%+v, reply:%+v, rf:%+v\n", args, reply, rf)
        return
    }
    rf.currentTerm = args.Term
    rf.toFollower()
    rf.persist()
    if len(rf.log) < args.PrevLogIndex {
        reply.NextLogIndex = len(rf.log)
        log.Printf("AppendEntries: args:%+v, reply:%+v, rf:%+v\n", args, reply, rf)
        return
    }
    if args.PrevLogIndex > 0 && len(rf.log) >= args.PrevLogIndex {
        if rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm {
            for i:= args.PrevLogIndex;i > 0;i-- {
                if rf.log[i - 1].Term != rf.log[args.PrevLogIndex - 1].Term {
                    reply.NextLogIndex = Max(i, 1)
                    break
                }
            }
            reply.NextLogIndex = Max(reply.NextLogIndex, 1)
            log.Printf("AppendEntries: args:%+v, reply:%+v, rf:%+v\n", args, reply, rf)
            return
        }
    }
    for i := 0; i < len(args.Entries); i++ {
        if len(rf.log) > args.PrevLogIndex + i {
            if rf.log[args.PrevLogIndex + i].Term != args.Entries[i].Term {
                rf.log = rf.log[:args.PrevLogIndex + i]
                rf.log = append(rf.log, args.Entries[i:] ...)
                break
            }
        } else {
            rf.log = append(rf.log, args.Entries[i])
        }
    }
    rf.persist()
    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = Min(args.LeaderCommit, len(rf.log))
    }
    reply.Success = true
    for rf.lastApplied < rf.commitIndex {
        rf.lastApplied += 1
        var msg ApplyMsg
        msg.CommandValid = true
        msg.Command = rf.log[rf.lastApplied - 1].Command
        msg.CommandIndex = rf.log[rf.lastApplied - 1].CommandIndex
        rf.applyCh <-msg
    }
    //log.Printf("AppendEntries: args:%+v, reply:%+v, rf:%+v\n", args, reply, rf)
}

func (rf *Raft) GetAppliedLogs() ([]Log){
    rf.mu.Lock()
    defer rf.mu.Unlock()
    var logs []Log
    if len(rf.log) > 0 && rf.lastApplied > 0 {
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
    rf.mu.Lock()
	index := len(rf.log) + 1
    var l Log
	term := rf.currentTerm
	isLeader := true
    if rf.role != Leader {
        isLeader = false
        rf.mu.Unlock()
        return index, term, isLeader
    }
    l.Term = rf.currentTerm
    l.CommandIndex = index
    l.Command = command
    rf.log = append(rf.log, l)
    rf.persist()
    rf.mu.Unlock()
    log.Printf("Start: me:%d, command:%+v,index:%d, term:%d, isLeader:%+v", rf.me, command, index, term, isLeader)

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.killed = true
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
	rf.readPersist(persister.ReadRaftState())
    for i := 0;i < len(rf.peers); i++ {
        rf.nextIndex = append(rf.nextIndex ,len(rf.log) + 1)
        rf.matchIndex = append(rf.matchIndex, 0)
    }
    go rf.startElection()
	return rf
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

        for server := range peers {
            if server == rf.me {
                continue
            }
            go func(server int) {
                rf.mu.Lock()
                args := new(AppendEntriesArgs)
                args.Term = rf.currentTerm
                args.LeaderId = rf.me
                args.PrevLogIndex = rf.nextIndex[server] - 1
                args.PrevLogTerm = 0
                if len(rf.log) > 0 {
                    args.PrevLogIndex = rf.nextIndex[server] - 1
                    if args.PrevLogIndex > 0 {
                        args.PrevLogTerm = rf.log[args.PrevLogIndex - 1].Term
                    }
                    if len(rf.log) >= rf.nextIndex[server] {
                        //log.Printf("rf.log:%+v, nextIndex[server]:%d", rf.log, rf.nextIndex[server])
                        args.Entries = rf.log[rf.nextIndex[server] - 1:]
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
                            rf.nextIndex[server] = args.Entries[len(args.Entries) -1].CommandIndex + 1
                            rf.matchIndex[server] = rf.nextIndex[server] - 1
                        } else if rf.nextIndex[server] > reply.NextLogIndex && reply.NextLogIndex > 0{
                            rf.nextIndex[server] = reply.NextLogIndex
                        } else if len(args.Entries) > 0 {
                            rf.nextIndex[server] -= 1
                        }
                    }
                    rf.mu.Unlock()
                    rf.updateCommitIndex()
                }
            }(server)
        }
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
    log.Printf("rf.me:%d, rf.lastApplied:%d, rf.commitIndex:%d, matchIndex:%d\n", rf.me ,rf.lastApplied, rf.commitIndex, matchIndex)
    if matchIndex > rf.commitIndex && rf.log[matchIndex - 1].Term == rf.currentTerm {
        rf.commitIndex = matchIndex
    }
    for rf.lastApplied < rf.commitIndex {
        log.Printf("rf.me:%d, rf.lastApplied:%d, rf.commitIndex:%d\n", rf.me ,rf.lastApplied, rf.commitIndex)
        rf.lastApplied += 1
        var msg ApplyMsg
        msg.CommandValid = true
        msg.Command = rf.log[rf.lastApplied - 1].Command
        msg.CommandIndex = rf.log[rf.lastApplied - 1].CommandIndex
        log.Printf("rf.me:%d, rf.lastApplied:%d, rf.commitIndex:%d, msg.CommandIndex:%d\n", rf.me ,rf.lastApplied, rf.commitIndex, msg.CommandIndex)
        rf.applyCh <-msg
    }
    rf.mu.Unlock()
}

func (rf *Raft) toLeader() {
    {
        rf.mu.Lock()
        defer rf.mu.Unlock()
        if rf.role == Leader {
            return
        }
        if rf.role == Follower {
            panic("to leader from follower!")
        }
        rf.role = Leader
        for server := range rf.peers {
            rf.nextIndex[server] = len(rf.log) + 1
            rf.matchIndex[server] = 0
        }
    }
    go rf.startHeartbeat()
}

func (rf *Raft) sendRequestVoteToAll(currentTerm int) {
    rf.mu.Lock()
    peers := rf.peers
    term := currentTerm
    candidateId := rf.me
    lastLogIndex := len(rf.log)
    lastLogTerm := 0
    if len(rf.log) > 0 {
        lastLogTerm = rf.log[len(rf.log)-1].Term
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
        if rf.role == Leader {
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
