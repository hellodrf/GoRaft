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
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)
import "sync/atomic"

// import "bytes"
// import "../labgob"

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex   // Lock to protect shared access to this peer's state
	peers     []*ClientEnd // RPC end points of all peers
	persister *Persister   // Object to hold this peer's persisted state
	me        int          // this peer's index into peers[]
	dead      int32        // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent
	currentTerm int32
	votedFor    int32
	logs        []LogEntry
	state       int32

	// Volatile
	innitMu     sync.Mutex
	commitIndex int32
	lastApplied int32

	nextIndex  []int32
	matchIndex []int32

	// Channels
	electionChan  chan int
	heartbeatChan chan int
}

type LogEntry struct {
	Term    int32
	Command interface{}
}

func (rf *Raft) Log(s string) {
	log.Println("Node", strconv.Itoa(rf.me)+" (S"+strconv.Itoa(int(atomic.LoadInt32(&rf.state)))+
		"|T"+strconv.Itoa(int(atomic.LoadInt32(&rf.currentTerm)))+
		"|L"+strconv.Itoa(int(atomic.LoadInt32(&rf.votedFor)))+"): "+s)
}

func (rf *Raft) initialiseVolatile() {
	//rf.innitMu.Lock()
	rf.nextIndex = []int32{}
	rf.matchIndex = []int32{}
	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, 0)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	//rf.innitMu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

	// Your code here (2A).
	term = int(atomic.LoadInt32(&rf.currentTerm))
	isLeader = atomic.LoadInt32(&rf.state) == 1

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int32
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
}

func (r *AppendEntriesArgs) String() string {
	if r.Entries == nil {
		return "{RPC:Heartbeat|REQUEST|N" + strconv.Itoa(r.LeaderId) + "|T" + strconv.Itoa(int(r.Term)) + "}"
	} else {
		return "{RPC:LogEntries|REQUEST|N" + strconv.Itoa(r.LeaderId) + "|T" + strconv.Itoa(int(r.Term)) + "}"
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	mu.Lock() // lock currentTerm
	if args.Term < atomic.LoadInt32(&rf.currentTerm) {
		rf.Log("AppendEntries RPC outdated! - L" + strconv.Itoa(args.LeaderId) + "|T" + strconv.Itoa(int(args.Term)))
		reply.Success = false
		reply.Term = atomic.LoadInt32(&rf.currentTerm)
		mu.Unlock()
		return
	}

	if args.Entries == nil { // heartbeat
		mu.Unlock()
		rf.Log("Heartbeat received from " + strconv.Itoa(args.LeaderId))
		atomic.StoreInt32(&rf.votedFor, int32(args.LeaderId))

		if atomic.LoadInt32(&rf.state) != -1 {
			rf.checkAbdicate(args.Term, "HB", "REQUEST", int(args.LeaderId))
			rf.Log("Accepted leader " + strconv.Itoa(args.LeaderId))
		}

		rf.electionChan <- 1
		reply.Success = true
		reply.Term = atomic.LoadInt32(&rf.currentTerm)
		return
	} else { // log entries
		rf.checkAbdicate(args.Term, "LOG", "REQUEST", int(args.LeaderId))
	}

	reply.Success = false
	reply.Term = atomic.LoadInt32(&rf.currentTerm)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.checkAbdicate(reply.Term, "VOTE", "REQUEST", int(args.CandidateId))

	mu.Lock() // lock currentTerm
	defer mu.Unlock()
	reply.Term = atomic.LoadInt32(&rf.currentTerm)
	if args.Term < atomic.LoadInt32(&rf.currentTerm) {
		rf.Log("Devoted " + strconv.Itoa(int(args.CandidateId)) + " (Term_Error)")
		reply.VoteGranted = false
		return
	}

	vt := atomic.LoadInt32(&rf.votedFor)
	if vt == -1 || args.Term > atomic.LoadInt32(&rf.currentTerm) {
		reply.VoteGranted = true
		atomic.StoreInt32(&rf.votedFor, args.CandidateId)
		rf.Log("Voted " + strconv.Itoa(int(args.CandidateId)))
		return
	} else {
		rf.Log("Devoted " + strconv.Itoa(int(args.CandidateId)) + " (Already_Voted)")
		reply.VoteGranted = false
		return
	}
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an Election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := atomic.LoadInt32(&rf.state) == 1

	// Your code here (2B).
	if !isLeader || rf.killed() {
		return index, term, isLeader
	}

	index = int(atomic.LoadInt32(&rf.commitIndex)) + 1
	term = int(atomic.LoadInt32(&rf.currentTerm))

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.electionChan <- -1
	rf.heartbeatChan <- -1
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) checkAbdicate(Term int32, RPCType string, MessageType string, Node int) bool {
	mu.Lock()
	defer mu.Unlock()
	if Term > atomic.LoadInt32(&rf.currentTerm) && atomic.LoadInt32(&rf.state) != -1 {
		atomic.StoreInt32(&rf.currentTerm, Term)
		if atomic.LoadInt32(&rf.state) == 1 {
			rf.Log("Leader abdicated!")
			rf.heartbeatChan <- 0 // stop Heartbeat
			rf.initialiseVolatile()
		}
		atomic.StoreInt32(&rf.state, -1)
		rf.Log("Converted to follower by RPC (" + RPCType + "|" + MessageType + "|N" + strconv.Itoa(Node) + "|T" + strconv.Itoa(int(Term)) + ")")
		return true
	}
	return false
}

func (rf *Raft) Election(source string, term int32) {

	if term != atomic.LoadInt32(&rf.currentTerm) || atomic.LoadInt32(&rf.state) != -1 {
		rf.Log("Election ceased: Leader already exists/Duplicate election")
		return
	}

	if !atomic.CompareAndSwapInt32(&rf.currentTerm, term, term+1) {
		// only competing process is *accepting a new leader*
		return
	}

	voteCount := int32(0)
	rf.Log("Election triggered by " + source)
	atomic.StoreInt32(&rf.state, 0)
	atomic.StoreInt32(&rf.votedFor, int32(rf.me))
	rf.Log("Voted myself!")
	voteCount = int32(1)
	threshold := int32(len(rf.peers) / 2)

	for i := 0; i < len(rf.peers); i++ {
		i := i
		go func() {
			if i != rf.me {
				args := &RequestVoteArgs{
					Term:         atomic.LoadInt32(&rf.currentTerm),
					CandidateId:  int32(rf.me),
					LastLogIndex: int(atomic.LoadInt32(&rf.lastApplied)),
					LastLogTerm:  0,
				}
				reply := &RequestVoteReply{}
				rf.Log("Try sending RequestVote RPC: N" + strconv.Itoa(i))

				if rf.sendRequestVote(i, args, reply) { // send RPC
					rf.Log("RequestVote RPC sent: N" + strconv.Itoa(i))
					if rf.checkAbdicate(reply.Term, "RV", "REPLY", i) {
						return
					}

					if reply.VoteGranted {
						atomic.AddInt32(&voteCount, 1)
						rf.Log("Vote received: N" + strconv.Itoa(i))
						if atomic.LoadInt32(&voteCount) > threshold && atomic.LoadInt32(&rf.dead) == 0 {
							// we won!
							atomic.StoreInt32(&rf.state, 1)
							rf.Log("Won Election!")
							rf.Heartbeat()
							rf.heartbeatChan <- 1
							rf.initialiseVolatile()
							return
						}
					} else {
						rf.Log("De-vote received from N" + strconv.Itoa(i))
					}
				} else {
					rf.Log("Failed to send RequestVote RPC: N" + strconv.Itoa(i))
				}
			}

			if atomic.LoadInt32(&rf.state) != 0 || rf.killed() {
				return
			}
		}()
	}

	time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
	if atomic.LoadInt32(&voteCount) <= threshold && !rf.killed() {
		if atomic.LoadInt32(&rf.state) != 0 {
			return
		}
		rf.Log("No one won election!")
		atomic.StoreInt32(&rf.state, -1)
		rf.Election("failed Election", atomic.LoadInt32(&rf.currentTerm))
	}
}

func (rf *Raft) Heartbeat() {
	args := &AppendEntriesArgs{
		Term:         atomic.LoadInt32(&rf.currentTerm),
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		Entries:      nil,
		LeaderCommit: 0,
	}
	reply := &AppendEntriesReply{
		Term: -1, // -1 => network error
	}

	for i := 0; i < len(rf.peers); i++ {
		i := i
		if atomic.LoadInt32(&rf.state) != 1 {
			break
		}
		go func() {
			if atomic.LoadInt32(&rf.state) != 1 {
				return
			}

			if i != rf.me {
				if rf.SendAppendEntries(i, args, reply) { // send RPC
					if reply.Success {
						rf.Log("Heartbeat sent to N" + strconv.Itoa(i))
					} else {
						rf.Log("Heartbeat rejected by N" + strconv.Itoa(i))
					}
					if rf.checkAbdicate(reply.Term, "HB", "REPLY", i) {
						return
					}
				} else {
					if atomic.LoadInt32(&reply.Term) == -1 { // network issue
						rf.Log("Heartbeat failed to send: N" + strconv.Itoa(i))
					}
				}
			}
		}()
	}
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
func Make(peers []*ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logs = make([]LogEntry, 1000)
	rf.initialiseVolatile()

	rf.electionChan = make(chan int, 1)
	rf.heartbeatChan = make(chan int, 1)

	go func() {
		rand.Seed(time.Now().UnixNano())
		s := time.Duration(rand.Intn(650-550) + 550) // 450ms - 549ms
		time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)
		rf.Election("fresh startup", 0)

		for {
			select {
			case o := <-rf.electionChan:
				if o < 0 {
					rf.Log("Election goroutine received stop signal " + strconv.Itoa(o))
					return
				}
			case <-time.After(s * time.Millisecond):
				if atomic.LoadInt32(&rf.state) < 0 {
					rf.Election("timeout", atomic.LoadInt32(&rf.currentTerm))
				}
			}
		}
	}()

	go func() {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		for {
			i := <-rf.heartbeatChan
			switch i {
			case -1:
				rf.Log("Heartbeat goroutine received stop signal " + strconv.Itoa(i))
				return
			case 1:
				rf.Log("Heartbeat started!")
				for {
					exit := false
					select {
					case <-time.After(110 * time.Millisecond):
						rf.Heartbeat()
					case s := <-rf.heartbeatChan:
						if s == -1 {
							rf.Log("Heartbeat goroutine received stop signal " + strconv.Itoa(s))
							return
						}
						if s == 0 {
							rf.Log("Heartbeat stopped!")
							exit = true
							break
						}
					}
					if exit {
						break
					}
				}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
