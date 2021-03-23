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

const (
	STATE_LEADER    = 1
	STATE_CANDIDATE = 0
	STATE_FOLLOWER  = -1
	EMPTY           = -1
	SIGNAL_PROCEED  = 1
	SIGNAL_PAUSE    = 0
	SIGNAL_STOP     = -1
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	electionMu sync.RWMutex // Lock to protect shared access to this peer's state
	logMu      sync.RWMutex // Lock to log entries
	peers      []*ClientEnd // RPC end points of all peers
	persister  *Persister   // Object to hold this peer's persisted state
	me         int          // this peer's index into peers[]
	dead       int32        // set by Kill()

	// Persistent
	currentTerm int32
	votedFor    int32
	logs        []*LogEntry
	state       int32 // -1 follower; 0 candidate; 1 leader

	// Volatile
	commitIndex int32
	lastApplied int32
	snapIndex   int32
	voteTerm    int32

	nextIndex  []int32
	matchIndex []int32

	// Channels
	electionChan  chan int
	heartbeatChan chan int
	applyChan     chan ApplyMsg

	// Counters
	logFlushCounter *Counter

	// TickExecutor
	tickExecutor *TickExecutor
}

// Counter:
// Counts appearances of int32 keys in inputChan,
// Upon threshold will invoke predefined callback function (Signature: int32 -> void).
type Counter struct {
	countMap  map[int32]int32
	trashMap  map[int32]bool
	upto      int32
	threshold int32
	callback  func(int32)
	inputChan chan int32
}

func MakeCounter(th int32, cb func(int32)) *Counter {
	c := &Counter{
		countMap: make(map[int32]int32),
		//trashMap:  make(map[int32]bool),
		threshold: th,
		callback:  cb,
		inputChan: make(chan int32, 20),
		upto:      EMPTY,
	}
	go func() {
		for {
			i := <-c.inputChan
			if i == SIGNAL_STOP {
				return
			}
			//if _, ok := c.trashMap[i]; ok {
			//	continue
			//}
			if _, ok := c.countMap[i]; !ok && i > c.upto {
				c.countMap[i] = 0
				c.upto = i
			}
			c.countMap[i]++
			for k := range c.countMap {
				if k <= i {
					c.countMap[k]++
				}
				if c.countMap[k] >= c.threshold {
					_, _ = VPrintf("{VERBOSE} [Counter] Callback invoked: %d", k)
					go c.callback(k) // invoke callback
					for k2 := range c.countMap {
						if k2 <= k {
							//c.trashMap[k2] = true
							delete(c.countMap, k2)
							_, _ = VPrintf("{VERBOSE} [Counter] Garbage key collected: %d", k2)
						}
					}
				}
			}
		}
	}()
	return c
}

func (c *Counter) Remove(i int32) {
	delete(c.countMap, i)
}

// TickExecutor:
// Execute bunch of predefined jobs after every tick-period
// Use ControlChan to shutdown goroutine gracefully
type TickExecutor struct {
	mu          sync.RWMutex
	TickPeriod  time.Duration
	JobList     []func()
	ControlChan chan int32
}

func newTickExecutor(tickPeriod time.Duration) *TickExecutor {
	e := TickExecutor{
		TickPeriod:  tickPeriod,
		ControlChan: make(chan int32, 1),
	}
	go func() {
		for {
			select {
			case <-time.After(tickPeriod):
				for _, f := range e.JobList {
					if f != nil {
						go f()
					}
				}
				_, _ = VPrintf("{VERBOSE} [TickExecutor] Tick finished, %d job(s) executed", len(e.JobList))
			case msg := <-e.ControlChan:
				if msg == SIGNAL_STOP {
					return
				}
			}
		}
	}()
	return &e
}

func (e *TickExecutor) Register(f func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.JobList = append(e.JobList, f)
	_, _ = VPrintf("{VERBOSE} [TickExecutor] Function successfully registered")
}

type LogEntry struct {
	Term    int32
	Command interface{}
}

func (rf *Raft) Log(s string) {
	log.Println(rf.String() + ": " + s)
}

func (rf *Raft) String() string {
	return "Node " + strconv.Itoa(rf.me) +
		" (S" + strconv.Itoa(int(atomic.LoadInt32(&rf.state))) +
		"|T" + strconv.Itoa(int(atomic.LoadInt32(&rf.currentTerm))) +
		"|V" + strconv.Itoa(int(atomic.LoadInt32(&rf.votedFor))) +
		"|CI" + strconv.Itoa(int(atomic.LoadInt32(&rf.commitIndex))) +
		"|LA" + strconv.Itoa(int(atomic.LoadInt32(&rf.lastApplied))) +
		"|SN" + strconv.Itoa(int(atomic.LoadInt32(&rf.snapIndex))) +
		"|DEAD" + strconv.Itoa(int(atomic.LoadInt32(&rf.dead))) +
		"|LOG" + strconv.Itoa(len(rf.logs)-1) + ")"
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(atomic.LoadInt32(&rf.currentTerm)), atomic.LoadInt32(&rf.state) == STATE_LEADER
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

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex int32
	PrevLogTerm  int32
	Entries      []*LogEntry
	LeaderCommit int32
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Term Check
	if args.Term < rf.currentTerm {
		rf.Log("AppendEntries RPC outdated! - N" + strconv.Itoa(args.LeaderId) + "|T" + strconv.Itoa(int(args.Term)))
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Entries == nil {
		rf.Log("Heartbeat received from N" + strconv.Itoa(args.LeaderId))
	}

	// Election Stuff
	atomic.StoreInt32(&rf.currentTerm, args.Term)
	atomic.StoreInt32(&rf.votedFor, int32(args.LeaderId))
	if atomic.LoadInt32(&rf.state) == STATE_LEADER {
		rf.Abdicate(args.Term)
	}
	if atomic.LoadInt32(&rf.state) != STATE_FOLLOWER {
		atomic.StoreInt32(&rf.state, STATE_FOLLOWER)
		rf.Log("Accepted leader N" + strconv.Itoa(args.LeaderId))
	}
	rf.electionChan <- SIGNAL_PROCEED
	reply.Success = true
	reply.Term = rf.currentTerm

	// Log Stuff
	if args.Entries != nil {
		// Entries
		rf.logMu.RLock() // RLock
		if args.PrevLogIndex != -1 && (int32(len(rf.logs)-1)+rf.snapIndex < args.PrevLogIndex ||
			rf.logs[args.PrevLogIndex-atomic.LoadInt32(&rf.snapIndex)].Term != args.PrevLogTerm) {
			reply.Success = false
			reply.Term = atomic.LoadInt32(&rf.currentTerm)
			rf.Log("LogEntry Mismatch! Rejecting AppendEntries RPC")
			return
		}
		rf.logMu.RUnlock()
		rf.logMu.Lock() // WLock
		defer rf.logMu.Unlock()
		rf.Log("LogEntries Received from N" + strconv.Itoa(args.LeaderId))

		if args.PrevLogIndex != -1 && rf.commitIndex > args.PrevLogIndex {
			rf.logs = rf.logs[:args.PrevLogIndex-atomic.LoadInt32(&rf.snapIndex)]
			atomic.StoreInt32(&rf.commitIndex, args.PrevLogIndex)
			rf.Log("LogEntry Conflict! Trimming Log to " + strconv.Itoa(int(args.PrevLogIndex)))
		}
		rf.logs = append(rf.logs, args.Entries...)
		rf.Log("AppendEntries RPC Accepted! Log stored upto (I" + strconv.Itoa(len(rf.logs)-1) + ")")
	}

	if args.LeaderCommit > rf.commitIndex {
		atomic.StoreInt32(&rf.commitIndex, args.LeaderCommit)
		rf.Log("Follower flushing apply (I" + strconv.Itoa(int(rf.commitIndex)) + ")")
		rf.FlushApply()
	}

}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int32
	LastLogIndex int32
	LastLogTerm  int32
}

func (a *RequestVoteArgs) String() string {
	return "[RequestVoteArgs|Term=" + strconv.Itoa(int(a.Term)) + "|CandidateId=" + strconv.Itoa(int(a.CandidateId)) +
		"|LastLogIndex=" + strconv.Itoa(int(a.LastLogIndex)) + "|LastLogTerm=" + strconv.Itoa(int(a.LastLogTerm)) + "]"
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = atomic.LoadInt32(&rf.currentTerm)
	if args.Term < atomic.LoadInt32(&rf.currentTerm) {
		rf.Log("De-Voted N" + strconv.Itoa(int(args.CandidateId)) + " (Term_error)")
		reply.VoteGranted = false
		return
	}

	if atomic.LoadInt32(&rf.votedFor) == args.CandidateId && args.Term == atomic.LoadInt32(&rf.voteTerm) {
		// Duplicate RequestVote
		rf.Log("WARN: Duplicate RequestVote from N" + strconv.Itoa(int(args.CandidateId)))
		reply.VoteGranted = false
		return
	}
	if atomic.LoadInt32(&rf.votedFor) == -1 || atomic.LoadInt32(&rf.voteTerm) < args.Term &&
		args.Term > atomic.LoadInt32(&rf.currentTerm) {
		atomic.StoreInt32(&rf.voteTerm, args.Term)
		rf.Log("Voted N" + strconv.Itoa(int(args.CandidateId)))
		atomic.StoreInt32(&rf.votedFor, args.CandidateId)
		reply.VoteGranted = true
		return
	} else {
		rf.Log("De-Voted N" + strconv.Itoa(int(args.CandidateId)) + " (Already_Voted)")
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
	if atomic.LoadInt32(&rf.state) == STATE_LEADER {
		entry := LogEntry{
			Term:    atomic.LoadInt32(&rf.currentTerm),
			Command: command,
		}
		rf.logs = append(rf.logs, &entry)
		rf.Log("Command received from client, promised at I" + strconv.Itoa(len(rf.logs)-1))

		// Flush to followers
		rf.FlushLog()
	}
	return len(rf.logs), int(rf.currentTerm), rf.state == STATE_LEADER
}

func (rf *Raft) checkFlushApply() {
	if atomic.LoadInt32(&rf.lastApplied) < atomic.LoadInt32(&rf.commitIndex) {
		rf.FlushApply()
	}
}

func (rf *Raft) FlushApply() {
	if len(rf.logs) == 0 {
		return
	}
	rf.logMu.RLock()
	defer rf.logMu.RUnlock()

	for i := atomic.LoadInt32(&rf.lastApplied) + 1; i <= atomic.LoadInt32(&rf.commitIndex); i++ {
		if i < atomic.LoadInt32(&rf.lastApplied) {
			continue
		}
		iSnap := i + atomic.LoadInt32(&rf.snapIndex)
		rf.Log("Pushing to apply (I" + strconv.Itoa(int(iSnap)) + ")")
		rf.apply(rf.logs[i].Command, iSnap)
		atomic.StoreInt32(&rf.lastApplied, i)
	}
}

func (rf *Raft) apply(command interface{}, index int32) {
	if index < atomic.LoadInt32(&rf.lastApplied)+atomic.LoadInt32(&rf.snapIndex) {
		return
	}
	msg := ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: int(index + 1),
	}
	rf.applyChan <- msg
	rf.Log("Command successfully applied to state-machine (I" + strconv.Itoa(int(index)) + ")")
}

// MUST FIRST ACQUIRE logMU
func (rf *Raft) FlushLog() {
	if len(rf.logs) == 0 {
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		go rf.sendFlushLog(i)
	}
}

func (rf *Raft) sendFlushLog(target int) {
	if rf.nextIndex[target] == -1 {
		return
	}
	if atomic.LoadInt32(&rf.state) == STATE_LEADER && target != rf.me && int(rf.nextIndex[target]) <= len(rf.logs)-1 {
		rf.logMu.RLock()
		cutOff := len(rf.logs) - 1
		args := AppendEntriesArgs{
			Term:         atomic.LoadInt32(&rf.currentTerm),
			LeaderId:     int(atomic.LoadInt32(&rf.votedFor)),
			PrevLogIndex: atomic.LoadInt32(&rf.nextIndex[target]) - 1,
			PrevLogTerm:  rf.logs[atomic.LoadInt32(&rf.nextIndex[target])].Term,
			Entries:      rf.logs[atomic.LoadInt32(&rf.nextIndex[target]) : cutOff+1],
			LeaderCommit: rf.commitIndex,
		}
		rf.logMu.RUnlock()
		reply := AppendEntriesReply{}
		if rf.nextIndex[target] == int32(cutOff) {
			rf.Log("Try sending Log to N" + strconv.Itoa(target) + ": I" +
				strconv.Itoa(int(rf.nextIndex[target])))
		} else {
			rf.Log("Try sending Log to N" + strconv.Itoa(target) + ": I" +
				strconv.Itoa(int(rf.nextIndex[target])) + "-" + strconv.Itoa(cutOff))
		}
		atomic.StoreInt32(&rf.nextIndex[target], int32(cutOff+1))

		if rf.SendAppendEntries(target, &args, &reply) {
			if reply.Success {
				atomic.StoreInt32(&rf.nextIndex[target], int32(cutOff+1))
				atomic.StoreInt32(&rf.matchIndex[target], int32(cutOff))
				rf.Log("Log replicated on N" + strconv.Itoa(target) + " to I" + strconv.Itoa(cutOff))
				rf.logFlushCounter.inputChan <- int32(cutOff)
			} else {
				if rf.nextIndex[target] > 0 {
					s := rf.nextIndex[target]
					for s > 0 {
						s--
						cutOff = len(rf.logs) - 1
						rf.Log("Log disparity reported by N" + strconv.Itoa(target) + ", retrying at I" +
							strconv.Itoa(int(s)))
						args := AppendEntriesArgs{
							Term:         atomic.LoadInt32(&rf.currentTerm),
							LeaderId:     int(atomic.LoadInt32(&rf.votedFor)),
							PrevLogIndex: s - 1,
							PrevLogTerm:  rf.logs[s].Term,
							Entries:      rf.logs[s : cutOff+1],
							LeaderCommit: rf.commitIndex,
						}
						if rf.SendAppendEntries(target, &args, &reply) {
							if reply.Success {
								atomic.StoreInt32(&rf.nextIndex[target], int32(cutOff+1))
								atomic.StoreInt32(&rf.matchIndex[target], int32(cutOff))
								rf.Log("Log replicated on N" + strconv.Itoa(target) + " to I" + strconv.Itoa(cutOff))
								rf.logFlushCounter.inputChan <- int32(cutOff)
								break
							}
						} else {
							if !rf.killed() {
								rf.Log("Failed to send AppendEntries to " + strconv.Itoa(target) +
									": upto-I" + strconv.Itoa(int(s)))
								s++
							}
						}
					}
				} else {
					panic("FATAL ERROR - LOG OUT OF INDEX")
				}
			}
		} else {
			if !rf.killed() {
				rf.Log("Failed to send AppendEntries to " + strconv.Itoa(target) +
					": upto-I" + strconv.Itoa(int(rf.commitIndex)))
				rf.sendFlushLog(target)
			}
		}
	}
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
	rf.Log("Kill() Called, closing down gracefully...")
	rf.electionChan <- SIGNAL_STOP
	rf.heartbeatChan <- SIGNAL_STOP
	rf.logFlushCounter.inputChan <- SIGNAL_STOP
	rf.tickExecutor.ControlChan <- SIGNAL_STOP
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Abdicate(term int32) {
	rf.Log("Leader abdicated!")
	atomic.StoreInt32(&rf.currentTerm, term)
	rf.heartbeatChan <- SIGNAL_PAUSE // stop Heartbeat
	atomic.StoreInt32(&rf.state, STATE_FOLLOWER)
}

func (rf *Raft) Election(source string, term int32) {
	if rf.killed() || term != rf.currentTerm || atomic.LoadInt32(&rf.state) != STATE_FOLLOWER {
		rf.Log("Election ceased prematurely (no need to elect)")
		return
	}
	atomic.AddInt32(&rf.currentTerm, 1)
	rf.Log("Election triggered by " + source)
	atomic.StoreInt32(&rf.state, STATE_CANDIDATE)
	atomic.StoreInt32(&rf.votedFor, int32(rf.me))
	rf.Log("Voted myself!")
	voteCount := int32(1)
	threshold := int32(len(rf.peers) / 2)
	waitGroup := sync.WaitGroup{}

	for i := 0; i < len(rf.peers); i++ {
		target := i
		go rf.Campaign(target, &waitGroup, &voteCount, &threshold)
	}

	waitGroup.Wait()
	if atomic.LoadInt32(&rf.state) != STATE_CANDIDATE || rf.killed() {
		return
	}
	time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
	if voteCount <= threshold && atomic.LoadInt32(&rf.dead) == 0 {
		if atomic.LoadInt32(&rf.state) != STATE_CANDIDATE || rf.killed() {
			return
		}
		rf.Log("No one won election!")
		atomic.StoreInt32(&rf.state, STATE_FOLLOWER)
		rf.Election("Failed Election", rf.currentTerm)
	}
}

func (rf *Raft) Campaign(target int, wg *sync.WaitGroup, voteCount *int32, threshold *int32) bool {
	if rf.killed() || atomic.LoadInt32(&rf.state) != STATE_CANDIDATE {
		return true
	}
	if target != rf.me {
		wg.Add(1)
		defer wg.Done()
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  int32(rf.me),
			LastLogIndex: 0,
			LastLogTerm:  0,
		}
		reply := &RequestVoteReply{}

		rf.Log("Try sending RequestVote RPC: N" + strconv.Itoa(target))
		if rf.sendRequestVote(target, args, reply) {
			rf.Log("RequestVote RPC sent: N" + strconv.Itoa(target))
			if reply.VoteGranted {
				atomic.AddInt32(voteCount, 1)
				rf.Log("Vote received: N" + strconv.Itoa(target))
				rf.electionMu.Lock()
				if atomic.LoadInt32(voteCount) > atomic.LoadInt32(threshold) && atomic.LoadInt32(&rf.dead) == 0 {
					// we won!
					rf.Log("Election Won!")
					rf.Coronation()
					return true
				}
				rf.electionMu.Unlock()
			} else {
				rf.Log("De-vote received: N" + strconv.Itoa(target))
				return false
			}
		} else {
			if atomic.LoadInt32(&rf.state) == STATE_CANDIDATE {
				if !rf.killed() {
					rf.Log("RequestVote RPC failed to send: N" + strconv.Itoa(target))
					go rf.Campaign(target, wg, voteCount, threshold)
				}
				return false
			} else {
				if !rf.killed() {
					rf.Log("Outdated RequestVote RPC failed to send: N" + strconv.Itoa(target))
				}

				return true
			}
		}
	}
	return true
}

func (rf *Raft) Coronation() {
	atomic.StoreInt32(&rf.state, STATE_LEADER)
	rf.Log("Coronation in action: starting Heartbeat")
	rf.Heartbeat()
	rf.heartbeatChan <- SIGNAL_PROCEED

	// log
	rf.logMu.Lock()
	for i := range rf.nextIndex {
		atomic.StoreInt32(&rf.nextIndex[i], int32(len(rf.logs))+atomic.LoadInt32(&rf.snapIndex))
	}
	for i := range rf.matchIndex {
		atomic.StoreInt32(&rf.matchIndex[i], 0)
	}
	rf.logMu.Unlock()
	rf.FlushLog()
}

func (rf *Raft) Heartbeat() {
	args := &AppendEntriesArgs{
		Term:         atomic.LoadInt32(&rf.currentTerm),
		LeaderId:     rf.me,
		PrevLogIndex: EMPTY,
		Entries:      nil,
		LeaderCommit: atomic.LoadInt32(&rf.commitIndex),
	}
	reply := &AppendEntriesReply{}

	for i := 0; i < len(rf.peers); i++ {
		i := i
		if atomic.LoadInt32(&rf.state) != STATE_LEADER {
			return
		}
		go func() {
			if atomic.LoadInt32(&rf.state) == STATE_LEADER && i != rf.me {
				if rf.SendAppendEntries(i, args, reply) {
					if reply.Success {
						rf.Log("Heartbeat sent to N" + strconv.Itoa(i))
					} else {
						rf.Log("Heartbeat rejected by N" + strconv.Itoa(i))
						if reply.Term > atomic.LoadInt32(&rf.currentTerm) {
							rf.Abdicate(reply.Term)
						} else {
							rf.Log("Already abdicated? N" + strconv.Itoa(i) +
								"|T" + strconv.Itoa(int(reply.Term)))
						}
					}
				} else {
					if atomic.LoadInt32(&rf.state) == STATE_LEADER {
						if !rf.killed() {
							rf.Log("Heartbeat failed to send: N" + strconv.Itoa(i) + "|T ")
						}

					} else {
						if !rf.killed() {
							rf.Log("Outdated Heartbeat failed to send: N" + strconv.Itoa(i) + "|T ")
						}
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

	rf.state = STATE_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = EMPTY
	rf.commitIndex = EMPTY
	rf.lastApplied = EMPTY
	rf.snapIndex = 0

	rf.nextIndex = make([]int32, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 0
	}
	rf.matchIndex = make([]int32, len(peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	rf.electionChan = make(chan int, 1)
	rf.heartbeatChan = make(chan int, 1)
	rf.applyChan = applyCh
	rf.logFlushCounter = MakeCounter(int32(len(rf.peers)/2-1),
		func(i int32) {
			atomic.StoreInt32(&rf.commitIndex, i)
			rf.FlushApply()
		})

	rf.tickExecutor = newTickExecutor(1 * time.Second)
	rf.tickExecutor.Register(rf.checkFlushApply)

	go func() {
		rand.Seed(time.Now().UnixNano())
		electionTimeout := time.Duration(rand.Intn(650-550) + 550) // 550ms - 649ms
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
		rf.Election("Fresh Start-up", 0)

		for {
			select {
			case o := <-rf.electionChan:
				if o < 0 {
					rf.Log("Election goroutine received stop signal " + strconv.Itoa(o))
					return
				}
			case <-time.After(electionTimeout * time.Millisecond):
				if atomic.LoadInt32(&rf.state) == STATE_FOLLOWER {
					rf.Election("timeout", rf.currentTerm)
				}
			}
		}
	}()

	go func() {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		for {
			i := <-rf.heartbeatChan
			switch i {
			case SIGNAL_STOP:
				rf.Log("Heartbeat goroutine received stop signal " + strconv.Itoa(i))
				return
			case SIGNAL_PROCEED:
				exit := false
				for {
					select {
					case <-time.After(110 * time.Millisecond):
						rf.Heartbeat()
					case s := <-rf.heartbeatChan:
						if s == SIGNAL_STOP {
							rf.Log("Heartbeat goroutine received stop signal " + strconv.Itoa(s))
							return
						}
						if s == SIGNAL_PAUSE {
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
