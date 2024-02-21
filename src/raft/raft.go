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
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const VoteNone int = -1

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persisted state on all server
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile state on all server
	// commitIndex int
	// lastApplied int

	// Volatile state on leader
	// nextIndex  []int
	// matchIndex []int

	// Other necessary fields
	state            State
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration
	lastElection     time.Time
	lastHeartbeat    time.Time
}

// Log entry
type LogEntry struct {
	// term    int
	// content struct{}
}

type State int

const (
	StateLeader    State = 1
	StateFollower  State = 2
	StateCandidate State = 3
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	term, isleader = rf.currentTerm, rf.state == StateLeader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[server-%v][term-%v][RequestVote] received from candidate-%v", rf.me, rf.currentTerm, args.CandidateId)
	term, _ := rf.GetState()
	if term > args.Term {
		reply.Term = term
		reply.VoteGranted = false
		DPrintf("[server-%v][term-%v][RequestVote] won't vote for candidate-%v since its term(%v) too old", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	grantInSameTerm := term == args.Term && (rf.votedFor == VoteNone || rf.votedFor == args.CandidateId)
	if term < args.Term || grantInSameTerm {
		rf.becomeFollower(args.Term, args.CandidateId)
		reply.Term = args.Term
		reply.VoteGranted = true
		DPrintf("[server-%v][term-%v] become follower. And it votes for: server-%v", rf.me, rf.currentTerm, rf.votedFor)
		return
	}

	DPrintf("[server-%v][term-%v][RequestVote] won't vote for candidate-%v since already vote for another candidate(%v)", rf.me, rf.currentTerm, args.CandidateId, rf.votedFor)
	reply.Term = args.Term
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[server-%v][term-%v][AppendEntries] received from server-%v", rf.me, rf.currentTerm, args.LeaderId)

	term, _ := rf.GetState()
	if term > args.Term {
		reply.Term = term
		reply.Success = false
		DPrintf("[server-%v][term-%v][AppendEntries] won't accept from server-%v since its term(%v) too old", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}

	rf.becomeFollower(args.Term, args.LeaderId)
	reply.Term = args.Term
	reply.Success = true
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.routine()

		// pause 100 milliseconds.
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) routine() {
	// DPrintf("current time elapsed: %v", rf.timeElapsed.Load())
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	if state == StateLeader && rf.pastHeartbeatTimeout() {
		rf.sendHeartBeat()
	} else if state != StateLeader && rf.pastElectionTimeout() {
		rf.kickstartVote()
	}
}

func (rf *Raft) pastElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastElection) > rf.electionTimeout
}

func (rf *Raft) pastHeartbeatTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[server-%v][term-%v] send heartbeat to %v peers...", rf.me, rf.currentTerm, len(rf.peers))
	term, _ := rf.GetState()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		sid := i
		arg := &AppendEntryArgs{
			Term:     term,
			LeaderId: rf.me,
			// more to go
		}
		reply := &AppendEntryReply{}
		go func() {
			rf.sendAppendEntries(sid, arg, reply)
			if !reply.Success {
				rf.becomeFollower(reply.Term, sid)
			}
		}()
	}
}

func (rf *Raft) kickstartVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[server-%v][term-%v] kick start a vote now... nPeer: %v", rf.me, rf.currentTerm, len(rf.peers))
	rf.becomeCandidate()
	term, _ := rf.GetState()

	ballot := atomic.Int32{}
	ballot.Store(1)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		sid := i
		arg := &RequestVoteArgs{
			Term:        term,
			CandidateId: rf.me,
			// more to go
		}
		reply := &RequestVoteReply{}
		go func() {
			rf.sendRequestVote(sid, arg, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term < rf.currentTerm {
				DPrintf("[server-%v][term-%v] won't accept ballot from server-%v in older term: %v", rf.me, rf.currentTerm, sid, reply.Term)
				return
			}

			if reply.Term > rf.currentTerm {
				DPrintf("[server-%v][term-%v] it won't become leader since higher term exist. Quit vote...", rf.me, rf.currentTerm)
				rf.becomeFollower(reply.Term, VoteNone)
				return
			}

			if reply.VoteGranted {
				ballot.Add(1)
			}

			nPeer := len(rf.peers)
			if int(ballot.Load()) >= (nPeer+1)/2 {
				rf.becomeLeader()
				return
			}
		}()
	}
}

// Should be called within lock block
func (rf *Raft) reset() {
	rf.lastElection = time.Now()
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) becomeFollower(newTerm int, voteFor int) {
	rf.reset()
	rf.currentTerm = newTerm
	rf.votedFor = voteFor
	rf.state = StateFollower
}

func (rf *Raft) becomeLeader() {
	rf.reset()
	// check whether it is still a candidate
	if rf.state != StateCandidate {
		DPrintf("[server-%v][term-%v] won't become leader since no longer a candidate. Instead, it is a %+v", rf.me, rf.currentTerm, rf.state)
		return
	}
	rf.state = StateLeader
	rf.votedFor = rf.me
	DPrintf("[server-%v][term-%v] become leader!", rf.me, rf.currentTerm)
	go rf.sendHeartBeat()
}

func (rf *Raft) becomeCandidate() {
	rf.reset()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.state = StateCandidate
	DPrintf("[server-%v][term-%v] become candidate", rf.me, rf.currentTerm)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = VoteNone
	rf.logs = []LogEntry{}
	rf.state = StateFollower
	rf.electionTimeout = time.Duration(me*100+400) * time.Millisecond
	rf.heartbeatTimeout = 150 * time.Millisecond
	rf.lastElection = time.Now()
	rf.lastHeartbeat = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
