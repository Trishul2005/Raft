package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.


import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)


// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// State
	currentTerm int
	votedFor	int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex   []int
	matchIndex  []int

	role 	    RaftRole
	timestamp   time.Time

	applyCh     chan raftapi.ApplyMsg
	heartbeatCh chan struct{}

	// snapshot
	snapshotIndex int
	snapshotTerm  int
	snapshot      []byte

}

type LogEntry struct {
	Command interface{}
	Term    int
}

type RaftRole string
const (
	Follower  RaftRole = "Follower"
	Candidate RaftRole = "Candidate"
	Leader    RaftRole = "Leader"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data			  []byte
}

type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).

	term = rf.currentTerm
	isleader = (rf.role == Leader)

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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)

}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	var snapshotIndex int
	var snapshotTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {

		// error...

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
	}

	// restore snapshot state
	rf.snapshot = rf.persister.ReadSnapshot()

}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// ignore if the snapshot index is less than or equal to the current snapshot index
	if index <= rf.snapshotIndex {
		return
	}

	// update snapshotIndex and snapshotTerm
	arrayIdx := index - rf.snapshotIndex
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log[arrayIdx].Term

	// trim the log to remove entries up to and including the snapshot index
	rf.log = rf.log[arrayIdx:]

	// save the snapshot and the updated Raft state to persistent storage
	rf.snapshot = snapshot
	rf.persist()

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// all servers rule
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	
	rf.mu.Lock()

	// all servers rule
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	// reject if term is less than currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	// ignore the snapshot if lastIncludedIndex is less than or equal to the current snapshot index
	if args.LastIncludedIndex <= rf.snapshotIndex {
		rf.mu.Unlock()
		return
	}

	 // replace log, preserving entries beyond the snapshot if terms match
	arrayIdx := args.LastIncludedIndex - rf.snapshotIndex
	if arrayIdx < len(rf.log) && rf.log[arrayIdx].Term == args.LastIncludedTerm {
		rf.log = append([]LogEntry{{Term: args.LastIncludedTerm}}, rf.log[arrayIdx+1:]...)
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	// persist the snapshot and the updated Raft state to persistent storage
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Data

	rf.commitIndex = max(rf.commitIndex, rf.snapshotIndex)
	rf.lastApplied = max(rf.lastApplied, rf.snapshotIndex)

	rf.persist()

	// send snapshot to the service via applyCh
	applyMsg := raftapi.ApplyMsg{
		CommandValid: false,
		SnapshotValid: true,
		Snapshot: rf.snapshot,
		SnapshotTerm: rf.snapshotTerm,
		SnapshotIndex: rf.snapshotIndex,
	}

	rf.mu.Unlock()

	rf.applyCh <- applyMsg
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// all servers rule
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	// reject if term is less than currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// reset election timer
	rf.timestamp = time.Now()
	reply.Term = rf.currentTerm
	reply.Success = true

	// reject if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= rf.lastLogIndex()+1 || (args.PrevLogIndex >= rf.snapshotIndex && rf.log[rf.toArrayIdx(args.PrevLogIndex)].Term != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false

		// provide information for the leader to optimize its next AppendEntries RPC
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.lastLogIndex() + 1

		if args.PrevLogIndex < rf.lastLogIndex() + 1 {
			reply.XTerm = rf.log[rf.toArrayIdx(args.PrevLogIndex)].Term

			for i := args.PrevLogIndex; i >= rf.snapshotIndex; i-- {
				if rf.log[rf.toArrayIdx(i)].Term != reply.XTerm {
					reply.XIndex = i + 1
					break
				}
			}
		}

		return
	}

	// existing entry conflicts with a new one (same index
    // but different terms), delete the existing entry and all that
    // follow it
	if args.PrevLogIndex+1 < rf.lastLogIndex() + 1 && len(args.Entries) > 0 {

		for i, entry := range args.Entries {
			if args.PrevLogIndex+1+i < rf.lastLogIndex() + 1 && args.PrevLogIndex+1+i >= rf.snapshotIndex {
				if rf.log[rf.toArrayIdx(args.PrevLogIndex+1+i)].Term != entry.Term {
					rf.log = rf.log[:rf.toArrayIdx(args.PrevLogIndex+1+i)]
					rf.persist()
					break
				}
			}
		}

	}

	// append any new entries not already in the log
	for i, entry := range args.Entries {
		if args.PrevLogIndex+1+i >= rf.snapshotIndex+len(rf.log) {
			rf.log = append(rf.log, entry)
			rf.persist()
		}
	}

	// update commitIndex if leaderCommit > commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}

	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	// 3A
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// all servers rule
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	// case 1 in fig 2
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// case 2 in fig 2
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// check if candidate's log is at least as up-to-date as receiver's log
		lastLogIndex := rf.lastLogIndex()
		lastLogTerm := rf.lastLogTerm()

		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.timestamp = time.Now() // reset election timer
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.persist()
			return
		}
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm


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


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	if rf.role != Leader {
		isLeader = false
		return index, term, isLeader
	}

	term = rf.currentTerm
	index = rf.lastLogIndex() + 1
	rf.log = append(rf.log, LogEntry{Command: command, Term: term})
	select {
	case rf.heartbeatCh <- struct{}{}:
	default:
	}
	rf.persist()

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()

		if rf.role != Leader && time.Since(rf.timestamp) > time.Duration(300+rand.Intn(300))*time.Millisecond {
			rf.role = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.timestamp = time.Now()

			rf.persist()

			votes := 1 // count self vote

			// send RequestVote RPCs to all other servers
			for i := range rf.peers {
				if i != rf.me {
					args := &RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: rf.lastLogIndex(),
						LastLogTerm:  0,
					}
					if len(rf.log) > 0 {
						args.LastLogTerm = rf.log[len(rf.log)-1].Term
					}
					reply := &RequestVoteReply{}
					
					go func(server int, args *RequestVoteArgs, reply *RequestVoteReply, votes *int) {
						rf.electLeader(server, args, reply, votes)
					}(i, args, reply, &votes)
				}
			}
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) electLeader(server int, args *RequestVoteArgs, reply *RequestVoteReply, votes *int) {

	ok := rf.sendRequestVote(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// all servers rule
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.role = Follower
			rf.persist()
			return
		}

		if reply.VoteGranted {

			// if the term is not the same, ignore the vote
			if reply.Term != rf.currentTerm {
				return
			}

			*votes++

			// leader election wins
			if *votes > len(rf.peers)/2 && rf.role == Candidate {
				rf.role = Leader
				rf.timestamp = time.Now()
				// initialize nextIndex and matchIndex for each follower
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for j := range rf.peers {
					if j != rf.me {
						rf.nextIndex[j] = rf.lastLogIndex() + 1
						rf.matchIndex[j] = 0
					}
				}

				go rf.heartbeat() // start sending heartbeats to followers

			}

		}
	}
}

func (rf *Raft) heartbeat() {
	for {

		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			break
		}

		for i := range rf.peers {
			if i != rf.me {

				args := &AppendEntriesArgs{}

				if rf.nextIndex[i] <= rf.snapshotIndex {

					// send InstallSnapshot RPC
					argsSnapshot := &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.snapshotIndex,
						LastIncludedTerm:  rf.snapshotTerm,
						Data:              rf.snapshot,
					}

					replySnapshot := &InstallSnapshotReply{}
					go func(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
						rf.sendInstallSnapshot(server, args, reply)
					}(i, argsSnapshot, replySnapshot)

					continue

				} else if rf.lastLogIndex() + 1 > rf.nextIndex[i] {
					// send AppendEntries RPC with log entries
					args = &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  0,
						Entries:      rf.log[rf.toArrayIdx(rf.nextIndex[i]):],
						LeaderCommit: rf.commitIndex,
					}
					args.PrevLogTerm = rf.log[rf.toArrayIdx(args.PrevLogIndex)].Term

				} else {
					// send heartbeat (AppendEntries RPC with no log entries)
					args = &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.lastLogIndex(),
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}
					args.PrevLogTerm = rf.log[rf.toArrayIdx(args.PrevLogIndex)].Term
				}

				reply := &AppendEntriesReply{}

				go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
					rf.sendAppendEntries(server, args, reply)
				}(i, args, reply)

			}
		}

		rf.mu.Unlock()
		select {
		case <-rf.heartbeatCh:
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// all servers rule
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.role = Follower
			rf.persist()
			return
		}

		// if the term is not the same, ignore the reply
		if reply.Term != rf.currentTerm {
			return
		}

		if reply.Success {
			// update nextIndex and matchIndex for the follower
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1

			// update commitIndex if a majority of followers have replicated the entry
			for N := rf.commitIndex + 1; N < rf.lastLogIndex() + 1; N++ {
				count := 1 // count self
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= N && rf.log[rf.toArrayIdx(N)].Term == rf.currentTerm {
						count++
					}
				}
				if count > len(rf.peers)/2 {
					rf.commitIndex = N
				}
			}

		} else {

			if reply.XTerm != -1 {
				// find the last index of XTerm in the leader's log
				lastIndexOfXTerm := -1
				for i := rf.lastLogIndex(); i >= rf.snapshotIndex; i-- {
					if rf.log[rf.toArrayIdx(i)].Term == reply.XTerm {
						lastIndexOfXTerm = i
						break
					}
				}

				// Case 1: leader doesn't have XTerm:
				//     nextIndex = XIndex
				// XTerm:  term in the conflicting entry (if any)
				if lastIndexOfXTerm == -1 {
					rf.nextIndex[server] = reply.XIndex
				} else {
					// Case 2: leader has XTerm:
					//     nextIndex = (index of leader's last entry for XTerm) + 1
					rf.nextIndex[server] = lastIndexOfXTerm + 1
				}

			} else {
				// Case 3: follower's log is too short:
				//     nextIndex = XLen
				// XLen:   log length
				rf.nextIndex[server] = reply.XLen
			}


			// ensure the nextIndex to be at least 1
			if rf.nextIndex[server] < 1 {
				rf.nextIndex[server] = 1
			}

		}

	}
}

// applier is a goroutine that applies committed log entries to the state machine
func (rf *Raft) applier() {

	for {

		// important to lock and unlock here to avoid deadlock, 
		// since sending on applyCh can block
		rf.mu.Lock()
		messages := []raftapi.ApplyMsg{}

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.toArrayIdx(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
			}
			messages = append(messages, applyMsg)
		}
		rf.mu.Unlock()

		// send the ApplyMsg messages to the applyCh channel AFTER unlock
		for _, msg := range messages {
			rf.applyCh <- msg
		}

		time.Sleep(10 * time.Millisecond)

	}

}

// === HELPER FUNCTIONS FOR SNAPSHOTS ===

// toArrayIdx converts a global log index to an index in the log slice, 
// taking into account the snapshot index
func (rf *Raft) toArrayIdx(globalIdx int) int {
	return globalIdx - rf.snapshotIndex
}

// lastLogIndex returns the index of the last log entry, taking into account the snapshot index
func (rf *Raft) lastLogIndex() int {
	return rf.snapshotIndex + len(rf.log) - 1
}

// lastLogTerm returns the term of the last log entry, taking into account the snapshot index
func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.snapshotTerm
	}
	return rf.log[len(rf.log)-1].Term
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// 3A initialization: state
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1) // log starts with index 1, so we add a dummy entry at index 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.role = Follower
	rf.timestamp = time.Now()
	// note: no need to initialize nextIndex and matchIndex here, 
	// since they are only used by leaders

	// 3B initialization: applyCh
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 3D initialization: snapshot recovery
	rf.commitIndex = rf.snapshotIndex
	rf.lastApplied = rf.snapshotIndex

	rf.heartbeatCh = make(chan struct{}, 1)

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine to apply committed log entries
	go rf.applier()


	return rf
}
