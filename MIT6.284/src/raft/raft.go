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

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Log struct {
	Term    int
	Index   int
	Command interface{}
}
type persist struct {
	Term     int
	Log      []Log
	VotedFor int
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm      int
	votedFor         int
	log              []Log
	commitIndex      int
	lastApplied      int
	nextIndex        []int
	matchIndex       []int
	HeartsBeatsIndex int
	isLeader         bool
	LeaderId         int
	MyState          int
	Votes            int
	Applies          []int
	Applied          []bool
	applyCh          chan ApplyMsg
	Consistent       bool
	LastGetIndex     int
	Forbidden        bool
}

const (
	Follower   = 0
	Candidator = 1
	Leader     = 2
)
const (
	Succeed      = 0
	Failed       = 1
	Disconnected = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.MyState == Leader {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
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
	content := persist{Term: rf.currentTerm, VotedFor: rf.votedFor, Log: rf.log}
	e.Encode(content)
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
	var content persist
	err := d.Decode(&content)
	if err != nil {
		log.Println(err)
		return
	} else {
		rf.currentTerm = content.Term
		rf.votedFor = content.VotedFor
		rf.log = content.Log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	Forbidden   bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevlogIndex int
	PrevLogTerm  int
	Entries      Log
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	GetLog  bool
	Handled bool
}

// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if len(rf.log) >= 1 {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			rf.persist()
			rf.mu.Unlock()
		} else if args.LastLogIndex >= rf.log[len(rf.log)-1].Index && args.LastLogTerm == rf.log[len(rf.log)-1].Term {
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			rf.persist()
			rf.mu.Unlock()
		} else {
			reply.Forbidden = true
			reply.VoteGranted = false
		}
	} else {
		if rf.Votes == 0 && args.Term >= rf.currentTerm {
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			rf.persist()
			rf.mu.Unlock()
		} else {
			reply.VoteGranted = false
		}

	}
	fmt.Println("vote:  " + strconv.Itoa(args.CandidateId) + "->" + strconv.Itoa(rf.me) + "  " + strconv.FormatBool(reply.VoteGranted))
}
func (rf *Raft) HeatBeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.Forbidden = false
	if args.Term >= rf.currentTerm || rf.MyState != Leader {
		rf.HeartsBeatsIndex++
		if args.LeaderId != rf.LeaderId {
			rf.LastGetIndex = 0
		}
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.persist()
		rf.mu.Unlock()
		rf.MyState = Follower
		rf.isLeader = false
		rf.LeaderId = args.LeaderId
		rf.Votes = 0
		//rf.LastGetIndex = 0
		if args.Entries.Index != -1 {
			if args.PrevlogIndex == 0 {
				rf.mu.Lock()
				rf.log = append(rf.log, args.Entries)
				rf.persist()
				rf.mu.Unlock()
				reply.Handled = true
				reply.GetLog = true
				rf.LastGetIndex = args.Entries.Index
				rf.Applied = append(rf.Applied, false)
			} else {
				if len(rf.log) >= args.PrevlogIndex && rf.log[args.PrevlogIndex-1].Term == args.PrevLogTerm {
					if len(rf.log) > args.PrevlogIndex {
						rf.mu.Lock()
						rf.log[args.PrevlogIndex] = args.Entries
						rf.persist()
						rf.mu.Unlock()
					}
					rf.mu.Lock()
					rf.log = append(rf.log, args.Entries)
					rf.persist()
					rf.mu.Unlock()
					rf.Applied = append(rf.Applied, false)
					rf.LastGetIndex = args.Entries.Index
					fmt.Println(strconv.Itoa(rf.me) + "get")
					fmt.Println(args.Entries)
					reply.GetLog = true
					reply.Handled = true
				} else {
					fmt.Println("move back")
					reply.GetLog = false
					reply.Handled = true
				}
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > rf.LastGetIndex {
				rf.commitIndex = rf.LastGetIndex
				fmt.Println("get commit message from" + strconv.Itoa(args.LeaderId) + "and the lastgetindex is" + strconv.Itoa(rf.LastGetIndex))
			} else {
				rf.commitIndex = args.LeaderCommit
				fmt.Println("get commit message from" + strconv.Itoa(args.LeaderId) + "and the leadercommit is" + strconv.Itoa(args.LeaderCommit))
			}
		}
	} else {
		reply.Term = rf.currentTerm
	}
	//fmt.Println("heatbeat: " + strconv.Itoa(args.LeaderId) + "->" + strconv.Itoa(rf.me) + "  ")
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
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) SendHeatBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HeatBeat", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	index = len(rf.log) + 1
	term = rf.currentTerm
	isLeader = rf.isLeader
	rf.Applies = append(rf.Applies, 1)
	rf.Applied = append(rf.Applied, false)
	if isLeader {
		rf.mu.Lock()
		rf.log = append(rf.log, Log{Term: rf.currentTerm, Command: command, Index: index})
		rf.persist()
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
		}
		fmt.Println("start " + strconv.Itoa(index) + " task")
		fmt.Println(rf.log)
	}
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.MyState != Leader {
			heatbeatindex := rf.HeartsBeatsIndex
			ms := 350 + (rand.Int63() % 200)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			heatbeatindexnext := rf.HeartsBeatsIndex
			if heatbeatindexnext == heatbeatindex && rf.MyState != Leader && !rf.Forbidden {
				rf.MyState = Candidator
			}
		}
	}
}
func (rf *Raft) SendVotes(index int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.SendRequestVote(index, args, reply)
	if reply.VoteGranted && args.Term == rf.currentTerm {
		rf.mu.Lock()
		rf.Votes++
		rf.mu.Unlock()
		fmt.Println(strconv.Itoa(rf.me) + "now votes is" + strconv.Itoa(rf.Votes) + "and currentterm is" + strconv.Itoa(args.Term))
	} else {
		fmt.Println(strconv.Itoa(rf.me) + "now votes is" + strconv.Itoa(rf.Votes) + "and currentterm is" + strconv.Itoa(args.Term))
	}
	if reply.Forbidden {
		rf.Forbidden = true
	}
}
func (rf *Raft) SendHeatBeats(index int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.SendHeatBeat(index, args, reply)
	if reply.Handled {
		if reply.GetLog {
			rf.Applies[args.Entries.Index-1]++
			rf.nextIndex[index]++
			if !rf.Applied[args.Entries.Index-1] && (2*rf.Applies[args.Entries.Index-1] > len(rf.peers)) {
				rf.Applied[args.Entries.Index-1] = true
				rf.commitIndex = args.Entries.Index
				//fmt.Println(strconv.Itoa(index) + "get" + strconv.Itoa(args.Entries.Index))
				//fmt.Println("commitindex is " + strconv.Itoa(rf.commitIndex))
			}
		} else if !reply.GetLog && args.Entries.Index != -1 {
			rf.nextIndex[index]--
			fmt.Println(strconv.Itoa(index) + " peer nextindex is " + strconv.Itoa(rf.nextIndex[index]))
		}
	}
	if reply.Term > rf.currentTerm {
		rf.MyState = Follower
		rf.isLeader = false
		rf.mu.Lock()
		rf.currentTerm = reply.Term
		rf.persist()
		rf.mu.Unlock()
	}
}
func (rf *Raft) Apply() {
	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied {
			fmt.Println("commitindex is " + strconv.Itoa(rf.commitIndex))
			fmt.Println("lastapplied is " + strconv.Itoa(rf.lastApplied))
			msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.log[rf.lastApplied].Index, CommandTerm: rf.log[rf.lastApplied].Term}
			rf.applyCh <- msg
			rf.Applied[rf.lastApplied] = true
			rf.lastApplied++
			fmt.Println(strconv.Itoa(rf.me) + " apply " + strconv.Itoa(rf.lastApplied) + "when the leader is" + strconv.Itoa(rf.LeaderId))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func (rf *Raft) Loop() {
	for !rf.killed() {
		switch rf.MyState {
		case Follower:
			//fmt.Println(strconv.Itoa(rf.me) + "is follow")
			//time.Sleep(10 * time.Millisecond)
			rf.Votes = 0
		case Candidator:
			//fmt.Println(strconv.Itoa(rf.me) + "is candidator")
			rf.mu.Lock()
			rf.votedFor = rf.me
			rf.currentTerm++
			rf.persist()
			rf.mu.Unlock()
			rf.mu.Lock()
			rf.Votes = 0
			rf.Votes++
			rf.mu.Unlock()
			fmt.Println("the init votes of" + strconv.Itoa(rf.me) + "is" + strconv.Itoa(rf.Votes) + "and the term is" + strconv.Itoa(rf.currentTerm))
			args := RequestVoteArgs{}
			args.CandidateId = rf.me
			args.Term = rf.currentTerm
			if len(rf.log) >= 1 {
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
				args.LastLogIndex = rf.log[len(rf.log)-1].Index
			}
			reply := RequestVoteReply{}
			reply.VoteGranted = false
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go rf.SendVotes(i, &args, &reply)
			}
			time.Sleep(10 * time.Millisecond)
			fmt.Println(strconv.Itoa(rf.me) + "get " + strconv.Itoa(rf.Votes) + "vote")
			if 2*rf.Votes > len(rf.peers) && !rf.Forbidden {
				rf.MyState = Leader
				rf.isLeader = true
				rf.LeaderId = rf.me
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex = append(rf.nextIndex, len(rf.log)+1)
					//fmt.Println(rf.currentTerm)
					//fmt.Println(rf.nextIndex[i])
					rf.matchIndex = append(rf.matchIndex, 0)
				}
			} else {
				rf.MyState = Follower
			}
		case Leader:
			//fmt.Println(strconv.Itoa(rf.me) + "is leader")
			//fmt.Println(rf.nextIndex)
			for i := 0; i < len(rf.peers); i++ {
				args := AppendEntriesArgs{}
				args.LeaderId = rf.me
				args.Term = rf.currentTerm
				args.LeaderCommit = rf.commitIndex
				args.Entries.Index = -1
				reply := AppendEntriesReply{}
				reply.Success = false
				reply.Handled = false
				if i == rf.me {
					continue
				}
				if len(rf.log) > 0 && rf.nextIndex[i] <= len(rf.log) {
					fmt.Println("the leader is" + strconv.Itoa(rf.me))
					//fmt.Println(rf.nextIndex[i])
					if rf.nextIndex[i] > 1 {
						args.PrevlogIndex = rf.nextIndex[i] - 1
						args.PrevLogTerm = rf.log[args.PrevlogIndex-1].Term
					}
					args.Entries = rf.log[rf.nextIndex[i]-1]
					fmt.Println("send" + strconv.Itoa(i) + "log")
					fmt.Println(args.Entries)
					//fmt.Println(rf.log)
				}
				go rf.SendHeatBeats(i, &args, &reply)
			}
			time.Sleep(100 * time.Millisecond)
		}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.currentTerm = 0
	rf.isLeader = false
	rf.MyState = 0
	rf.Votes = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.mu.Unlock()
	rf.votedFor = len(rf.peers) + 1
	rf.HeartsBeatsIndex = 0
	rf.LeaderId = len(rf.peers) + 1
	rf.applyCh = applyCh
	rf.LastGetIndex = 0
	rf.Forbidden = false
	go rf.Apply()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	if len(rf.log) >= 1 {
		for i := 0; i < len(rf.log); i++ {
			rf.Applies = append(rf.Applies, 1)
			rf.Applied = append(rf.Applied, false)
		}
	}
	go rf.ticker()
	go rf.Loop()
	return rf
}
