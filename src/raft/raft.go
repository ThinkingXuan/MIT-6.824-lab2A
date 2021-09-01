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
	"fmt"
	_ "fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

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
	Follower = 0
	Candidate = 1
	Leader = 2
)

const (
	ElectionTimeOut = time.Millisecond*150
	HeartBeatTimeOut = time.Millisecond*120
	RpcTimeOut = time.Millisecond*50
)

const (
	NoBody = -1
)

type Role int

type LogEntry struct {
	Term int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role Role
	curTerm int
	votedFor int					// -1: no vote anybody

	electionTimeOut *time.Timer
	heartBeatTimeOut *time.Timer

	log []LogEntry
	// all raft nodes stay by
	commitIdx int
	lastApplied int
	// leader stay by
	nextIdx []int
	matchIdx []int

	applyCh chan ApplyMsg
}

// generate electionTimeOut about 150ms~300ms
func randElectionTimeOut() time.Duration {
	t := time.Duration(rand.Int63()) % ElectionTimeOut
	return ElectionTimeOut+t
}

// heartBeat gap about 150ms
func fixedHeartBeatTimeOut() time.Duration {
	return HeartBeatTimeOut
}

// rpcTimeOut gap about 100ms
func fixRpcTimeOut() time.Duration {
	return RpcTimeOut
}

// my Raft ctor
func NewRaft(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers: peers,
		me: me,
		persister: persister,
		role: Follower,
		curTerm: 0,
		votedFor: NoBody,
		electionTimeOut: time.NewTimer(randElectionTimeOut()),
		heartBeatTimeOut: time.NewTimer(fixedHeartBeatTimeOut()),
		log: make([]LogEntry, 0),
		commitIdx: -1,
		lastApplied: -1,
		nextIdx: make([]int, len(peers)),
		matchIdx: make([]int, len(peers)),
		applyCh: applyCh,
	}
	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.curTerm
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
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
	Term int
	CandId int
	LastLogIdx int
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

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIdx int
	PrevTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

// just for RequestVote
func (rf *Raft) notVoteFor(reply *RequestVoteReply) {
	reply.Term = rf.curTerm
	reply.VoteGranted = false
}

// just for RequestVote
func (rf *Raft) voteForIt(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.votedFor = args.CandId
	rf.curTerm = args.Term
	rf.role = Follower
	rf.electionTimeOut.Reset(randElectionTimeOut())
	rf.heartBeatTimeOut.Stop()
	reply.Term = rf.curTerm
	reply.VoteGranted = true
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.curTerm > args.Term {
		rf.notVoteFor(reply)
		//fmt.Printf("[%v->%v], term..%v, not voteFor in RequestVote\n", rf.me, args.CandId, rf.curTerm)
	} else if rf.curTerm < args.Term {
		rf.voteForIt(args, reply)
		//fmt.Printf("[%v->%v], term..%v, voteFor in RequestVote\n", rf.me, args.CandId, rf.curTerm)
	} else {
		switch rf.role {
		case Candidate:
			rf.notVoteFor(reply)
			//fmt.Printf("[%v->%v], term..%v, not voteFor in RequestVote\n", rf.me, args.CandId, rf.curTerm)
			break
		case Leader:
			rf.notVoteFor(reply)
			//fmt.Printf("[%v->%v], term..%v, not voteFor in RequestVote\n", rf.me, args.CandId, rf.curTerm)
			break
		case Follower:
			if rf.votedFor == NoBody {
				rf.voteForIt(args, reply)
				//fmt.Printf("[%v->%v], term..%v, voteFor in RequestVote\n", rf.me, args.CandId, rf.curTerm)
			} else {
				rf.notVoteFor(reply)
				//fmt.Printf("[%v->%v], term..%v, not voteFor in RequestVote\n", rf.me, args.CandId, rf.curTerm)
			}
			break
		}
	}
}

// just for HeartBeat
func (rf *Raft) rejectHeartBeat(reply *AppendEntriesReply) {
	reply.Term = rf.curTerm
	reply.Success = false
}

// just for HeartBeat
func (rf *Raft) respondHeartBeat(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	rf.votedFor = args.LeaderId
	rf.curTerm = args.Term
	rf.role = Follower
	rf.electionTimeOut.Reset(randElectionTimeOut())
	rf.heartBeatTimeOut.Stop()
	reply.Term = rf.curTerm
	reply.Success = true
}

func (rf *Raft) HeartBeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.curTerm > args.Term {
		rf.rejectHeartBeat(reply)
		//fmt.Printf("[%v->%v], term %v, rejectHeartBeat in HeartBeat\n", rf.me, args.LeaderId, rf.curTerm)
	} else {
		rf.respondHeartBeat(args, reply)
		//fmt.Printf("[%v->%v], term %v, respondHeartBeatin HeartBeat\n", rf.me, args.LeaderId, rf.curTerm)
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

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	if rf.role == Leader {
		rf.log = append(rf.log, LogEntry{
			Term: rf.curTerm,
			Command: command,
		})
		index = len(rf.log)
		isLeader = true
	}
	term = rf.curTerm

	rf.mu.Unlock()
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

// for sendHeartBeat and becomeLeader
func (rf *Raft) stillBeLeader() {
	rf.votedFor = rf.me
	rf.role = Leader
	rf.heartBeatTimeOut.Reset(fixedHeartBeatTimeOut())
}

// just for sendRequestVote
func (rf *Raft) becomeLeader() {
	rf.stillBeLeader()
	rf.electionTimeOut.Stop()
}

// for sendHeartBeat and sendRequestVote
func (rf *Raft) turnBackFollower() {
	rf.votedFor = NoBody
	rf.role = Follower
	rf.electionTimeOut.Reset(randElectionTimeOut())
	rf.heartBeatTimeOut.Stop()
}

// for seekVotes, don't wait if one peer disconnect
func (rf *Raft) sendRequestVoteToPeer(serv int, request *RequestVoteArgs, reply *RequestVoteReply) bool {
	rcpTimeOut := time.NewTimer(fixRpcTimeOut())
	defer rcpTimeOut.Stop()

	okCh := make(chan bool, 1)
	go func() {
		ok := rf.sendRequestVote(serv, request, reply)
		if ok {
			okCh <- ok
		}
	}()
	select {
	case <-rcpTimeOut.C:
		return false
	case <-okCh:
		return true
	}
}

// as candidate, seek votes
func (rf *Raft) seekVotes() {
	cnt := 1			// vote counter
	var wg sync.WaitGroup
	wg.Add(len(rf.peers)-1)

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(voteIdx int) {
			reply := RequestVoteReply{}
			request := RequestVoteArgs{
				Term: rf.curTerm,
				CandId: rf.me,
			}
			// seek votes from peers
			fmt.Printf("[%v->%v] in seekVotes\n", rf.me, voteIdx)
			ok := rf.sendRequestVoteToPeer(voteIdx, &request, &reply)
			if ok {
				fmt.Printf("[%v<-%v] feedback in time in seekVotes\n", rf.me, voteIdx)
				if reply.VoteGranted == true {
					fmt.Printf("[%v<-%v], term %v, vote for me in seekVotes\n", rf.me, voteIdx, reply.Term)
					cnt++
				} else {
					fmt.Printf("[%v<-%v], term %v, not vote for me in seekVotes\n", rf.me, voteIdx, reply.Term)
				}
			} else {
				fmt.Printf("[%v<-%v] timeout in sendHeartBeatToPeers\n", rf.me, voteIdx)
			}
			wg.Done()
			return
		}(idx)
	}
	wg.Wait()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// judge this candidate become the leader?
	if cnt >= (len(rf.peers)+1)/2 {
		rf.becomeLeader()
		fmt.Printf("I'm..%v, term..%v, become a Leader in seekVotes\n", rf.me, rf.curTerm)
	} else {
		rf.turnBackFollower()
		fmt.Printf("I'm..%v, term..%v, failed to a Leader in seekVotes\n", rf.me, rf.curTerm)
	}
}

func (rf *Raft) sendHeartBeatToPeer(serv int, request *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rcpTimeOut := time.NewTimer(fixRpcTimeOut())
	defer rcpTimeOut.Stop()

	okCh := make(chan bool, 1)
	go func() {
		ok := rf.sendHeartBeat(serv, request, reply)
		if ok {
			okCh <- ok
		}
	}()
	select {
	case <-rcpTimeOut.C:
		return false
	case <-okCh:
		return true
	}
}

// as a leader, send heartBeat to followers
func(rf *Raft) sendHeartBeatToPeers() {
	cnt := 1			// follower feedback counter
	var wg sync.WaitGroup
	wg.Add(len(rf.peers)-1)
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(voteIdx int) {
			reply := AppendEntriesReply{}
			heartBeat := AppendEntriesArgs{
				Term:         rf.curTerm,
				LeaderId:     rf.me,
			}
			// send appendEntries to peers
			fmt.Printf("[%v->%v] in sendHeartBeatToPeers\n", rf.me, voteIdx)
			ok := rf.sendHeartBeatToPeer(voteIdx, &heartBeat, &reply)
			if ok {
				fmt.Printf("[%v<-%v] feedback in time in sendHeartBeatToPeers\n", rf.me, voteIdx)
				if reply.Success==true && reply.Term==rf.curTerm {
					fmt.Printf("node..%v follows %v in sendHeartBeatToPeers\n", voteIdx, rf.me)
					cnt++
				} else {
					fmt.Printf("node..%v not follow %v in sendHeartBeatToPeers\n", voteIdx, rf.me)
				}
			} else {
				fmt.Printf("[%v<-%v] timeout in sendHeartBeatToPeers\n", rf.me, voteIdx)
			}
			wg.Done()
			return
		}(idx)
	}
	wg.Wait()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// the majority of Followers react
	if cnt >= (len(rf.peers)+1)/2 {
		rf.stillBeLeader()
		fmt.Printf("I'm %v, a Leader, receive feedbacks successfully in sendHeartBeatToPeers\n", rf.me)
	} else {
		rf.turnBackFollower()
		fmt.Printf("I'm %v, a Leader, receive feedbacks failed, turn back Follower in sendHeartBeatToPeers\n", rf.me)
	}
}

// just for Loop
func (rf *Raft) becomeCandidate() {
	rf.role = Candidate
	rf.curTerm++
}

func(rf *Raft) Loop() {
	for {
		switch rf.role {
		case Follower:
			select {
			case <-rf.electionTimeOut.C:
				rf.becomeCandidate()
				fmt.Printf("election time out, I'm..%v, term %v turn to candidate in Loop\n", rf.me, rf.curTerm)
			}
			break
		case Candidate:
			fmt.Printf("I'm..%v, term..%v, seek vote in Loop\n", rf.me, rf.curTerm)
			rf.seekVotes()
			fmt.Printf("I'm..%v, term..%v, seeked vote in Loop\n", rf.me, rf.curTerm)
			break
		case Leader:
			select {
			case <-rf.heartBeatTimeOut.C:
				rf.sendHeartBeatToPeers()
			}
			break
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
	rf := NewRaft(peers, me, persister, applyCh)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// main function
	go rf.Loop()

	return rf
}
