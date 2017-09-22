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
	"time"
	"math/rand"
	"math"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Entry struct {
	Term    int
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	expire_ch chan bool
	votes     int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var isleader bool
	// Your code here (2A).
	if rf.votes > len(rf.peers)/2 {
		isleader = true
	}
	return rf.currentTerm, isleader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type AppendEntriesArgs struct {
	Term        int
	Items       []Entry
	PreLogTerm  int
	PreLogIndex int
	LeaderID    int
	CommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.LeaderID != rf.me {
		rf.votes = 0
		rf.votedFor = args.LeaderID
	}
	rf.expire_ch <- true

	rf.currentTerm = args.Term
	reply.Term = args.Term
	preLogIdx := args.PreLogIndex

	if preLogIdx == -1 {
		reply.Success = true
		return
	}

	if preLogIdx > len(rf.logs) {
		reply.Success = false
		return
	}

	preLogEntry := rf.logs[args.PreLogIndex]
	if preLogEntry.Term != args.PreLogTerm {
		reply.Success = false
		return
	}

	if preLogIdx == len(rf.logs) {
		for _, item := range (args.Items) {
			rf.logs = append(rf.logs, item)
		}
	} else {
		rf.logs = rf.logs[:args.PreLogIndex+1]
		for _, item := range (rf.logs) {
			rf.logs = append(rf.logs, item)
		}
	}
	reply.Success = true

	rf.commitIndex = int(math.Min(float64(args.CommitIndex), float64(len(rf.logs))))

}

func (rf *Raft) sendAppendEntries() {
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func() {
			for {
				nx := rf.nextIndex[server]
				preLogTerm := 0
				preLogIndex := -1
				toEntries := []Entry{}
				if nx > 1 {
					preLogIndex = nx - 1
					preLog := rf.logs[preLogIndex]
					preLogTerm = preLog.Term
					toEntries = rf.logs[nx:]
				}
				args := &AppendEntriesArgs{
					rf.currentTerm,
					toEntries,
					preLogTerm,
					preLogIndex,
					rf.me,
					rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
				if ok && reply.Success {
					rf.nextIndex[server] += len(toEntries)
					rf.matchIndex[server] = rf.nextIndex[server] - 1
					//更新rf.commitIndex
				}

				if ok {
					time.Sleep(100 * time.Millisecond)
				} else {
					continue
				}
			}
		}()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateIdx int
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
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//在当前term内，已经投票后，就不能再给其他节点投票了
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateIdx {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	lastLogTerm := 0
	lastLogIndex := len(rf.logs) - 1
	if lastLogIndex > -1 {
		lastLogTerm = rf.logs[lastLogIndex].Term
	}

	if args.LastLogTerm > lastLogTerm {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateIdx
		return
	}

	if args.LastLogTerm < lastLogTerm || args.LastLogIndex < lastLogIndex {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateIdx
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
// may fail or lose an election.
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
	rf.votedFor = -1

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for idx := 0; idx < len(rf.peers); idx++ {
		rf.nextIndex[idx] = 1
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.expire_ch = make(chan bool)
	go func() {
	timeout_loop:
		for {
			rand_timeout := 300 + rand.Intn(200)
			select {
			case <-rf.expire_ch:
				break
			case <-time.After(time.Millisecond * time.Duration(rand_timeout)):
				break timeout_loop
			}
		}

		rf.votes = 1
		rf.currentTerm += 1
		rf.votedFor = rf.me

		lastLogItem := 0
		lastLogIdx := len(rf.logs) - 1

		if lastLogIdx > -1 {
			lastLogItem = rf.logs[lastLogIdx].Term
		}

		voteArgs := &RequestVoteArgs{
			rf.currentTerm,
			rf.me,
			lastLogIdx,
			lastLogItem,
		}

		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
			go func() {
				voteReply := &RequestVoteReply{}

				for {
					ok := rf.sendRequestVote(idx, voteArgs, voteReply)
					if ok || rf.votedFor != me {
						break
					}
				}

				if voteReply.VoteGranted {
					rf.votes += 1
					if rf.votes == len(rf.peers)/2+1 {
						rf.sendAppendEntries()
					}
				}
			}()
		}

	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
