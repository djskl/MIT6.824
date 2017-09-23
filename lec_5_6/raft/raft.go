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
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
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

func (rf *Raft) updateIndex(serverIdx int, size int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex[serverIdx] += size
	idx := rf.nextIndex[serverIdx] - 1
	rf.matchIndex[serverIdx] = idx

	for idx > -1 {
		nums := 0
		for s, _ := range rf.peers {
			if rf.matchIndex[s] >= idx {
				nums += 1
			}
		}
		if nums > len(rf.peers) {
			rf.commitIndex = idx
			break
		}
		idx--
	}

}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm
}

func (rf *Raft) incrTerm(deltaTerm int) {
	rf.currentTerm += deltaTerm
}

func (rf *Raft) setTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) getVotes() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.votes
}

func (rf *Raft) incrVotes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votes += 1
	return rf.votes
}

func (rf *Raft) resetVotes() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votes = 0
}

func (rf *Raft) getNextIndex(server int) int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	nx := rf.nextIndex[server]
	return nx
}

func (rf *Raft) incrNextIndex(server int, deltaNX int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server] += deltaNX
}

func (rf *Raft) requestVotes() {
	rf.resetVotes()
	rf.incrVotes()
	rf.incrTerm(1)

	rf.votedFor = rf.me

	lastLogItem := 0
	lastLogIdx := len(rf.logs) - 1

	if lastLogIdx > -1 {
		lastLogItem = rf.logs[lastLogIdx].Term
	}

	voteArgs := &RequestVoteArgs{
		rf.getCurrentTerm(),
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
				if ok || rf.votedFor != rf.me {
					break
				}
			}
			if voteReply.VoteGranted {
				currentVotes := rf.incrVotes()
				if currentVotes == len(rf.peers)/2+1 {
					rf.sendAppendEntries()
				}
			}
		}()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var isleader bool
	// Your code here (2A).
	currentVotes := rf.getVotes()
	if currentVotes > len(rf.peers)/2 {
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
		rf.requestVotes()
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
