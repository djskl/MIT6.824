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
	"time"
	"math/rand"
	"MIT6.824/lec_5_6/labrpc"
	"sync/atomic"
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
	Term    int32
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
	votedFor    int
	currentTerm int32
	logs        []Entry

	votes int32

	commitIndex int32
	lastApplied int

	nextIndex  []int
	matchIndex []int

	expCh chan bool
	apdCh chan bool
	cmtCh chan int
	apyCh chan ApplyMsg

	heartbeats []int
	logChs     []chan int
}

func (rf *Raft) TurntoFollower(serverIdx int) {
	atomic.StoreInt32(&rf.votes, 0)
	rf.votedFor = serverIdx
}

func (rf *Raft) updateServerIndex(leaderCmtIdx int32, size int) {
	cmtIdx := int(leaderCmtIdx)
	if cmtIdx > size {
		atomic.StoreInt32(&rf.commitIndex, int32(size-1))
		//rf.commitIndex = size - 1
	} else {
		atomic.StoreInt32(&rf.commitIndex, int32(leaderCmtIdx))
		//rf.commitIndex = leaderCmtIdx
	}

	go func(cmtIdx int32) {
		rf.cmtCh <- int(cmtIdx)
	}(rf.commitIndex)

}

func (rf *Raft) updateLeaderIndex(serverIdx int, size int) {
	if size < 1 {
		return
	}

	rf.nextIndex[serverIdx] += size 						//atomic.AddInt32(&rf.nextIndex[serverIdx], int32(size))
	rf.matchIndex[serverIdx] = rf.nextIndex[serverIdx] - 1 	//atomic.StoreInt32(&rf.matchIndex[serverIdx], int32(idx))

	for	idx := rf.nextIndex[serverIdx]-1;idx > -1;idx-- {

		aEntry := rf.logs[idx]

		//不能提交上一个term的日志
		if aEntry.Term != rf.currentTerm {
			break
		}

		nums := 0
		for s, _ := range rf.peers {
			if rf.matchIndex[s] >= idx {
				nums += 1
			}
		}

		if nums > len(rf.peers)/2 {
			atomic.StoreInt32(&rf.commitIndex, int32(idx)) //rf.commitIndex = idx
			go func(cmtIdx int32) {
				rf.cmtCh <- int(cmtIdx)
			}(rf.commitIndex)
			break
		}
	}
}

func (rf *Raft) addLogEntries(entries []Entry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for _, item := range (entries) {
		rf.logs = append(rf.logs, item)
	}
}

func (rf *Raft) addLogEntry(entry Entry) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs = append(rf.logs, entry)
	size := len(rf.logs)
	rf.nextIndex[rf.me] = size
	rf.matchIndex[rf.me] = size - 1
	return size - 1
}

func (rf *Raft) getLogEntry(idx int) Entry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.logs[idx]
}

func (rf *Raft) delLogEntry(beg int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs = rf.logs[:beg]
}

func (rf *Raft) requestVotes() {
	atomic.StoreInt32(&rf.votes, 1)	//rf.resetVotes();rf.incrVotes()
	atomic.AddInt32(&rf.currentTerm, 1)	//rf.incrTerm(1)

	rf.votedFor = rf.me
	lastLogIdx := len(rf.logs) - 1

	var lastLogItem int32
	if lastLogIdx > -1 {
		lastLogItem = rf.logs[lastLogIdx].Term
	}

	voteArgs := &RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		lastLogIdx,
		lastLogItem,
	}

	rf.ResetTimeOut()

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(serverIdx int) {
			voteReply := &RequestVoteReply{}
			DPrintln(rf.me, rf.currentTerm, "请求", serverIdx, "投票")
			for {
				ok := rf.sendRequestVote(serverIdx, voteArgs, voteReply)
				if ok {
					break
				}
				if rf.votedFor != rf.me || voteArgs.Term != rf.currentTerm {
					return
				}
			}

			if voteArgs.Term != rf.currentTerm {
				return
			}

			if voteReply.Term > rf.currentTerm {
				rf.currentTerm = voteReply.Term
				rf.TurntoFollower(-1)
				return
			}

			if voteReply.VoteGranted {
				//currentVotes := rf.incrVotes()
				currentVotes := int(atomic.AddInt32(&rf.votes, 1))
				if currentVotes == len(rf.peers)/2+1 {
					DPrintln(rf.me, rf.currentTerm, "当选", rf.logs)
					rf.ResetTimeOut()
					for idx := 0; idx < len(rf.peers); idx++ {
						rf.nextIndex[idx] = len(rf.logs)
						rf.matchIndex[idx] = 0
					}
					rf.sendAppendEntries()
				}
			}
		}(idx)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	if int(rf.votes) > len(rf.peers)/2 {
		return int(rf.currentTerm), true
	}

	return int(rf.currentTerm), false
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
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, false
	}

	logEntry := Entry{int32(term), command}

	index = rf.addLogEntry(logEntry)

	for idx := 0; idx < len(rf.peers); idx++ {
		if idx == rf.me {
			continue
		}
		go func(serverIdx int) {
			rf.logChs[serverIdx] <- index
		}(idx)
	}

	DPrintln("Get Command:", rf.me, rf.currentTerm, index, command)

	return index, term, isLeader
}

func (rf *Raft) StartApply() {
	go func() {
		for {
			cmtIdx := <-rf.cmtCh //cmtIdx := int(atomic.LoadInt32(&rf.commitIndex))
			for rf.lastApplied < cmtIdx {
				toIdx := rf.lastApplied + 1
				logEntry := rf.getLogEntry(toIdx) //rf.logs[toIdx]
				rf.apyCh <- ApplyMsg{toIdx, logEntry.Command, false, nil}
				rf.lastApplied = toIdx
			}
		}
	}()
}

func (rf *Raft) StartTimeOut() {
	go func() {
		for {
			rand_timeout := ELECTION_TIMEOUT_BASE + rand.Intn(ELECTION_TIMEOUT_FACT)
			select {
			case <-rf.expCh:
				break
			case <-time.After(time.Millisecond * time.Duration(rand_timeout)):
				_, isLeader := rf.GetState()
				if isLeader {
					nums := 1
					for _, v := range rf.heartbeats {
						nums += v
					}
					for idx := 0; idx < len(rf.heartbeats); idx++ {
						rf.heartbeats[idx] = 0
					}
					if nums <= len(rf.peers)/2 {
						DPrintln("server:", rf.me, "term:", rf.currentTerm, "timeout:", rand_timeout, "与大部分节点断开...", nums)
						rf.TurntoFollower(-1)
					}
				} else {
					DPrintln("server:", rf.me, "term:", rf.currentTerm, "timeout:", rand_timeout, "开始竞选...", rf.logs)
					rf.requestVotes()
				}
				break
			}
		}
	}()
}

func (rf *Raft) ResetTimeOut() {
	go func() {
		select {
		case rf.expCh <- true:
			break
		case <-time.After(time.Second * time.Duration(1)):
			break
		}
	}()
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
	rf.apyCh = applyCh

	rf.logChs = make([]chan int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for idx := 0; idx < len(rf.peers); idx++ {
		rf.nextIndex[idx] = 1
		rf.logChs[idx] = make(chan int)
	}

	rf.heartbeats = make([]int, len(rf.peers))

	// Your initialization code here (2A, 2B, 2C).
	rf.apdCh = make(chan bool)
	rf.expCh = make(chan bool)
	rf.cmtCh = make(chan int)

	rf.StartTimeOut()
	rf.StartApply()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
