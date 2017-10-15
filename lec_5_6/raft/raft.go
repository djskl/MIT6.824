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
	"errors"
	"bytes"
	"encoding/gob"
)

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
	apyCh chan ApplyMsg

	heartbeats []int
}

func (rf *Raft) TurnToLeader() {
	rf.ResetTimeOut()

	for idx := 0; idx < len(rf.peers); idx++ {
		rf.nextIndex[idx] = len(rf.logs)
		rf.matchIndex[idx] = 0
	}

	rf.Start(-1)

	rf.persist()

	rf.startHeartBeat()

}

func (rf *Raft) TurnToCandidater() {
	atomic.StoreInt32(&rf.votes, 1)     //rf.resetVotes();rf.incrVotes()
	atomic.AddInt32(&rf.currentTerm, 1) //rf.incrTerm(1)
	rf.votedFor = rf.me
	go func() {
		rf.persist()
	}()
	rf.ResetTimeOut()
	rf.requestVotes()
}

func (rf *Raft) TurnToFollower(leaderIdx int, term int32) {
	atomic.StoreInt32(&rf.votes, 0)
	rf.votedFor = leaderIdx
	atomic.StoreInt32(&rf.currentTerm, term) //rf.currentTerm = args.Term
	go func() {
		rf.persist()
	}()
}

func (rf *Raft) updateServerIndex(leaderCmtIdx int32) {
	cmtIdx, logNums := int(leaderCmtIdx), len(rf.logs)

	if cmtIdx > logNums {
		atomic.StoreInt32(&rf.commitIndex, int32(logNums-1))
	} else {
		atomic.StoreInt32(&rf.commitIndex, int32(leaderCmtIdx))
	}

	DPrintln(rf.me, rf.commitIndex, rf.commitIndex, "之前的日志可以提交...")

}

func (rf *Raft) updateLeaderIndex(serverIdx int, nxtIdx int) {

	rf.matchIndex[serverIdx] = rf.nextIndex[serverIdx] - 1

	for idx := rf.matchIndex[serverIdx]; idx > -1; idx-- {

		err, aEntry := rf.getLogEntry(idx) //aEntry := rf.logs[idx]
		if err != nil {
			continue
		}

		nums := 0
		for s, _ := range rf.peers {
			if rf.matchIndex[s] >= idx {
				nums += 1
			}
		}

		COUNT := len(rf.peers)/2 + 1
		if aEntry.Term != rf.currentTerm {
			COUNT = len(rf.peers)
		}

		if nums >= COUNT {
			atomic.StoreInt32(&rf.commitIndex, int32(idx)) //rf.commitIndex = idx
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
	rf.persist()
}

func (rf *Raft) addLogEntry(entry Entry) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs = append(rf.logs, entry)
	rf.persist()
	size := len(rf.logs)
	rf.nextIndex[rf.me] = size
	rf.matchIndex[rf.me] = size - 1
	return size - 1
}

func (rf *Raft) getLogEntry(idx int) (error, Entry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if idx < 0 || idx >= len(rf.logs) {
		err := errors.New("Out of Index")
		return err, Entry{}
	}
	return nil, rf.logs[idx]
}

func (rf *Raft) getLogEntries(beg int) []Entry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	size := len(rf.logs)
	if size == 0 || beg < 0 || beg >= size {
		return nil
	}
	return rf.logs[beg:]
}

func (rf *Raft) delLogEntries(beg int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	size := len(rf.logs)
	if size == 0 || beg < 0 || beg > size {
		return
	}
	rf.logs = rf.logs[:beg]
	rf.persist()
}

func (rf *Raft) getFirstIndex(pos int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	size := len(rf.logs)
	if size == 0 || pos >= size || pos < 0 {
		return size
	}

	term := rf.logs[pos].Term
	for pos > -1 && rf.logs[pos].Term == term {
		pos--
	}
	pos++
	return pos
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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.logs)
	e.Encode(rf.votes)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.logs)
	d.Decode(&rf.votes)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.currentTerm)

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

	rf.sendAppendEntries()

	DPrintln(rf.me, rf.currentTerm, "Get Command:", index, command)

	return index, term, isLeader
}

func (rf *Raft) StartApply() {
	go func() {
		for {
			t := false

			cmtIdx := int(atomic.LoadInt32(&rf.commitIndex))

			for rf.lastApplied < cmtIdx {
				t = true
				toIdx := rf.lastApplied + 1
				err, logEntry := rf.getLogEntry(toIdx)
				if err != nil {
					DPrintln(rf.me, toIdx, "不存在", rf.logs)
					break
				}
				rf.apyCh <- ApplyMsg{toIdx, logEntry.Command, false, nil}
				rf.lastApplied = toIdx
			}

			if t {
				DPrintln(rf.me, rf.currentTerm, rf.lastApplied, rf.logs[rf.lastApplied], cmtIdx, rf.logs[cmtIdx], "已应用")
			}

			time.Sleep(HEARTBEAT_TIMEOUT)
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
						rf.TurnToFollower(-1, rf.currentTerm)
					}
				} else {
					DPrintln("server:", rf.me, "term:", rf.currentTerm, "timeout:", rand_timeout, "开始竞选...", rf.logs)
					rf.TurnToCandidater()
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

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for idx := 0; idx < len(rf.peers); idx++ {
		rf.nextIndex[idx] = 1
	}

	rf.heartbeats = make([]int, len(rf.peers))

	// Your initialization code here (2A, 2B, 2C).
	rf.apdCh = make(chan bool)
	rf.expCh = make(chan bool)
	rf.apyCh = applyCh

	rf.StartTimeOut()
	rf.StartApply()

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
