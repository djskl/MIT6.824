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
	"fmt"
	"MIT6.824/lec_5_6/labrpc"
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

	expCh chan bool
	apdCh chan bool
	apyCh chan ApplyMsg

	votes int
	ot bool
}

func (rf *Raft) checkLeader() bool {
	oks := 1
	ok_ch := make(chan bool)

	fails := 0
	fail_ch := make(chan bool)
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(serverIdx int) {
			nx := rf.getNextIndex(serverIdx)
			px := nx - 1
			preLogTerm := 0
			if px > -1 && px < len(rf.logs) {
				preLog := rf.logs[px]
				preLogTerm = preLog.Term
			}

			args := &AppendEntriesArgs{
				Term:        rf.currentTerm,
				PreLogTerm:  preLogTerm,
				PreLogIndex: px,
				LeaderID:    rf.me,
				CommitIndex: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			ok := rf.peers[serverIdx].Call("Raft.AppendEntries", args, reply)

			if !ok || reply.Term > rf.currentTerm {
				if fails++; fails > len(rf.peers)/2 {
					fail_ch <- true
				}
			} else {
				if !reply.Success {
					rf.decrNextIndex(serverIdx, 1)
				}
				if oks++; oks > len(rf.peers)/2 {
					ok_ch <- true
				}
			}
		}(server)
	}

	select {
	case <-ok_ch:
		return true
	case <-fail_ch:
		return false
	}
}

func (rf *Raft) resetTimeOut() {
	if rf.ot {
		go func() {
			select {
			case rf.expCh <- true:
				break
			case <-time.After(time.Second*time.Duration(1)):
				break
			}
		}()
	}else{
		rf.startTimeOut()
	}
}

func (rf *Raft) stopTimeOut() {
	rf.ot = false
	go func() {
		select {
		case rf.expCh <- true:
			break
		case <-time.After(time.Second*time.Duration(1)):
			break
		}
	}()
}

func (rf *Raft) startTimeOut() {
	rf.ot = true
	go func() {
	timeout_loop:
		for rf.ot {
			rand_timeout := 100 + rand.Intn(200)
			select {
			case <-rf.expCh:
				break
			case <-time.After(time.Millisecond * time.Duration(rand_timeout)):
				fmt.Println(rf.me, "/", rf.currentTerm, rand_timeout, "开始竞选...", rf.logs)
				break timeout_loop
			}
		}
		rf.requestVotes()
	}()
}

func (rf *Raft) updateServerIndex(leaderCmtIdx int, size int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if leaderCmtIdx > size {
		rf.commitIndex = size - 1
	} else {
		rf.commitIndex = leaderCmtIdx
	}

	go func() {
		for rf.lastApplied < rf.commitIndex {
			toIdx := rf.lastApplied + 1
			logEntry := rf.logs[toIdx]
			rf.apyCh <- ApplyMsg{toIdx, logEntry.Command, false, nil}
			rf.lastApplied = toIdx
		}
	}()
}

func (rf *Raft) updateLeaderIndex(serverIdx int, size int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex[serverIdx] += size
	idx := rf.nextIndex[serverIdx] - 1
	rf.matchIndex[serverIdx] = idx

	for idx > -1 {
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
			rf.commitIndex = idx
			break
		}

		idx -= 1
	}

	go func() {
		for rf.lastApplied < rf.commitIndex {
			toIdx := rf.lastApplied + 1
			logEntry := rf.logs[toIdx]
			rf.apyCh <- ApplyMsg{toIdx, logEntry.Command, false, nil}
			rf.lastApplied = toIdx
		}
	}()
}

func (rf *Raft) addLogEntry(entry Entry) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs = append(rf.logs, entry)
	size := len(rf.logs)
	rf.nextIndex[rf.me] = size
	rf.matchIndex[rf.me] = size-1
	return size-1
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

func (rf *Raft) decrNextIndex(server int, deltaNX int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server] -= deltaNX
	if rf.nextIndex[server] < 0 {
		rf.nextIndex[server] = 0
	}
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

	rf.resetTimeOut()

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(serverIdx int) {
			voteReply := &RequestVoteReply{}
			fmt.Println(rf.me, rf.currentTerm, "请求", serverIdx, "投票")
			for {
				ok := rf.sendRequestVote(serverIdx, voteArgs, voteReply)
				if ok || rf.votedFor != rf.me {
					break
				}
			}

			if voteReply.Term > rf.currentTerm {
				rf.currentTerm = voteReply.Term
				rf.resetVotes()
				rf.votedFor = -1
				return
			}

			if voteReply.VoteGranted {
				fmt.Println(rf.me, rf.currentTerm, "获得1票：", serverIdx)
				currentVotes := rf.incrVotes()
				if currentVotes == len(rf.peers)/2+1 {
					rf.stopTimeOut()
					fmt.Println(rf.me, rf.currentTerm, "当选", rf.logs)
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
	var isleader bool
	// Your code here (2A).
	currentVotes := rf.getVotes()
	if currentVotes <= len(rf.peers)/2 {
		return rf.currentTerm, false
	}

	isleader = rf.checkLeader()

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
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, false
	}

	logEntry := Entry{term, command}

	index = rf.addLogEntry(logEntry)

	fmt.Println("Get Command:", rf.me, rf.currentTerm, index, command)

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
	rf.apyCh = applyCh

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for idx := 0; idx < len(rf.peers); idx++ {
		rf.nextIndex[idx] = 1
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.apdCh = make(chan bool)
	rf.expCh = make(chan bool)
	rf.startTimeOut()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
