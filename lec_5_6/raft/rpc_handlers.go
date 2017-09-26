package raft

import (
	"time"
	"fmt"
)

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

	rf.resetTimeOut()

	if args.LeaderID != rf.me {
		rf.toFollower(args.LeaderID)
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term
	preLogIdx := args.PreLogIndex

	if preLogIdx >= len(rf.logs) {
		reply.Success = false
		return
	}

	//前缀一致性检查
	if preLogIdx > -1 && preLogIdx < len(rf.logs) {
		preLogEntry := rf.logs[args.PreLogIndex]
		if preLogEntry.Term != args.PreLogTerm {
			reply.Success = false
			return
		}
	}

	t := false
	if preLogIdx == len(rf.logs)-1 {
		for _, item := range (args.Items) {
			t = true
			rf.logs = append(rf.logs, item)
		}
	} else {
		rf.logs = rf.logs[:args.PreLogIndex+1]
		for _, item := range (args.Items) {
			t = true
			rf.logs = append(rf.logs, item)
		}
	}
	reply.Success = true

	if t {
		fmt.Println(rf.me, rf.currentTerm, "NEW", args.Items)
		fmt.Println(rf.me, rf.currentTerm, "NOW", rf.logs)
	}

	rf.updateServerIndex(args.CommitIndex, len(rf.logs))
}

func (rf *Raft) sendAppendEntries() {

	isLeader := true

	for server, _ := range rf.peers {

		if server == rf.me {
			continue
		}

		go func(serverIdx int) {
			preLogTerm := 0
			for isLeader {
				nx := rf.getNextIndex(serverIdx)
				px := nx - 1
				toEntries := []Entry{}
				if len(rf.logs) > nx {
					toEntries = rf.logs[nx:]
				}

				if px > -1 && px < len(rf.logs) {
					preLog := rf.logs[px]
					preLogTerm = preLog.Term
				}

				args := &AppendEntriesArgs{
					rf.currentTerm,
					toEntries,
					preLogTerm,
					px,
					rf.me,
					rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				for {
					ok := rf.peers[serverIdx].Call("Raft.AppendEntries", args, reply)
					if ok {
						break
					}
					time.Sleep(time.Millisecond * 10)
				}

				if reply.Success {
					rf.updateLeaderIndex(serverIdx, len(toEntries))
				} else {
					if rf.currentTerm < reply.Term {
						isLeader = false
						rf.resetVotes()
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						return
					}
					if rf.getVotes() <= len(rf.peers)/2 {
						continue
					}
					rf.decrNextIndex(serverIdx, 1)
				}
				rf.heartbeats[serverIdx] = 1
				time.Sleep(10 * time.Millisecond)
			}
		}(server)
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
		fmt.Println(rf.me,"拒绝为", args.CandidateIdx, "投票(1)")
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//在当前term内，已经投票后，就不能再给其他节点投票了
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateIdx {
		fmt.Println(rf.me,"拒绝为", args.CandidateIdx, "投票(2)", rf.votedFor)
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
		rf.resetTimeOut()
		reply.VoteGranted = true
		rf.votedFor = args.CandidateIdx
		fmt.Println(rf.me,"为", args.CandidateIdx, "投票")
		return
	}

	if args.LastLogTerm < lastLogTerm || args.LastLogIndex < lastLogIndex {
		reply.VoteGranted = false
		return
	}

	fmt.Println(rf.me,"为", args.CandidateIdx, "投票")
	rf.resetTimeOut()
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
