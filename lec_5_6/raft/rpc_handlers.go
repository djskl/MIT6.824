package raft

import (
	"time"
	"sync/atomic"
)

type AppendEntriesArgs struct {
	Term        int32
	Items       []Entry
	PreLogTerm  int32
	PreLogIndex int
	LeaderID    int
	CommitIndex int32
}

type AppendEntriesReply struct {
	Term      int32
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.ResetTimeOut()

	if args.LeaderID != rf.me {
		rf.TurnToFollower(args.LeaderID, args.Term)
	} else {
		atomic.StoreInt32(&rf.currentTerm, args.Term) //rf.currentTerm = args.Term
	}

	reply.Term = args.Term
	preLogIdx := args.PreLogIndex

	if preLogIdx >= len(rf.logs) {
		reply.NextIndex = len(rf.logs)
		reply.Success = false
		return
	}

	//前缀一致性检查
	err, preLogEntry := rf.getLogEntry(args.PreLogIndex) //rf.logs[args.PreLogIndex]
	if err == nil && preLogEntry.Term != args.PreLogTerm {
		first_index := rf.getFirstIndex(args.PreLogIndex)
		reply.NextIndex = first_index
		rf.delLogEntries(first_index)
		reply.Success = false
		return
	}

	if preLogIdx == len(rf.logs)-1 {
		rf.addLogEntries(args.Items)
	} else {
		rf.delLogEntries(args.PreLogIndex + 1)
		rf.addLogEntries(args.Items)
	}

	reply.Success = true

	if len(args.Items) > 0 {
		DPrintln(rf.me, rf.currentTerm, "NEW", args.Items)
		DPrintln(rf.me, rf.currentTerm, "NOW", rf.logs)
	}

	rf.updateServerIndex(args.CommitIndex)
}

func (rf *Raft) sendAppendEntries() {

	isLeader := true

	for server, _ := range rf.peers {

		if server == rf.me {
			continue
		}

		go func(serverIdx int) {
			var preLogTerm int32
			for isLeader {
				nx := rf.nextIndex[serverIdx]
				px := nx - 1
				toEntries := []Entry{}
				if len(rf.logs) > nx {
					toEntries = rf.getLogEntries(nx) //toEntries = rf.logs[nx:]
				}

				/*if px > -1 && px < len(rf.logs) {
					preLog := rf.logs[px]
					preLogTerm = preLog.Term
				}*/
				err, preLog := rf.getLogEntry(px) //rf.logs[px]
				if err == nil {
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
				for isLeader {
					ok := rf.peers[serverIdx].Call("Raft.AppendEntries", args, reply)

					if args.Term != rf.currentTerm {
						DPrintln(rf.me, rf.currentTerm, "收到过期AppendEntries响应", serverIdx, args.Term)
						break
					}

					if ok || !isLeader {
						break
					}
				}

				if args.Term != rf.currentTerm {
					if int(rf.votes) <= len(rf.peers)/2 {
						isLeader = false
					}
					continue
				}

				if reply.Success {
					rf.updateLeaderIndex(serverIdx, len(toEntries))
				} else {
					if rf.currentTerm < reply.Term {
						isLeader = false
						rf.TurnToFollower(-1, reply.Term)
						return
					}
					if int(rf.votes) <= len(rf.peers)/2 {
						isLeader = false
						continue
					}
					rf.nextIndex[serverIdx] = reply.NextIndex //rf.nextIndex[serverIdx] = rf.nextIndex[serverIdx] - 1
				}

				rf.heartbeats[serverIdx] = 1

				select {
				case logIdx := <-rf.logChs[serverIdx]:
					if logIdx < rf.matchIndex[serverIdx] {
						time.Sleep(HEARTBEAT_TIMEOUT)
					}
					break
				case <-time.After(HEARTBEAT_TIMEOUT):
					break
				}
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
	Term         int32
	CandidateIdx int
	LastLogIndex int
	LastLogTerm  int32
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		DPrintln(rf.me, "(", rf.currentTerm, ")", "拒绝为", args.CandidateIdx, "(", args.Term, ")投票(1)")
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//在当前term内，已经投票后，就不能再给其他节点投票了
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateIdx {
		DPrintln(rf.me, "拒绝为", args.CandidateIdx, "投票(2)", rf.votedFor)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	lastLogIndex := len(rf.logs) - 1
	var lastLogTerm int32
	if lastLogIndex > -1 {
		err, logEntry := rf.getLogEntry(lastLogIndex)
		if err == nil {
			lastLogTerm = logEntry.Term //lastLogTerm = rf.logs[lastLogIndex].Term
		}
	}

	if args.LastLogTerm > lastLogTerm {
		rf.ResetTimeOut()
		reply.VoteGranted = true
		rf.votedFor = args.CandidateIdx
		DPrintln(rf.me, "为", args.CandidateIdx, "投票")
		return
	}

	if args.LastLogTerm < lastLogTerm || args.LastLogIndex < lastLogIndex {
		reply.VoteGranted = false
		return
	}

	DPrintln(rf.me, "为", args.CandidateIdx, "投票")
	rf.ResetTimeOut()
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
