package raft

import (
	"math"
	"time"
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

	if args.LeaderID != rf.me {
		rf.resetVotes()
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
					rf.updateIndex(server, len(toEntries))
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
