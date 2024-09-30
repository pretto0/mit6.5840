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
	// "log"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"sort"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//按照figure2描述的那样，每一个raftserver需要维护的状态值
	currentTerm int
	votedFor int //在当前任期内投给了哪个candidate
	logs	[]LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//properties
	state NodeState
	electionTimer *time.Timer //timer for election timeout
	heartbeatTimer *time.Timer //timer for heartbeat

	// Volatile state on all servers
	// commitIndex :本raft服务器成功确认的最新一个日志操作的索引
	// lastApplied :本raft服务器应用成功的最新一个日志操作的索引
	commitIndex int // index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine(initialized to 0, increases monotonically)


	// Volatile state on all servers
	// Reinitialized after election
	// nextIndex 表示，对于索引号i的rf，本rf需要去发送的下一个log entry的索引号
	// matchIndex 表示，对于索引号i的rf，本rf与其通过复制log entry成功完成的最新的一个的log entry的索引号
	nextIndex []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	

	applyCond      *sync.Cond    // condition variable for apply goroutine
	replicatorCond []*sync.Cond  // condition variable for replicator goroutine	

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// 返回log中的最后一个LogEntry元素
func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() LogEntry {
	return rf.logs[0]
}

type NodeState uint8

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type LogEntry struct{
	Command interface{}
	Term int
	Index int
}

const ElectionTimeout = 1000
const HeartbeatTimeout = 125

type LockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *LockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var GlobalRand = &LockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),}

func RandomElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+GlobalRand.Intn(ElectionTimeout)) * time.Millisecond
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

func (rf *Raft) StartElection(){
	//先给自己投一票
	rf.votedFor = rf.me
	// rf.persist()
	args := rf.genRequestVoteArgs()
	//初始化计票变量，当它大于1/2的总数时赢得选举
	grantedVotes := 1
	// DPrintf("{Node %v} starts election with RequestVoteArgs %v", rf.me, args)

	for peer := range rf.peers{
		if peer == rf.me{
			continue
		}
		go func(peer int){
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer,args,reply){
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// DPrintf("{Node %v} receives RequestVoteReply %v from {Node %v} after sending RequestVoteArgs %v", rf.me, reply, peer, args)
				//因为是并发执行，判断的时候要确认当前的raft服务器的状态
				if args.Term == rf.currentTerm && rf.state == Candidate {
					if reply.VoteGranted{
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2{
							DPrintf("{Node %v} receives over half of the votes", rf.me)
							rf.ChangeState(Leader)
							rf.BroadcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term,-1 
					}
				
				}

			}

		}(peer)
	}

}

// func (rf *Raft) BroadcastHeartbeat(){
// 	args := &HeartBeatArgs{
// 		Term: rf.currentTerm,
// 	}

// 	for peer := range rf.peers{
// 		if peer == rf.me{
// 			continue
// 		}
// 		reply := new(HeartBeatReply)
// 		go rf.sendHeartBeat(peer,args,reply)
// 	}
// }

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool){
	for peer := range rf.peers{
		if peer == rf.me{
			continue
		}
		if isHeartBeat {
			go rf.replicateOnceRound(peer)
		} else {
			// just need to signal replicator to send log entries to peer
			rf.replicatorCond[peer].Signal()
		}
	}
}



func (rf *Raft) ChangeState(state NodeState){
	if rf.state == state{
		return
	}
	fmt.Printf("{Node %v} changes state from %v to %v\n", rf.me, rf.state, state)
	rf.state = state
	switch state{
	case Follower:
		rf.electionTimer.Reset(RandomElectionTimeout())
		rf.heartbeatTimer.Stop()
	case Candidate:
	case Leader:
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		// LastLogIndex: rf.getLastLog().Index,
		// LastLogTerm:  rf.getLastLog().Term,
	}
	return args
}

func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
	firstLogIndex := rf.getFirstLog().Index
	entries := make([]LogEntry, len(rf.logs[prevLogIndex-firstLogIndex+1:]))
	copy(entries, rf.logs[prevLogIndex-firstLogIndex+1:])
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex-firstLogIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	return args
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term int
	CandidateId int
	// LastLogIndex int
	// LastLogTerm int
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

type HeartBeatArgs struct{
	Term int
}

type HeartBeatReply struct{

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}


func (rf *Raft) isLogUpToDate(index, term int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

func (rf *Raft) isLogMatched(index, term int) bool {
	return index <= rf.getLastLog().Index && term == rf.logs[index-rf.getFirstLog().Index].Term
}


// example RequestVote RPC handler.
// 这段代码是站在raft接收服务器的视角下实现的
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer fmt.Printf("{Node %v}'s state is {state %v, term %v}} after processing RequestVote,  RequestVoteArgs %v and RequestVoteReply %v \n", rf.me, rf.state, rf.currentTerm, args, reply)
	//参选者的任期小于自身任期，或者在同一任期中，此server已经投过票了
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor!=-1 && rf.votedFor != args.CandidateId){
		reply.Term, reply.VoteGranted = rf.currentTerm,false
		return
	}

	if args.Term > rf.currentTerm{
		rf.ChangeState(Follower)
		rf.currentTerm,rf.votedFor = args.Term,-1
		// rf.persist()
	}

	// if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
	// 	reply.Term, reply.VoteGranted = rf.currentTerm, false
	// 	return
	// }

	rf.votedFor = args.CandidateId
	// rf.persist()
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term = rf.currentTerm
	reply.VoteGranted = true

}

func (rf *Raft) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
	
	} else {
		rf.electionTimer.Reset(RandomElectionTimeout())
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing AppendEntries,  AppendEntriesArgs %v and AppendEntriesReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)
	// 不接受之前任期的leader的RPC请求
	if args.Term < rf.currentTerm{
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		//改变追随者
		rf.currentTerm , rf.votedFor = args.Term, -1
		// rf.persist()
	}

	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomElectionTimeout())

	// 已经同步完毕
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if !rf.isLogMatched(args.PrevLogIndex,args.PrevLogTerm) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastLogIndex := rf.getLastLog().Index
		
		if lastLogIndex < args.PrevLogIndex{
			reply.ConflictIndex, reply.ConflictTerm = lastLogIndex+1, -1
		} else {
			firstLogIndex := rf.getFirstLog().Index
			// find the first index of the conflicting term
			index := args.PrevLogIndex
			for index >= firstLogIndex && rf.logs[index-firstLogIndex].Term == args.PrevLogTerm {
				index--
			}
			reply.ConflictIndex, reply.ConflictTerm = index+1, args.PrevLogTerm
		}
		return

	}

	firstLogIndex := rf.getFirstLog().Index
	for index,entry := range args.Entries{
		if entry.Index - firstLogIndex >= len(rf.logs) || rf.logs[entry.Index-firstLogIndex].Term != entry.Term {
			rf.logs = shrinkEntries(append(rf.logs[:entry.Index-firstLogIndex], args.Entries[index:]...))
			// rf.persist()
			break
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) (paper)
	newCommitIndex := Min(args.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %v} advances commitIndex from %v to %v with leaderCommit %v in term %v", rf.me, rf.commitIndex, newCommitIndex, args.LeaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
	reply.Term, reply.Success = rf.currentTerm, true

}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func shrinkEntries(entries []LogEntry) []LogEntry {
	const lenMultiple = 2
	if cap(entries) > len(entries)*lenMultiple {
		newEntries := make([]LogEntry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args *HeartBeatArgs, reply *HeartBeatReply) bool {
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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

//start是rf和客户端交互的接口函数，这个函数表示rf在收到一个command命令后的保存和广播的操作。
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//不是leader不接收command
	if rf.state != Leader{
		return -1, -1, false
	}
	newLogIndex := rf.getLastLog().Index + 1
	rf.logs = append(rf.logs, LogEntry{
		Term: rf.currentTerm,
		Command: command,
		Index: newLogIndex,
	})
	
	// rf.persist()
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLogIndex, newLogIndex+1
	fmt.Printf("{Node %v} starts agreement on a new log entry with command %v in term %v\n", rf.me, command, rf.currentTerm)

	// TODO 向其他rf发送同步log的广播
	rf.BroadcastHeartbeat(false)

	return newLogIndex, rf.currentTerm, true
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// check the logs of peer is behind the leader
	// 检查自己还是不是Leader，检查需不需要发送同步请求。
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}


func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			//等待后续的Signal()来唤醒它
			rf.replicatorCond[peer].Wait()
		}
		// send log entries to peer
		rf.replicateOnceRound(peer)
	}
}


//这个函数到了快照lab3C时还要调整
func (rf *Raft) replicateOnceRound(peer int){
	rf.mu.RLock()
	if rf.state != Leader{
		rf.mu.RUnlock()
		return
	}
	//与peer的rf机先前同步的最后一个索引号
	prevLogIndex := rf.nextIndex[peer] - 1

	args := rf.genAppendEntriesArgs(prevLogIndex)
	rf.mu.RUnlock()
	reply := new(AppendEntriesReply)
	if rf.sendAppendEntries(peer, args, reply) {
		//确认向peer发送RPC成功后，开始获取写锁, 根据返回结果进行操作
		rf.mu.Lock()
		if args.Term == rf.currentTerm && rf.state == Leader{
			if !reply.Success {
				if reply.Term > rf.currentTerm {
					//新的任期开始了
					rf.ChangeState(Follower)
					rf.currentTerm, rf.votedFor = reply.Term, -1
					// rf.persist()
				} else if reply.Term == rf.currentTerm {
					rf.nextIndex[peer] = reply.ConflictIndex
					if reply.ConflictTerm != -1{
						firstLogIndex := rf.getFirstLog().Index
						//逐渐向前寻找Index
						for index := args.PrevLogIndex - 1; index >= firstLogIndex; index-- {
							if rf.logs[index - firstLogIndex].Term == reply.ConflictTerm {
								rf.nextIndex[peer] = index
								break
							}
						}
					} 
				}
			} else {
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				// advance commitIndex if possible
				rf.advanceCommitIndexForLeader()
			}
		}
		rf.mu.Unlock()
		DPrintf("{Node %v} sends AppendEntriesArgs %v to {Node %v} and receives AppendEntriesReply %v", rf.me, args, peer, reply)
	}
}

func (rf *Raft) advanceCommitIndexForLeader() {
	n := len(rf.matchIndex)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sort.Ints(sortMatchIndex)
	// get the index of the log entry with the highest index that is known to be replicated on a majority of servers
	newCommitIndex := sortMatchIndex[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		if rf.isLogMatched(newCommitIndex, rf.currentTerm) {
			fmt.Printf("{Node %v} advances commitIndex from %v to %v in term %v\n", rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}
	}
}


// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed(){
		select{
		case <- rf.electionTimer.C:
			// fmt.Printf("raft sever %v begin elect\n", rf.me)
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm += 1

			rf.StartElection()
			rf.electionTimer.Reset(RandomElectionTimeout())

			rf.mu.Unlock()
		case <- rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader{
				fmt.Printf("{node %v} broadcast heartbeat\n", rf.me)
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}

		// Your code here (3A)
		// Check if a leader election should be started.


		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers: peers,
		persister: persister,
		me: me,
		dead: 0,

		
		currentTerm: 0,
		votedFor: -1,
		logs:  make([]LogEntry, 1), // dummy entry at index 0

		state: Follower,
		electionTimer: time.NewTimer(RandomElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),

		commitIndex:    0,
		lastApplied:    0,

		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		
		replicatorCond: make([]*sync.Cond, len(peers)),
	}

	fmt.Printf("Node %v created!\n", rf.me)
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// should use mu to protect applyCond, avoid other goroutine to change the critical section
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize nextIndex and matchIndex, and start replicator goroutine
	for peer := range peers {
		rf.matchIndex[peer], rf.nextIndex[peer] = 0, rf.getLastLog().Index+1
		if peer != rf.me {
			rf.replicatorCond[peer] = sync.NewCond(&sync.Mutex{})

			fmt.Printf("Node %v start a go route for %v \n", rf.me, peer)

			// start replicator goroutine to send log entries to peer
			// 开始一个协程，这个协程会定期向peer发送日志同步操作的请求
			go rf.replicator(peer)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
