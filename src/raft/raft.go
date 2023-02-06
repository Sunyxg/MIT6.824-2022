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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	LEADER    = 1
	FOLLOWER  = 2
	CANDIDATE = 3

	HEARTBEATTIME     = 100
	ELECTIONTIMEBASE  = 500
	ELECTIONTIMERANGE = 800
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
// 当每个Raft peer意识到连续的日志被提交时，peer通过传递给Make()的applyCh向同一服务器
// 上的服务者发送一个ApplyMsg，CommandValid为true表明ApplyMsg包含一个新提交的日志条目
type ApplyMsg struct {
	CommandValid bool        // 为true表明包含一个新提交的日志条目
	Command      interface{} // 当前命令的内容
	CommandIndex int         // 当前命令在日志中的索引

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
// 一个实现单个Raft对等体的Go对象
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // 当前服务器在peers[]中的索引
	dead      int32               // 当前服务器是否已调用kill()函数

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// 持久化存储
	CurrentTerm int // 当前Raft任期
	VotedFor    int // 当前Raft给哪个服务器投了票
	Logs        Log // 日志序列

	LastIncludedIndex int // 最后一条被快照取代的日志的索引
	LastIncludedTerm  int // 最后一条被快照取代的日志的任期

	// 非持久化存储
	State        int           // 1：leader 2:follower 3:candidate
	HeartBeats   time.Duration // 心跳检测间隔时间
	ElectionTime time.Time     // 选举过期时间

	CommitIndex int // 即将被提交的日志索引(已被大多数server接收)
	LastApplied int // 已被提交到状态机的日志索引

	// Leader独有
	NextIndex  []int // 每个节点下一次发送日志条目索引
	MatchIndex []int // 每个节点最后成功复制的日志条目索引

}

// return currentTerm and whether this server
// believes it is the leader.
// 返回当前Term以及当前服务器是否认为自己是leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.CurrentTerm
	isleader := (rf.State == LEADER)
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) persistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	data := w.Bytes()
	return data
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 将Raft的持久化状态保存在稳定的存储设备中
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.persistState())
	//DPrintf("[%d] call Persist(),rf.CurrentTerm = %d,rf.VotedFor = %d,rf.Log len = %d", rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Logs.Entries))
}

// restore previously persisted state.
// 恢复之前保存的持久性存储状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curterm int
	var votedfor int
	var logs Log
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&curterm) != nil || d.Decode(&votedfor) != nil || d.Decode(&logs) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		panic("readPersist error!!")
	} else {
		rf.CurrentTerm = curterm
		rf.VotedFor = votedfor
		rf.Logs = logs
		rf.LastIncludedIndex = lastIncludedIndex
		rf.LastIncludedTerm = lastIncludedTerm
		//DPrintf("[%d] call readPersist(),rf.CurrentTerm = %d,rf.VotedFor = %d,rf.Log len = %d", rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Logs.Entries))
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 应用服务通知Raft服务器它已经创建了快照snapshot
// 该快照包含了到索引index为止的所有信息
// 此时Raft不再需要包含index为止的所有日志信息
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果下标大于自身的提交，说明没被提交不能安装快照，如果自身快照点大于index说明不需要安装
	if index <= rf.LastIncludedIndex || index > rf.CommitIndex {
		return
	}

	// 更新快照日志
	lastlog := rf.logat(index)
	rf.dellogs(1, index)
	rf.LastIncludedIndex = lastlog.Index
	rf.LastIncludedTerm = lastlog.Term
	DPrintf("[%v] : Snapshot  index[%v]  rf.Log[%v] rf.commit[%v]", rf.me, index, rf.Logs, rf.CommitIndex)
	rf.persister.SaveStateAndSnapshot(rf.persistState(), snapshot)

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 自己当前的任期号
	Candidateid  int // 自己的ID
	LastlogIndex int // 自己最后一个日志的日志号
	LastlogTerm  int // 自己最后一个日志的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 自己当前的任期号
	VoteGranted bool // 自己是否投票给这个candidate
}

// 追加日志RPC Request
type AppendEntriesArgs struct {
	Term         int        // leader当前的任期号
	LeaderId     int        // leader的ID
	PrevLogIndex int        // 前一个日志的日志号
	PrevLogTerm  int        // 前一个日志的索引号
	LeaderCommit int        // leader已提交的日志号
	Entries      []LogEntry // Append的日志体
}

// 追加日志RPC Reply
type AppendEntriesReply struct {
	Term    int  // follower当前的任期号
	Success bool // follower是否匹配PrevLog
	XTerm   int  // follower中与Leader冲突的任期号
	XIndex  int  // follower中任期号为XTerm第一条Log的日志号
	XLen    int  // follower中空白的Log槽位数
}

// 更新快照RPC Request
type InstallSnapshotArgs struct {
	Term             int    // leader当前的任期号
	LeaderId         int    // leader的id
	LastIncludeIndex int    // 快照替换的最后一条日志的索引
	LastIncludeterm  int    // 快照替换的最后一条日志的任期
	Snapshot         []byte // 快照
}

// 更新快照RPC Reply
type InstallSnapshotReply struct {
	Term int // follower当前的任期号
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	// 接收到的任期号大于当前自己的任期号
	if args.Term > rf.CurrentTerm {
		rf.setNewTerm(args.Term)
	}

	reply.Term = rf.CurrentTerm
	// 接收到的任期号小于当前自己的任期号
	if args.Term < rf.CurrentTerm {
		// 拒绝当前请求，并返回任期号
		return
	}

	// 请求投票者的日志是否够新
	lastlog := rf.logat(rf.Logs.NextIndex - 1)
	if lastlog.Term > args.LastlogTerm {
		return
	}
	if lastlog.Term == args.LastlogTerm && lastlog.Index > args.LastlogIndex {
		return
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.Candidateid {
		rf.resetElectionTime()
		reply.VoteGranted = true
		rf.VotedFor = args.Candidateid
		rf.persist()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[%d] Term %d : recived log[%v]", rf.me, rf.CurrentTerm, args.Entries)

	reply.Term = rf.CurrentTerm
	reply.Success = false

	// 接收到的任期号小于当前自己的任期号
	if args.Term < rf.CurrentTerm {
		// 拒绝当前请求，并返回任期号
		return
	}

	// 接收到的任期号大于自己当前任期号
	if args.Term > rf.CurrentTerm {
		rf.setNewTerm(args.Term)
		reply.Term = rf.CurrentTerm
	}
	rf.resetElectionTime()

	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.XLen = -1
		reply.XIndex = rf.LastIncludedIndex
		return
	}

	// 前一个日志信息匹配情况
	// 如果不存在索引为PreLogIndex的日志，直接返回false
	if args.PrevLogIndex >= rf.Logs.NextIndex {
		// log快速恢复
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - rf.Logs.NextIndex + 1
		return
	}

	// 如果PreLogIndex位置日志任期不同
	if args.PrevLogTerm != rf.logat(args.PrevLogIndex).Term {
		// 删除PreLogIndex位置及以后的日志
		reply.XTerm = rf.logat(args.PrevLogIndex).Term
		for idx := args.PrevLogIndex; idx > rf.LastIncludedIndex; idx-- {
			if rf.logat(idx).Term == reply.XTerm {
				reply.XIndex = idx
			} else {
				break
			}
		}
		rf.dellogs(0, args.PrevLogIndex)
		rf.persist()
		return
	}

	// 插入Log条目
	for idx, entry := range args.Entries {
		if entry.Index < rf.Logs.NextIndex && entry.Term != rf.logat(entry.Index).Term {
			rf.dellogs(0, entry.Index)
			rf.persist()
		}

		if entry.Index >= rf.Logs.NextIndex {
			rf.Logs.append(args.Entries[idx:]...)
			rf.persist()
			DPrintf("[%d] Term %d : follower append : %v", rf.me, rf.CurrentTerm, args.Entries[idx:])
			break
		}
	}

	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(rf.Logs.NextIndex-1, args.LeaderCommit)
		rf.apply()
		DPrintf("[%d] Term %d : follower update Commindex to : %v", rf.me, rf.CurrentTerm, rf.CommitIndex)
	}
	reply.Success = true

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm

	// 接收到的任期号小于当前自己的任期号
	if args.Term < rf.CurrentTerm {
		// 拒绝当前请求，并返回任期号
		return
	}

	// 接收到的任期号大于自己当前任期号
	if args.Term > rf.CurrentTerm {
		rf.setNewTerm(args.Term)
		reply.Term = rf.CurrentTerm
	}
	rf.resetElectionTime()

	if args.LastIncludeIndex < rf.LastIncludedIndex || args.LastIncludeIndex <= rf.CommitIndex {
		return
	}

	// 保存快照并删除快照包含的日志
	// 不能直接在日志切片中进行删除，因为日志可能未达到LastIncludeIndex的位置
	if args.LastIncludeIndex < rf.Logs.NextIndex {
		rf.dellogs(1, args.LastIncludeIndex)
	} else {
		rf.Logs = Log{
			Entries:   make([]LogEntry, 0),
			NextIndex: args.LastIncludeIndex + 1,
		}
	}
	rf.LastIncludedIndex = args.LastIncludeIndex
	rf.LastIncludedTerm = args.LastIncludeterm

	rf.persister.SaveStateAndSnapshot(rf.persistState(), args.Snapshot)

	DPrintf("[%d] Term %d : recived snapshot[term : %v, index : %v]", rf.me, rf.CurrentTerm, args.LastIncludeterm, args.LastIncludeIndex)
	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: args.Snapshot, SnapshotTerm: args.LastIncludeterm, SnapshotIndex: args.LastIncludeIndex}
	rf.mu.Lock()
	DPrintf("[%d] Term %d : applyed snapshot[term : %v, index : %v]", rf.me, rf.CurrentTerm, args.LastIncludeterm, args.LastIncludeIndex)

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
// labrpc()软件包模拟了一个有损失的网络，其中服务器可能无法到达，且请求和回复可能会丢失。
// Call()发送一个请求并等待一个回复，如果回复在超时时间内到达，返回为真，否则返回假。
// Call()被保证会返回(也许是在延迟之后)，除非服务器端的处理函数没有返回。
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
// 向服务器server发送RequestVote RPC
// 其中server为peers[]中目标服务器的索引值
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
// 将命令追加到Raft日志副本中，如果当前服务器不是Leader返回false
// Start()应立即返回，无需等待日志追加完成，且不保证该命令会被提交到Raft日志中
// 返回这个请求会存放在Log中的索引，Leader的任期号，以及当前sever是否是Leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果当前sever不是Leader直接返回
	if rf.State != LEADER || rf.killed() {
		return -1, -1, false
	}

	index := rf.Logs.NextIndex
	term := rf.CurrentTerm
	alog := LogEntry{
		Command: command,
		Index:   index,
		Term:    term,
	}
	rf.Logs.append(alog)
	rf.persist()
	rf.sendEntries(false)

	DPrintf("[%d] Called Start : %v\n", rf.me, alog)
	return index, term, true
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
// 每次测试结束，不会停止Raft创建的goroutines,但是会调用Kill()方法
// 代码中使用Killed()函数检测Kill()方法是否被调用
// 任何具有长期运行循环的gorountine应该调用Killed()来检测它是否应该停止
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// Ticker goroutines会开始一轮新的选举，如果它最近没有收到心跳检测
func (rf *Raft) ticker() {
	// 若当前服务器处于存活状态
	for rf.killed() == false {
		// 以心跳间隔进行sleep()
		rf.mu.Lock()
		// 判断Raft服务器的状态
		if rf.State == LEADER {
			// 发送心跳检测RPC
			rf.sendEntries(true)
		} else {
			if time.Now().After(rf.ElectionTime) {
				rf.Election()
			}
		}
		rf.mu.Unlock()
		time.Sleep(rf.HeartBeats)
	}
}

// Raft更新自己的任期号
func (rf *Raft) setNewTerm(newTerm int) {
	rf.State = FOLLOWER
	rf.CurrentTerm = newTerm
	rf.VotedFor = -1
	rf.persist()
	DPrintf("[%d]: set new term to : %v", rf.me, rf.CurrentTerm)
}

// Raft领导者选取
func (rf *Raft) Election() {
	rf.State = CANDIDATE
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	rf.resetElectionTime()
	DPrintf("[%v]: start leader election, term %d\n", rf.me, rf.CurrentTerm)
	voteCount := 1
	lastlog := rf.logat(rf.Logs.NextIndex - 1)
	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		Candidateid:  rf.me,
		LastlogIndex: lastlog.Index,
		LastlogTerm:  lastlog.Term,
	}

	for peer, _ := range rf.peers {
		if peer != rf.me {
			go func(server int, term int) {
				reply := RequestVoteReply{}
				// 向索引为peer的服务器发送投票请求
				rf.sendRequestVote(server, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if term != rf.CurrentTerm || reply.Term < term {
					return
				}

				if reply.Term > rf.CurrentTerm {
					rf.setNewTerm(reply.Term)
					return
				}

				if reply.VoteGranted && rf.State == CANDIDATE {
					voteCount++
					// 判断得票数是否过半
					if voteCount > len(rf.peers)/2 && rf.State == CANDIDATE {
						rf.State = LEADER
						DPrintf("[%v]: leader election success, term %d\n", rf.me, rf.CurrentTerm)
						rf.sendEntries(true)
					}
				}

			}(peer, rf.CurrentTerm)
		}
	}

}

// 重置选举过期时间
func (rf *Raft) resetElectionTime() {
	t := time.Now()
	rand.Seed(t.UnixNano())
	etime := time.Duration(ELECTIONTIMEBASE+rand.Intn(ELECTIONTIMERANGE)) * time.Millisecond
	rf.ElectionTime = t.Add(etime)
}

// 向其他所有服务器追加日志或快照
func (rf *Raft) sendEntries(heatbeat bool) {
	// rf.updataCommit()

	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.NextIndex[peer] <= 0 {
			rf.NextIndex[peer] = 1
		}

		// 如果为心跳检测或者当前Leader有待发送日志
		if rf.NextIndex[peer] < rf.Logs.NextIndex || heatbeat {
			// 如果peer对应的NextIndex在leader的日志范围内则只需要发送日志
			if rf.NextIndex[peer] > rf.LastIncludedIndex {
				args := AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.CommitIndex,
				}
				args.Entries = rf.endlogs(rf.NextIndex[peer])
				args.PrevLogIndex = rf.NextIndex[peer] - 1
				args.PrevLogTerm = rf.logat(args.PrevLogIndex).Term
				go rf.sendEntriesto(peer, &args)

			} else {
				// 如果peer对应的NextIndex处的日志已经因为快照而被删除，则同时发送快照和日志
				args := InstallSnapshotArgs{
					Term:             rf.CurrentTerm,
					LeaderId:         rf.me,
					LastIncludeIndex: rf.LastIncludedIndex,
					LastIncludeterm:  rf.LastIncludedTerm,
					Snapshot:         rf.persister.snapshot,
				}
				go rf.sendSnapshotto(peer, &args)

			}

		}
	}
}

func (rf *Raft) sendSnapshotto(server int, args *InstallSnapshotArgs) {
	DPrintf("[%d] Term %d : send snapshot to [%d], last log[term : %v, index : %v]", args.LeaderId, args.Term, server, args.LastIncludeterm, args.LastIncludeIndex)
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term < rf.CurrentTerm {
		return
	}
	if reply.Term > rf.CurrentTerm {
		rf.setNewTerm(reply.Term)
		return
	}

	if rf.CurrentTerm == args.Term {
		rf.NextIndex[server] = args.LastIncludeIndex + 1
		rf.MatchIndex[server] = args.LastIncludeIndex
		DPrintf("[%d] Term %d : [%d] recive snapshot success last log  [term : %v, index : %v]", args.LeaderId, args.Term, server, args.LastIncludeterm, args.LastIncludeIndex)
	}
	rf.updataCommit()
}

func (rf *Raft) sendEntriesto(server int, args *AppendEntriesArgs) {
	rf.mu.Lock()
	DPrintf("[%d] Term %d : send log to [%d], last log[%v]", args.LeaderId, args.Term, server, args.Entries)
	reply := AppendEntriesReply{}
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term < rf.CurrentTerm {
		return
	}
	if reply.Term > rf.CurrentTerm {
		rf.setNewTerm(reply.Term)
		return
	}

	if rf.CurrentTerm == args.Term {
		if reply.Success {
			rf.NextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
			// DPrintf("[%d] Term %d : [%d] recive log success %d", rf.me, rf.CurrentTerm, server, rf.Logs)
		} else {
			if reply.XLen == -1 {
				rf.NextIndex[server] = reply.XIndex + 1
				rf.MatchIndex[server] = reply.XIndex
			} else if reply.XTerm == -1 {
				rf.NextIndex[server] = rf.NextIndex[server] - reply.XLen
				DPrintf("[%d] Term %d : [%d] recive log %v failed, NextIndex: %v", rf.me, rf.CurrentTerm, server, rf.logat(args.PrevLogIndex+len(args.Entries)), rf.NextIndex[server])
			} else {
				for idx := args.PrevLogIndex; idx >= reply.XIndex; idx-- {
					if rf.logat(idx).Term == reply.XTerm {
						break
					}
					rf.NextIndex[server] = idx
				}
				DPrintf("[%d] Term %d : [%d] recive log %v failed, NextIndex: %v", rf.me, rf.CurrentTerm, server, rf.logat(args.PrevLogIndex+len(args.Entries)), rf.NextIndex[server])
			}
			// DPrintf("[%d] Term %d : [%d] recive log %v failed, NextIndex: %v", rf.me, rf.CurrentTerm, server, rf.logat(args.PrevLogIndex+len(args.Entries)), rf.NextIndex[server])
		}
	}
	rf.updataCommit()
}

// 更新CommitIndex
func (rf *Raft) updataCommit() {
	rf.CommitIndex = max(rf.LastIncludedIndex, rf.CommitIndex)
	for idx := rf.CommitIndex + 1; idx < rf.Logs.NextIndex; idx++ {
		// Leader只能对当前任期内的log进行提交，对非当前任期的日志，采用捎带提交的方式
		if rf.logat(idx).Term != rf.CurrentTerm {
			continue
		}
		count := 1
		for peer, _ := range rf.MatchIndex {
			if rf.MatchIndex[peer] >= idx {
				count++
			}

			if count > len(rf.peers)/2 {
				rf.CommitIndex = idx
				rf.apply()
				DPrintf("[%d]: updata CommitIndex to : %v", rf.me, rf.CommitIndex)
				break
			}
		}
	}
}

// 通过applyChannl向应用服务中发送applyMsg
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.CommitIndex > rf.LastApplied {
			if rf.LastApplied >= rf.LastIncludedIndex {
				rf.LastApplied++
				entry := rf.logat(rf.LastApplied)
				applymsg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				// DPrintf("[%d]: apply log[%d]", rf.me, rf.LastApplied)
				rf.mu.Unlock()
				rf.applyCh <- applymsg
				rf.mu.Lock()
				// DPrintf("[%d]: applied log[%d] success", rf.me, rf.LastApplied)
			} else {
				rf.LastApplied = rf.LastIncludedIndex
			}
		} else {
			// DPrintf("[%v]: rf.applyCond.Wait()", rf.me)
			rf.applyCond.Wait()
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
	// DPrintf("[%d]: Broadcast\n", rf.me)
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
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
// Make()用于创建一个Raft peer
// 所有Raft服务器的端口都在peer[]中，peer[me]为当前服务器端口
// Make()必须快速返回，所以它调用goroutines处理任何长期运行的工作
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.State = FOLLOWER
	rf.HeartBeats = HEARTBEATTIME * time.Millisecond
	rf.resetElectionTime()
	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = -1

	rf.Logs = emptyLog()
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.MatchIndex = make([]int, len(peers))
	rf.NextIndex = make([]int, len(peers))

	DPrintf("[%d] is Making , len(peers) = %d\n", me, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.CommitIndex = rf.LastIncludedIndex
	rf.LastApplied = rf.LastIncludedIndex
	// DPrintf("[%d] is Making , LastIncludeindex = %v logs = [%v]\n", me, rf.LastIncludedIndex, rf.Logs)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
