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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

type NodeState int

const (
	StateFollower NodeState = iota //follower状态

	StateCandidate //候选者

	StateLeader //领导者
)

type Log struct {
	Command interface{} //使用interface{}以便接受任何类型的指令
	Term    int         //记录日志的任期，便于检测冲突
}

// A Go object implementing a single Raft peer.
type Raft struct {
	Mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C). e 
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//=================State==================//
	State NodeState //当前身份

	CurrentTerm int //当前任期

	VotedFor int //给谁投票

	Log []Log //日志条目
	//=================State==================//

	//==============Volatile State on all Servers===================//

	CommitIndex int //已知已提交的最高日志的索引

	LastApplied int //已经用到状态机的最高日志的索引

	//============================================================================//

	//================Volatile state on leaders====================//
	NextIndex []int

	MatchIndex []int

	LastActiveTime time.Time //用于ticket检查是否该发起选票了

	VoteCount int //作为候选者的时候得到的票数

	//闹钟
	applyCond *sync.Cond //条件变量

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	return rf.CurrentTerm, rf.State == StateLeader
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

	w:=new(bytes.Buffer)//1.准备一个字节桶，用来存放作为二进制的数据

	//2.创建一个编码器(Encoder)
	e:=labgob.NewEncoder(w)

	//3.开始往这里放东西，顺序很重要 要牢记
	if e.Encode(rf.CurrentTerm)!=nil||
	e.Encode(rf.VotedFor)!=nil||
	e.Encode(rf.Log)!=nil{
		return
	}

	//4.把桶里的东西倒出来变成一段[]byte
	data:=w.Bytes()
	rf.persister.Save(data,nil)

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

	//2.把存档的字节数据包装成一个可读的桶
	r:=bytes.NewBuffer(data)

	//3.创建一个解码器 告诉它去桶里捞数据
	d:=labgob.NewDecoder(r)

	//4.定义变量来接收从桶里捞出来的数据
	var currentTerm int
	var VotedFor int
	var log []Log//这里Log是你定义的结构体

	//5.按存档的顺序一个个捞出来 顺序错了数据就乱了
	if d.Decode(&currentTerm)!=nil||
	d.Decode(&VotedFor)!=nil||
	d.Decode(&log)!=nil{
		//如果解码失败 说明格式不对
		DPrintf("ReadPersist Error!")
	}else{
		//6.成功捞出来后 赋值回rf对象
		rf.Mu.Lock()
		rf.CurrentTerm=currentTerm
		rf.VotedFor=VotedFor
		rf.Log=log
		rf.Mu.Unlock()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int //候选人此时的任期号
	CandidateId  int //候选人的ID 便于投票者知道他投给了谁
	LastLogIndex int //日志目录里面最后一条日志的索引
	LastLogTerm  int //最后一条日志的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //目前返回者的term 如果大于候选者 候选者退出竞选
	VoteGranted bool //返回者是否同意了该票
}

type AppendEntriesArgs struct {
	Term         int   //leader的任期号
	LeaderId     int   //leader的ID
	PrevLogIndex int   //leader的最新日志的前一个索引号
	PrevLogTerm  int   //leader的最新日志的前一个索引的任期
	Entries      []Log //需要存储的日志条目 如果是心跳就为空
	LeaderCommit int   //leader已经确认提交的日志索引号
}

type AppendEntriesResults struct {
	Term    int  //返回者的term，如果更大 leader直接退位
	Success bool //此命令对方是否收到


	//========Lab3c新增参数====================//
	ConflictTerm int//Follower冲突条目的任期号
	ConflictIndex int//Follower冲突任期的第一个索引
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if args.Term < rf.CurrentTerm { //候选人的任期比自己的还小
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	//候选人的任期比我的大
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.State = StateFollower
		rf.VotedFor = -1
		rf.persist()
	}

	//给leader返回最新的任期
	reply.Term = rf.CurrentTerm

	//核心投票逻辑检查
	//条件A：我还没投过票（votefor==-1）或者投给了同一个候选人（幂等性）
	//条件B：对方的日志必须至少和我一样新
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.persist()
		DPrintf("[Node %d] 准备给 Node %d 投赞成票，任期 %d", rf.me, args.CandidateId, args.Term)

		//只要投了票  就要重置自己的时钟  否则不管它它超时选举 会干扰整个集群
		rf.LastActiveTime = time.Now()

	} else {
		reply.VoteGranted = false
	}

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesResults) {

	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	//1.先检查任期 任期比我的还小直接拒绝
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}
	//2.认可了对方 自己降级为follower  并且更新任期和心跳时间
	rf.State = StateFollower
	//3.重申一遍 必须更新心跳时间
	rf.LastActiveTime = time.Now() //重置闹钟  防止自己发起选举
	//4.进行一致性检查 检查对方的日志任期有没有我的大 以及同样的任期的话 有没有我的新
	//if len(rf.Log) <= args.PrevLogIndex || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
	//	reply.Term = rf.CurrentTerm
	//	reply.Success = false
	//	return
	//}

	//第一种失败：Follower的日志太短
	if len(rf.Log) <= args.PrevLogIndex{
		reply.Success=false
		reply.Term=rf.CurrentTerm
		reply.ConflictTerm=-1
		reply.ConflictIndex=len(rf.Log)
		return
	}

	//第二种失败 任期冲突
	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {

		reply.Success=false
		reply.Term=rf.CurrentTerm
		reply.ConflictTerm=rf.Log[args.PrevLogIndex].Term


		//向上寻找该冲突任期的第一条日志索引
		index:=args.PrevLogIndex

		//只要任期一直向前找 就不要越过下标0
		for index>0&&rf.Log[index].Term==reply.ConflictTerm{
			index--
		}
		reply.ConflictIndex=index+1
		return
	}


	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(rf.Log) {
			if rf.Log[index].Term != entry.Term {
				rf.Log = rf.Log[:index]
				rf.Log = append(rf.Log, args.Entries[i:]...)
				rf.persist()
				break
			} else {

			}
		} else { //说明rf.Log没那么长
			rf.Log = append(rf.Log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.CommitIndex {
		LastIndex := len(rf.Log) - 1
		if LastIndex > args.LeaderCommit {
			rf.CommitIndex = args.LeaderCommit
		} else {
			rf.CommitIndex = LastIndex
		}

		DPrintf("[Node %d - Follower] 更新 CommitIndex 为 %d 并唤醒 Applier", rf.me, rf.CommitIndex)
		rf.applyCond.Broadcast()
	}

	//匹配成功 返回true
	reply.Term = rf.CurrentTerm
	reply.Success = true
	DPrintf("[Node %d - Follower] 收到 Node %d 的 AE, Success: %v, CommitIndex: %d",
		rf.me, args.LeaderId, reply.Success, rf.CommitIndex)
}

func (rf *Raft) isLogUpToDate(candidateIndex int, candidateTerm int) bool {

	lastIndex := len(rf.Log) - 1
	lastTerm := rf.Log[lastIndex].Term

	//如果候选人的最后一条日志任期更大，或者任期相同但是日志更长  则投赞成票
	if candidateTerm != lastTerm {
		return candidateTerm > lastTerm
	}

	return candidateIndex >= lastIndex
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if rf.State != StateLeader {
		return -1, -1, false
	}
	term = rf.CurrentTerm
	index = len(rf.Log)
	Logentry := Log{
		Command: command,
		Term:    term,
	}
	rf.Log = append(rf.Log, Logentry)
	rf.persist()
	rf.NextIndex[rf.me] = len(rf.Log)
	rf.MatchIndex[rf.me] = index
	DPrintf("[Node %d - Leader] 收到新指令: Index %d, Term %d, Command ", rf.me, index, rf.CurrentTerm)

	// Your code here (3B).
	go rf.BroadcastHeartBeat()
	return index, term, isLeader
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
func (rf *Raft) startElection() {

	rf.Mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.Log) - 1,
		LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
	}
	rf.Mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(serverId int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverId, &args, &reply)
			if ok {
				rf.Mu.Lock()
				defer rf.Mu.Unlock()

				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.State = StateFollower
					rf.VotedFor = -1
					rf.LastActiveTime = time.Now()
					rf.persist()
					return
				}
				if rf.State != StateCandidate || rf.CurrentTerm != args.Term {
					return
				}

				//数票
				if reply.VoteGranted {
					rf.VoteCount++
					if rf.VoteCount > len(rf.peers)/2 && rf.State == StateCandidate {

						rf.State = StateLeader
						DPrintf("[Node %d] *** 成为 Leader ***, Term: %d, 日志长度: %d", rf.me, rf.CurrentTerm, len(rf.Log))
						for i := range rf.NextIndex {
							rf.NextIndex[i] = len(rf.Log)
							rf.MatchIndex[i] = 0
						}

						go rf.LeaderHeartBeatLoop()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) LeaderHeartBeatLoop() {

	for !rf.killed() {
		time.Sleep(100 * time.Millisecond) //心跳频率
		rf.Mu.Lock()
		if rf.State != StateLeader {
			rf.Mu.Unlock()
			return
		}
		rf.Mu.Unlock()
		go rf.BroadcastHeartBeat()
	}
}

func (rf *Raft) BroadcastHeartBeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.SendOneHeartbeat(i)
	}
}

func (rf *Raft) SendOneHeartbeat(serverID int) {
	rf.Mu.Lock()
	if rf.State != StateLeader {
		rf.Mu.Unlock()
		return
	}
	//构造心跳包
	prevIndex := rf.NextIndex[serverID] - 1
	nEntires := len(rf.Log) - rf.NextIndex[serverID]
	entries := make([]Log, nEntires)
	copy(entries, rf.Log[rf.NextIndex[serverID]:])
	args := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  rf.Log[prevIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.CommitIndex,
	}
	rf.Mu.Unlock()
	reply := AppendEntriesResults{}
	DPrintf("[Node %d - Leader] 向 Node %d 发送 AE, PrevIndex: %d, Log条数: %d",
		rf.me, serverID, args.PrevLogIndex, len(args.Entries))
	ok := rf.peers[serverID].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		rf.Mu.Lock()
		defer rf.Mu.Unlock()
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.State = StateFollower
			rf.VotedFor = -1
			rf.LastActiveTime = time.Now()
			rf.persist()
		}

		if rf.State != StateLeader {
			return
		}
		if reply.Success { //表示日志同步成功了
			newMatch := args.PrevLogIndex + len(args.Entries)
			if newMatch > rf.MatchIndex[serverID] {
				rf.MatchIndex[serverID] = newMatch
				DPrintf("[Node %d - Leader] 收到 Node %d 回执, matchIndex 更新为: %d, 数组现状: %v",
					rf.me, serverID, rf.MatchIndex[serverID], rf.MatchIndex)
				rf.NextIndex[serverID] = rf.MatchIndex[serverID] + 1
			}
			//开始数有没有一半commit了 如果有就选择commit
			rf.advanceCommit() //数票函数
		} else {
			//3.核心改动：快速回退逻辑
			if reply.ConflictTerm==-1{
				//情况一：Follower日志太短 根本没有PrevLogIndex那个位置
				rf.NextIndex[serverID]=reply.ConflictIndex
			}else{
				//对应位置任期不匹配
				//Leader尝试在自己的日志里找冲突的任期
				lastIndexTerm:=-1//核心地方 快速回退逻辑
				for i:=len(rf.Log)-1;i>=1;i--{//从后往前找，跳过下标0
					if rf.Log[i].Term==reply.ConflictTerm{
						lastIndexTerm=i
						break
					}

				}
				
				if lastIndexTerm!=-1{
					//Leader也有这个任期
					//将NextIndex设置为Leader 账本中该任期最后一条日志的下一个
					rf.NextIndex[serverID]=lastIndexTerm+1
				}else{
					rf.NextIndex[serverID]=reply.ConflictIndex
				}
			}

			//4.边界检查 确保NextIndex永远不会越过哨兵位
			if rf.NextIndex[serverID]<1{
				rf.NextIndex[serverID]=1
			}

		}

	}
}

func (rf *Raft) advanceCommit() {
	if rf.State != StateLeader { //先判断是不是leader
		return
	}
	tmpMatch := make([]int, len(rf.peers)) //先设置一个有所有客户端的大小的切片
	copy(tmpMatch, rf.MatchIndex)          //把所有节点的数据拷贝到这个切片上来
	tmpMatch[rf.me] = len(rf.Log) - 1      //先把leader自己的以提交的数据写为最大
	sort.Ints(tmpMatch)                    //排序
	N := tmpMatch[(len(rf.peers)-1)/2]     //找到存了中位数的节点 ，他所写的commit就是有一半提交的commit
	if N > rf.CommitIndex && rf.Log[N].Term == rf.CurrentTerm {
		rf.CommitIndex = N
		DPrintf("[Node %d - %v] !!! CommitIndex 更新 !!! -> %d", rf.me, rf.State, rf.CommitIndex)
		rf.applyCond.Broadcast()

	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 50)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		timeout := time.Duration(300+rand.Intn(200)) * time.Millisecond

		rf.Mu.Lock()
		if (time.Since(rf.LastActiveTime) > timeout) && rf.State != StateLeader {
			rf.State = StateCandidate
			rf.CurrentTerm++
			rf.VotedFor = rf.me
			rf.persist()
			rf.VoteCount = 1
			rf.LastActiveTime = time.Now()
			go rf.startElection()
		}
		rf.Mu.Unlock()

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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.State = StateFollower
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = append(make([]Log, 0), Log{Term: 0})
	rf.LastActiveTime = time.Now()
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.VoteCount = 0
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh 
	rf.applyCond = sync.NewCond(&rf.Mu)
	go rf.applier()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.Mu.Lock()
		//检查有没有新的提交的，如果没有 就等待
		for rf.CommitIndex <= rf.LastApplied&& !rf.killed() {
			rf.applyCond.Wait() //进入等待并且把锁释放
		}
		if rf.killed() { 
            rf.Mu.Unlock()
            return
        }
		commitIndex := rf.CommitIndex
		LastApplied := rf.LastApplied
		baseIndex := rf.LastApplied + 1
		entries := make([]Log, commitIndex-LastApplied)
		copy(entries, rf.Log[LastApplied+1:commitIndex+1])
		//更新要去执行的记录
		rf.LastApplied = rf.CommitIndex
		rf.Mu.Unlock()

		for i, entry := range entries {
			DPrintf("[Node %d - Applier] 送货到状态机: Index %d, Command...", rf.me, baseIndex+i)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: baseIndex + i,
			}
			DPrintf("[Node %d] 成功放入管道: Index %d", rf.me, baseIndex+i)
		}

	}
}
