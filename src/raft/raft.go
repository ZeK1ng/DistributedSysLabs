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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int 
	logs []LogEntry
	votedFor int 
	commitIndex int
	lastApplied int 
	nextIndex[] int 
	LeaderID int
	matchIndex[] int
	lastHeartBeat time.Time
	State string
	mut sync.Mutex
	applyChan chan ApplyMsg
}

type LogEntry struct{
	Term int
	Cmd interface{}
	Index int 
}
type AppendEntriesRequest struct{
	Term int
	LeaderID int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommintIndex int
}
type AppendEntriesResponce struct{
	Success bool
	Term int
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	isleader = false
	// Your code here (2A).
	rf.mut.Lock()
	defer rf.mut.Unlock()
	term = rf.currentTerm 
	if rf.State == "LEADER" { isleader = true}
	return term, isleader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	LastLogInd int
	LastLogTerm int 
	Term int 
	CandidateID int 
}	


//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mut.Lock()
	DPrintf("Args Term %v Rf %v  Candid %v \n",args.Term,rf.me , args.CandidateID)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf("Not Voted Rf %v  Candid %v \n",rf.me , args.CandidateID)

	}else if (args.Term == rf.currentTerm && rf.votedFor == -1) || args.Term >rf.currentTerm{
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.lastHeartBeat = time.Now()
		rf.State = "FOLLOWER"
		DPrintf("Voted Args Term %v Rf %v  Candid %v \n",args.Term,rf.me , args.CandidateID)
	}
	rf.mut.Unlock()
	rf.updateTerm(args.Term)

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


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.logs = make([]LogEntry, 0)
	rf.State = "FOLLOWER"
	peerln := len(peers) 
	rf.nextIndex = make([]int,peerln)
	rf.matchIndex = make([]int , peerln)
	rf.votedFor = -1
	rf.LeaderID = -1
	rf.lastApplied = -1
	rf.commitIndex = -1
	rf.applyChan = applyCh
	rf.lastHeartBeat = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.kickOff()

	return rf
}

func getRandomTimePoint(l int , h int) int {
	return l + rand.New(rand.NewSource(time.Now().UnixNano())).Intn(h-l)
}

func (rf* Raft)kickOff(){
	for{
		rf.mut.Lock()
		lastHr := rf.lastHeartBeat
		// votedFor := rf.votedFor
		if rf.State == "FOLLOWER" || rf.State == "CANDIDATE" {
			rf.mut.Unlock()
			randomTimePoint := getRandomTimePoint(200,600)
			time.Sleep(time.Duration(randomTimePoint)*time.Millisecond)
			if time.Since(lastHr) > time.Duration(randomTimePoint)*time.Millisecond {
				rf.handleTimeout()
			}
		}else{
			rf.mut.Unlock()
			// time.Sleep(time.Millisecond*100)
			rf.sendHeartBeats()	

		}
	}
}

func(rf * Raft) AppendEntries(req* AppendEntriesRequest , resp * AppendEntriesResponce){
	rf.mut.Lock()
	if req.Term >= rf.currentTerm{
		rf.votedFor = -1
		rf.LeaderID = -1 
		rf.currentTerm = req.Term
		rf.State = "FOLLOWER"
		rf.lastHeartBeat = time.Now()
	}
	rf.mut.Unlock()
}
func(rf *Raft) handleTimeout(){
	rf.mut.Lock()
	voteGranted := 1	
	req := RequestVoteArgs{}
	resp := RequestVoteReply{}
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.State = "CANDIDATE"
	req.CandidateID = rf.me
	req.Term = rf.currentTerm
	rf.mut.Unlock()

	for i :=0 ; i<len(rf.peers); i++ {
		
		if i == rf.me { continue }
		go func(ind int , responce RequestVoteReply, request RequestVoteArgs){	
			if rf.sendRequestVote(ind,&request,&responce) {
				rf.updateTerm(responce.Term)
				if responce.VoteGranted	{
					rf.mut.Lock()
					voteGranted++
					rf.mut.Unlock()
				}	
			}
		}(i,resp,req)
	}
	time.Sleep(time.Millisecond * 50)
	rf.mut.Lock()
	voteCopy:=voteGranted
	rf.mut.Unlock()
	rf.checkAndBecomeLeader(voteCopy,len(rf.peers))
}

func(rf *Raft)updateTerm(respterm int){
	rf.mut.Lock()
	defer rf.mut.Unlock()
	if respterm <= rf.currentTerm { return }
	rf.currentTerm = respterm
	rf.votedFor = -1 
	rf.State = "FOLLOWER"
	rf.LeaderID = -1
}


func (rf * Raft)checkAndBecomeLeader(voteGranted int , voteFinished int){
	rf.mut.Lock()
	if voteGranted < voteFinished/2 + 1{
		rf.mut.Unlock()
		return
	}
	if rf.State != "CANDIDATE" { 
		rf.mut.Unlock()
		return
	}
	
	rf.State = "LEADER"
	rf.votedFor = -1
	rf.mut.Unlock()
}
func (rf * Raft) sendHeartBeats(){

		rf.mut.Lock()
		if rf.State != "LEADER" {
			rf.mut.Unlock()
			return 
		}
		req := AppendEntriesRequest{}
		req.LeaderID = rf.me
		req.Term = rf.currentTerm
		rf.mut.Unlock()
		res := AppendEntriesResponce{}
		for i:=0; i< len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func (ind int , req AppendEntriesRequest, resp AppendEntriesResponce){
				rf.mut.Lock()
				if rf.State != "LEADER" {
					rf.mut.Unlock()
					return
				}
				rf.mut.Unlock()
				if rf.sendAppendEnrty(ind,&req,&res){
					rf.updateTerm(resp.Term)
				}
			}(i,req,res)
		}	

		time.Sleep(time.Millisecond*100)
}

func (rf * Raft) sendAppendEnrty(ind int ,req *AppendEntriesRequest,res *AppendEntriesResponce) bool{
	status := rf.peers[ind].Call("Raft.AppendEntries",req,res)
	return status
}