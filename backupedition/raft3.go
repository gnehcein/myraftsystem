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
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"
const (
	follower = 0
	candidate= 1
	leader   = 2
)


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

type entrry struct {
	Term			int
	Command 		interface{}
}
//
// A Go object implementing a single Raft peer.
//
type bcfl struct {
	term 	int
}
type Raft struct {
	mu        		sync.Mutex          // Lock to protect shared access to this peer's state
	peers     		[]*labrpc.ClientEnd // RPC end points of all peers
	persister 		*Persister          // Object to hold this peer's persisted state
	me        		int                 // this peer's index into peers[]
	dead      		int32               // set by Kill()
	term 	  		int
	commitindex     int
	lastapplied 	[]int
	nextindex 		[]int
	ident 			int
	log				[]entrry
	bcfollower		chan bcfl
	sendch			chan bool
	applyCh 		chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term=rf.term
	isleader= rf.ident==leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err1:=e.Encode(rf.log)
	err2:=e.Encode(rf.term)
	err3:=e.Encode(rf.commitindex)
	if err1!=nil||err2!=nil||err3!=nil{
		fmt.Println(err1,err2)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	err1:=d.Decode(&rf.log)
	err2:=d.Decode(&rf.term)
	err3:=d.Decode(&rf.commitindex)
	if err1!= nil||err2!=nil||err3!=nil{
	  	fmt.Println("decode,err")
	}
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
	time.Sleep(10*time.Millisecond)
	if rf.killed(){
		return index,term,false
	}
	rf.mu.Lock()
	isLeader := rf.ident==leader
	if isLeader{
		//for i:=0;i< len(rf.log);i++{
		//	if rf.log[i].Command==command {
		//		index=i
		//		term=rf.log[i].Term
		//		rf.mu.Unlock()
		//		return index,term,isLeader
		//	}
		//}
		index=len(rf.log)
		term=rf.term
		en1:=entrry{term,command}
		rf.log=append(rf.log,en1)
		rf.persist()
		go func() {
			if len(rf.sendch)<5{
				rf.sendch<-true
			}
		}()

	}
	rf.mu.Unlock()
	// Your code here (2B).
	return index, term, isLeader
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	Candidateid 	int
	Lastlogindex	int
	Lastlogterm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Termcurrent 	int
	Granted			bool
	Requestterm     int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
	//fmt.Println(args.Term,rf.me,args.Candidateid)
	// Your code here (2A, 2B).
	if rf.killed() {
		reply.Granted=false
		reply.Termcurrent=-1
		reply.Requestterm=args.Term
		return
	}
	rf.mu.Lock()

	defer rf.mu.Unlock()

	if args.Term<=rf.term{
		reply.Granted=false
		reply.Termcurrent=rf.term
		reply.Requestterm=args.Term
		return
	}else {
		cuindex:=len(rf.log)-1
		cuterm:=rf.log[cuindex].Term
		rf.term=args.Term
		rf.ident=follower
		rf.persist()
		if args.Lastlogterm>cuterm||((args.Lastlogterm==cuterm)&&(args.Lastlogindex>=cuindex)) {
			reply.Granted=true
			bc1:=bcfl{rf.term}
			reply.Termcurrent=rf.term
			rf.mu.Unlock()
			rf.bcfollower<-bc1
			rf.mu.Lock()

		}else {
			reply.Granted=false
		}

	}
	reply.Requestterm=args.Term
	return
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
func (rf *Raft)Vote() bool{
	var vote int =0
	var mu1 	sync.Mutex
	rf.mu.Lock()
	n:=len(rf.peers)
	if rf.ident!=candidate||rf.killed(){
		rf.mu.Unlock()
		return false
	}
	me:=rf.me
	term1:=rf.term
	lastlogi:=len(rf.log)-1
	lastlogt:=rf.log[lastlogi].Term
	rf.mu.Unlock()
	for i:=0;i<n;i++{
		if i==me{
			continue
		}
		go func(i int) {
			Votereq:=RequestVoteArgs{
				Term: term1,
				Candidateid: me,
				Lastlogindex: lastlogi,
				Lastlogterm: lastlogt,
			}
			var Voterpl RequestVoteReply
			ok:=rf.sendRequestVote(i,&Votereq,&Voterpl)
			rf.mu.Lock()
			if !ok||rf.ident!=candidate{
				rf.mu.Unlock()
				return
			}
			if Voterpl.Termcurrent>rf.term  {
				rf.ident=follower
				rf.term=Voterpl.Termcurrent
				bcfl1:=bcfl{rf.term}
				rf.persist()
				rf.mu.Unlock()
				rf.bcfollower<-bcfl1
				return
			}
			if Voterpl.Requestterm!=rf.term{
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			mu1.Lock()
			if Voterpl.Granted{
				//fmt.Println("true")
				vote++
			}
			mu1.Unlock()
		}(i)
	}
	time.Sleep(30*time.Millisecond)
	mu1.Lock()
	defer mu1.Unlock()
	if vote >=n/2{
		return true
	}
	return false
}
type AppendEntries struct {
	Id 			int
	Term 		int
	Prevlogi	int
	Prevlogt	int
	Leadercmit	int
	Lostsofent	[]entrry//如果需要大量传enttry的话，启用
}
type Appendreply struct {
	Nextindex 	int
	Baseterm 	int
	CurTerm 	int
	ReqTerm		int
}
func (rf *Raft)AppendEntries(req *AppendEntries,reply *Appendreply){
	//fmt.Println("lastlg",rf.term,req.Term,rf.me,req.Id)
	//fmt.Println("kdsalfjawdjwelfjwef",rf.log[len(rf.log)-1])
	if rf.killed() {
		return
	}
	rf.mu.Lock()

	//fmt.Println("lastlg111",rf.term,req.Term,rf.me,req.Id)
	defer rf.mu.Unlock()
	if req.Term<rf.term{
		reply.CurTerm=rf.term
		reply.Nextindex=0
		reply.ReqTerm=req.Term
	}else {
		rf.term=req.Term
		rf.ident=follower
		if len(rf.log)>req.Prevlogi {
			if req.Prevlogt<rf.log[req.Prevlogi].Term{
				for j:=req.Prevlogi-1;j>=0;j--{
					if rf.log[j].Term<=req.Prevlogt {
						reply.Nextindex=j+1
						break
					}
				}
			}else if req.Prevlogt>rf.log[req.Prevlogi].Term {
				reply.Baseterm=rf.log[req.Prevlogi].Term
				reply.Nextindex=1
			}else{
				if req.Lostsofent==nil{
					reply.Nextindex=req.Prevlogi+1

				}else {
					if len(rf.log)-req.Prevlogi> len(req.Lostsofent){
						for index:=0;index< len(req.Lostsofent);index++{
							if req.Lostsofent[index].Term!=rf.log[req.Prevlogi+1+index].Term {
								if len(rf.log)>req.Prevlogi+1{
									rf.log=rf.log[:req.Prevlogi+1]
								}
								for t:=0;t< len(req.Lostsofent);t++{
									rf.log=append(rf.log,req.Lostsofent[t])
								}
								reply.Nextindex=len(rf.log)
								rf.persist()
								break
							}
						}
					}else {
						if len(rf.log)>req.Prevlogi+1{
							rf.log=rf.log[:req.Prevlogi+1]
						}
						for t:=0;t< len(req.Lostsofent);t++{
							rf.log=append(rf.log,req.Lostsofent[t])
						}
						reply.Nextindex=len(rf.log)
						rf.persist()
					}
				}
			}
		}else{
			reply.Nextindex= len(rf.log)
		}
		if req.Leadercmit>rf.commitindex&&reply.Nextindex>req.Prevlogi&&reply.Nextindex-1>=req.Leadercmit{
			for i:=rf.commitindex+1;i<=req.Leadercmit;i++{
				msg:=ApplyMsg{
					CommandIndex: i,
					Command: rf.log[i].Command,
					CommandValid: true,
				}
				rf.applyCh<-msg
			}
			rf.commitindex=req.Leadercmit
			rf.persist()
		}
		reply.CurTerm=req.Term
		reply.ReqTerm=req.Term
		fl:=bcfl{rf.term}

		rf.mu.Unlock()
		rf.bcfollower<-fl
		rf.mu.Lock()
	}
}
func  (rf *Raft)AE (i int) bool {
	//fmt.Println(rf.nextindex[i],i)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.ident!=leader||rf.killed(){
		return	true
	}
	var ok bool
	var nexti int
	var req AppendEntries
	var rep Appendreply
	var newnext		int
	//if rf.nextindex[i]> len(rf.log){
	//	rf.nextindex[i]=len(rf.log)
	//}
	if rf.lastapplied[i]>-1{
		nexti=rf.lastapplied[i]+1
	}else {
		if rf.nextindex[i]>100{
			rf.nextindex[i]=1
			rf.lastapplied[i]=0
		}
		nexti=rf.nextindex[i]
	}
	req=AppendEntries{
		Id: 		rf.me,
		Term: 		rf.term,
		Prevlogi: 	nexti-1,
		Prevlogt: 	rf.log[nexti-1].Term,
		Leadercmit: rf.commitindex,
	}
	if nexti< len(rf.log)&&rf.lastapplied[i]!=-1{
		var m  int		//m是lostsofent最后的log 对应在rf.log的entry的index
		//如果是已经和follower达成之前的（index小的）日志一致，那么就一次全传过去;否则寻找一致的时候，只传一个entry
		m= len(rf.log)-1
		for k:=rf.lastapplied[i]+1;k<=m;k++{
			req.Lostsofent=append(req.Lostsofent,rf.log[k])
		}
	}
	rf.mu.Unlock()
	ok=rf.peers[i].Call("Raft.AppendEntries",&req,&rep)
	rf.mu.Lock()
	if !ok{
		return false
	}
	if rep.CurTerm>rf.term{
		rf.term=rep.CurTerm
		rf.ident=follower
		rf.persist()
		return true
	}
	if rep.ReqTerm==rf.term&&rep.CurTerm==rf.term {
		if rep.Baseterm>0 {
			for d:=req.Prevlogi-1;d>=0;d-- {
				if rf.log[d].Term<=rep.Baseterm {
					newnext=d+1
					break
					//fmt.Println("zhaodaole.//,/..........................ddfafsafas",i, rf.lastapplied,rf.nextindex)
				}
			}
		}else {
			newnext=rep.Nextindex
		}
		if newnext>rf.lastapplied[i] {
			if rf.lastapplied[i]<0&&newnext>rf.nextindex[i]{
				return true
			}
			if newnext>req.Prevlogi {
				rf.lastapplied[i]=newnext-1
			}
			//fmt.Println(rep.Nextindex,"ddsaafddddd")
			rf.nextindex[i]=newnext
		}
	}
	return true
}

func (rf *Raft)Worker()  {
	rand.Seed(time.Now().UnixNano())//随机值
	reled:=false //如果由别的身份变成leader，reled会被赋值为true然后成为leader后重新变为false;如果是持续leader状态，则为false
	for {

		n:=len(rf.peers)
		rf.mu.Lock()
		if rf.killed(){
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if rf.ident==leader {
			apply:=make([]int,len(rf.peers))
			if len(rf.lastapplied)==len(rf.peers){
				if reled{
					for i:=0;i<len(rf.lastapplied);i++{
						rf.lastapplied[i]=-1
						rf.nextindex[i]=len(rf.log)
					}
					reled=false
				}else {
					rf.lastapplied[rf.me]=len(rf.log)-1
					copy(apply,rf.lastapplied)
					sort.Ints(apply)
					max:=apply[(n-1)/2]
					if max>rf.commitindex&&max>=0&&rf.log[max].Term==rf.term{
						for i:=rf.commitindex+1;i<=max;i++{
							msg:=ApplyMsg{
								CommandValid:true,
								Command: rf.log[i].Command,
								CommandIndex: i,
							}
							rf.applyCh<-msg
						}
						rf.commitindex=max
						rf.persist()
					}
				}
			}else {
				for i:=0;i<n;i++{
					rf.lastapplied=append(rf.lastapplied,-1)
					rf.nextindex= append(rf.nextindex,len(rf.log))
				}
				if reled{
					reled=false
				}
			}
			rf.mu.Unlock()
			for i:=0;i<n ;i++ {
				if i==rf.me{
					continue
				}
				go rf.AE(i)
			}

			select {
			case bc:=<-rf.bcfollower:
				rf.mu.Lock()
				if rf.ident==leader&&bc.term==rf.term{
					rf.ident=follower
				}
				rf.mu.Unlock()
			case <-time.After(120*time.Millisecond):
			case <-rf.sendch:
				time.Sleep(40*time.Millisecond)
				if len(rf.sendch)>20{
					for i:=0;i<20;i++{
						<-rf.sendch
					}
				}
			}
		} else if rf.ident==candidate{
			term11:=rf.term
			rf.mu.Unlock()
			ok:=rf.Vote()
			rf.mu.Lock()
			if rf.ident!=candidate||rf.killed(){
				rf.mu.Unlock()
				continue
			}
			if ok&&rf.term==term11{
				reled=true
				rf.ident=leader
				rf.mu.Unlock()
			}else {
				rf.mu.Unlock()
				select {
				case <-time.After((time.Duration(rand.Int63()%15*20+500))*time.Millisecond):
					rf.mu.Lock()
					if !rf.killed()&&rf.ident==candidate&&rf.term==term11{
						rf.term++
						rf.persist()
					}
					rf.mu.Unlock()
				case <-rf.bcfollower:
				}
			}
		}else {
			rf.mu.Unlock()
			select {
			case <-time.After((time.Duration(rand.Int63()%15*55+150))*time.Millisecond):
				rf.mu.Lock()
				if !rf.killed()&&rf.ident==follower{
					rf.term++
					rf.ident=candidate
					rf.persist()
				}
				rf.mu.Unlock()
			case <-rf.bcfollower:
			}
		}
	}
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.bcfollower=make(chan bcfl)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.ident=follower
	rf.sendch=make(chan bool,40)
	rf.applyCh=applyCh
	rf.commitindex=0
	// Your initialization code here (2A, 2B, 2C).
	rf.readPersist(persister.ReadRaftState())
	if rf.log==nil{
		rf.log= append(rf.log, entrry{0,nil})
	}
	go rf.Worker()
	// initialize from state persisted before a crash

	return rf
}
