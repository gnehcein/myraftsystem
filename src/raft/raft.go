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
	"context"
	"fmt"
	"math"
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
type bcflld struct {
	term 	int
}
type Raft struct {
	mu        		sync.Mutex          	// Lock to protect shared access to this peer's state
	peers     		[]*labrpc.ClientEnd 	// RPC end points of all peers
	persister 		*Persister          	// Object to hold this peer's persisted state
	me        		int                 	// this peer's index into peers[]
	dead      		int32               	// set by Kill()
	term 	  		int
	voteFor 		bool
	commitindex     	int
	lastapplied 		[]int
	nextindex 		[]int
	ident 			int
	log			[]entrry
	bcfollower		chan bcflld		//两者共用bcflld(become follower or leader),
	bcleader		chan bcflld			
	sendch			chan bool
	applyCh 		chan ApplyMsg

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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.

func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err1:=e.Encode(rf.log)
	err2:=e.Encode(rf.term)
	err3:=e.Encode(rf.commitindex)
	err4:=e.Encode(rf.voteFor)
	if err1!=nil||err2!=nil||err3!=nil||err4!=nil{
		fmt.Println(err1,err2)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.

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
	err4:=d.Decode(&rf.voteFor)
	if err1!= nil||err2!=nil||err3!=nil||err4!=nil{
	  	fmt.Println("decode,err")
	}
}

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

func (rf *Raft) Kill() {		//stop the service
	atomic.StoreInt32(&rf.dead, 1)

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {	//生成一个新的entry
	index := -1
	term := -1

	if rf.killed(){
		return index,term,false
	}
	rf.mu.Lock()
	isLeader := rf.ident==leader
	if isLeader{
		//for i:=0;i< len(rf.log);i++{		//another style
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
		if len(rf.sendch)<30{
			rf.sendch<-true
		}
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
	Term		int
	Candidateid 	int
	Lastlogindex	int
	Lastlogterm	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Termcurrent 	int
	Granted		bool
	Requestterm     int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
	if rf.killed() {
		reply.Granted=false
		reply.Termcurrent=-1
		reply.Requestterm=args.Term
		return
	}
	rf.mu.Lock()
	if args.Term>rf.term||(args.Term==rf.term&&rf.ident==follower&&rf.voteFor==false){
		cuindex:=len(rf.log)-1
		cuterm:=rf.log[cuindex].Term
		rf.ident=follower
		rf.term=args.Term
		if args.Lastlogterm>cuterm||(args.Lastlogterm==cuterm&&args.Lastlogindex>=cuindex) {
			reply.Granted=true
			rf.voteFor=true
			reply.Termcurrent=rf.term
			rf.persist()
			rf.mu.Unlock()			//一定要记得先解锁，再传通道。在我写此项目中，锁和通道是死锁的唯二两个原因。
			rf.bcfollower<-bcflld{args.Term}
		}else {
			reply.Granted=false
			rf.voteFor=false		//因为如果是同term的follower，之前就是false，如果是term增加，也应该变成false
			reply.Termcurrent=rf.term
			rf.persist()
			rf.mu.Unlock()
		}
	}else {
		reply.Granted=false
		reply.Termcurrent=rf.term
		rf.mu.Unlock()
	}

	reply.Requestterm=args.Term
}


func (rf *Raft)Vote() {
	up:=make(chan int)
	vote:=make(chan int)
	ctx0:=context.Background()
	ctx1,cancel:=context.WithCancel(ctx0)
	defer cancel()
	//t1:=time.Now()  	//用来查看goroutine的执行时间
	//defer  func() {
	//	t2:=time.Now()
	//	fmt.Println(t2.Sub(t1),"vote")
	//}()
	var votenum int
	var mu 		sync.Mutex
	rf.mu.Lock()
	if rf.ident!=candidate||rf.killed(){
		rf.mu.Unlock()
		return
	}
	fmt.Println(rf.term,"VoteTerm",rf.me)
	n:=len(rf.peers)
	me:=rf.me			//对数据进行拷贝，每个子函数生成RequestVoteArgs结构时不用对raft加锁并调用其数据
	term1:=rf.term
	lastlogi:=len(rf.log)-1
	lastlogt:=rf.log[lastlogi].Term
	rf.mu.Unlock()
	for i:=0;i<n;i++{
		if i==me{
			continue
		}
		go func(i int) {
			mu.Lock()
			Votereq:=RequestVoteArgs{
				Term: term1,
				Candidateid: me,
				Lastlogindex: lastlogi,
				Lastlogterm: lastlogt,
			}
			var Voterpl RequestVoteReply
			//t1:=time.Now()
			mu.Unlock()
			ok := rf.peers[i].Call("Raft.RequestVote", &Votereq, &Voterpl)
			//fmt.Println("vote",time.Now().Sub(t1),ok)
			mu.Lock()
			defer mu.Unlock()
			if !ok||Voterpl.Requestterm!=term1{
				return
			}
			select {
			case <-ctx1.Done():
				return
			default:
			}
			if Voterpl.Termcurrent>term1  {
				up<-Voterpl.Termcurrent
				return
			}
			if Voterpl.Granted{
				vote<-9
			}
		}(i)
	}
	for {
		select {
		case newterm:=<-up:
			rf.mu.Lock()
			if newterm>rf.term{
				rf.term=newterm
				rf.ident=follower
				rf.voteFor=false
				rf.persist()
			}
			rf.mu.Unlock()
			rf.bcfollower<-bcflld{newterm}
			return
		case <-vote:
			votenum++
			if votenum>=n/2{
				select {
				case <-time.After(5*time.Millisecond):		//因为不知道worker的状态，可能不是candidate了，就没有bcleader通道
				case rf.bcleader<- bcflld{term1}:
				}
				return
			}
		case <-time.After(30*time.Millisecond):
			return
		}
	}
}
type AppendEntries struct {
	Id 		int
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
func (rf *Raft)AppendEntries(request AppendEntries,reply *Appendreply){
	//t1:=time.Now()  	//用来查看goroutine的执行时间
	//defer  func() {
	//	t2:=time.Now()
	//	fmt.Println(t2.Sub(t1),"append")
	//}()
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	if request.Term<rf.term{
		//fmt.Println("t0")
		reply.CurTerm=rf.term
		reply.Nextindex=1
		reply.ReqTerm=request.Term
		rf.mu.Unlock()
		return
	}else {
		if len(rf.log)>request.Prevlogi {
			//fmt.Println("t1")
			if request.Prevlogt<rf.log[request.Prevlogi].Term{
				//fmt.Println("t2")
				for j:=request.Prevlogi-1;j>=0;j--{
					if rf.log[j].Term<=request.Prevlogt {
						reply.Nextindex=j+1
						break
					}
				}
			}else if request.Prevlogt>rf.log[request.Prevlogi].Term {
				//fmt.Println("t3")
				reply.Baseterm=rf.log[request.Prevlogi].Term
				reply.Nextindex=1
			}else{
				//fmt.Println("t4")
				if request.Lostsofent==nil{
					reply.Nextindex=request.Prevlogi+1
				}else {
					if len(rf.log)-1-request.Prevlogi> len(request.Lostsofent){
						//如果接受者的log中在一致后的剩余长度比appendentries中的log要长，就进行鉴定，
						//否则万一是延迟过期的append，就可能把已完成的log给截断
						for index:=0;index< len(request.Lostsofent);index++{
							//一个一个鉴定，如果有不同，则直接截断更新（判断后截取）
							if request.Lostsofent[index].Term!=rf.log[request.Prevlogi+1+index].Term {
								rf.log=rf.log[:request.Prevlogi+1+index]
								for t:=index;t< len(request.Lostsofent);t++{
									rf.log=append(rf.log,request.Lostsofent[t])
								}
								rf.persist()
								break
							}
						}
					}else {
						rf.log=rf.log[:request.Prevlogi+1]	//直接截断
						for t:=0;t< len(request.Lostsofent);t++{
							rf.log=append(rf.log,request.Lostsofent[t])
						}
						rf.persist()
					}
					reply.Nextindex=request.Prevlogi+ len(request.Lostsofent)+1
					//因为保证了log中从0到lostsofent末尾所对应的index的entry是一致的
					// 所以用共同的的Nextindex
				}
			}
		}else{
			reply.Nextindex= len(rf.log)
		}
		if request.Leadercmit>rf.commitindex&&reply.Nextindex>request.Prevlogi&&reply.Nextindex-1>=request.Leadercmit{
			for i:=rf.commitindex+1;i<=request.Leadercmit;i++{
				msg:=ApplyMsg{
					CommandIndex: i,
					Command: rf.log[i].Command,
					CommandValid: true,
				}
				rf.applyCh<-msg
			}
			rf.commitindex=request.Leadercmit
		}
		rf.term=request.Term
		rf.ident=follower
		rf.voteFor=true
		reply.ReqTerm=request.Term
		fl:=bcflld{request.Term}
		rf.persist()
		rf.mu.Unlock()
		rf.bcfollower<-fl
		reply.CurTerm=request.Term
	}
}
func  (rf *Raft)AE (i int) {
	rf.mu.Lock()
	if rf.ident!=leader||rf.killed(){
		rf.mu.Unlock()
		return
	}
	term0:=rf.term
	var ok 			bool
	var nexti 		int
	var request 	AppendEntries
	var reply	 	Appendreply
	var newnext		int

	if rf.lastapplied[i]>=0{		//两种情况，一个已经达成一致，一个还在找一致的entry。
		nexti=rf.lastapplied[i]+1
	}else {
		nexti=rf.nextindex[i]
	}
	request=AppendEntries{
		Id: 		rf.me,
		Term: 		rf.term,
		Leadercmit: rf.commitindex,
		Prevlogi: 	nexti-1,
		Prevlogt: 	rf.log[nexti-1].Term,
		Lostsofent: nil,
	}
	if rf.lastapplied[i]!=-1&&nexti< len(rf.log){	//否则传的lostsofent为nil
		var m  int		//m是lostsofent中index最大的log 对应在rf.log的entry的index
		//如果是已经和follower达成之前的（index小的）日志一致，那么就一次全传过去;否则寻找一致的时候，只传一个entry
		m= int(math.Min(float64(nexti+200), float64(len(rf.log)-1))) //最多传200个
		for k:=nexti;k<=m;k++{
			request.Lostsofent=append(request.Lostsofent,rf.log[k])
		}
	}
	rf.mu.Unlock()
	t1:=time.Now()
	ok=rf.peers[i].Call("Raft.AppendEntries",request,&reply)
	t2:=time.Now()
	if t2.Sub(t1)>1*time.Second||!ok {
		if len(rf.sendch)<30{
			rf.sendch<-true		//超时重传
		}
		return
	}
	//fmt.Println("rpctime",t2.Sub(t1),ok,i)
	rf.mu.Lock()

	if reply.CurTerm>rf.term{
		rf.term=reply.CurTerm
		rf.voteFor=true
		rf.ident=follower
		rf.persist()
		rf.mu.Unlock()
		rf.bcfollower<-bcflld{reply.CurTerm}
		return
	}
	if rf.ident!=leader||rf.term!=term0||rf.killed(){
		rf.mu.Unlock()
		return
	}
	if reply.ReqTerm==request.Term&&reply.CurTerm==rf.term {		//用newnext进行对两种情况的整合
		if reply.Baseterm>0 {
			for d:=request.Prevlogi-1;d>=0;d-- {
				if rf.log[d].Term<=reply.Baseterm {	// runtime error: index out of range [214] with length 169
					newnext=d+1
					break
				}
			}
		}else {
			newnext=reply.Nextindex
		}
		if newnext>rf.lastapplied[i] {	//如果newnext<rf.lastapplied[i]，说明是延迟的rpc，忽略
			if rf.lastapplied[i]<0{
				if newnext<=rf.nextindex[i]{
					if newnext>request.Prevlogi {
						rf.lastapplied[i]=newnext-1
					}
					rf.nextindex[i]=newnext
				}
				//在未同步情况下，即rf.lastapplied[i]<0，如果这次达成同步了，newnext就等于rf.nextindext[i]，
				//否则newnext小于rf.nextindext[i]。所以本次rpc的newnext一定<=rf.nextindext[i]，如果>，则为延迟rpc
			}else {
				if request.Prevlogi==rf.lastapplied[i] {
					rf.lastapplied[i]=newnext-1
					rf.nextindex[i]=newnext
				}
			}
		}
	}
	rf.mu.Unlock()
	if t2.Sub(t1)>200*time.Millisecond{		//超时重传
		if len(rf.sendch)<30{
			rf.sendch<-true
		}
	}
}

func (rf *Raft)Worker()  {
	me:=rf.me
	rand.Seed(time.Now().UnixNano())//随机值
	for {
		rf.mu.Lock()
		n:=len(rf.peers)
		if rf.killed(){
			rf.mu.Unlock()
			return
		}
		if rf.ident==leader {
			apply:=make([]int,len(rf.peers))
			rf.lastapplied[rf.me]=len(rf.log)-1
			copy(apply,rf.lastapplied)
			sort.Ints(apply)
			max:=apply[(n-1)/2]		//max为大多数中最小的log提交高度
			if max>rf.commitindex&&rf.log[max].Term==rf.term{
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
			//for i:=0;i<n ;i++ {
			//	fmt.Println(rf.nextindex[i], "nextindex........",i)
			//	fmt.Println(rf.lastapplied[i], "lastapplied......",i)
			//}
			//fmt.Println()
			rf.mu.Unlock()
			for i:=0;i<n ;i++ {
				if i==me{
					continue
				}
				go rf.AE(i)
			}
			select {
			case <-rf.bcfollower:
			case <-time.After(120*time.Millisecond):
			case <-rf.sendch:	//加速模式，两种情况：1.收到新entry了。2.发送rpc超时了。3.收到过期的bcflld
				time.Sleep(50*time.Millisecond)		//稍等一下，要不然在one很多次的情况下，太快会产生大量无意义的较短append
				if len(rf.sendch)>5{
					for i:=0;i<5;i++{
						<-rf.sendch
					}
				}
			}
		} else if rf.ident==candidate{
			term2:=rf.term
			go rf.Vote()
			for  !rf.killed()&&rf.ident==candidate&&rf.term==term2{	//除了leader以外，candidate和follower设置了for循环。
				rf.mu.Unlock()										//因为在select中break没法跳出for，
				select {											//所以for来检测raft中的状态达到和内层for的共享
				case <-time.After((time.Duration(rand.Int63()%10*40+200))*time.Millisecond):
					rf.mu.Lock()
					if rf.ident==candidate&&!rf.killed(){
						rf.term++
						rf.voteFor=true
						rf.persist()
					}
				case <-rf.bcfollower:
					rf.mu.Lock()
				case bc2:=<-rf.bcleader:
					rf.mu.Lock()
					if rf.ident==candidate&&rf.term==bc2.term&&term2==bc2.term&&!rf.killed(){
						rf.voteFor=true
						rf.ident=leader
						rf.persist()
						rf.lastapplied=nil
						rf.nextindex=nil
						for i:=0;i<n;i++{
							rf.lastapplied=append(rf.lastapplied,-1)
							rf.nextindex= append(rf.nextindex,len(rf.log))
						}
					}
				}
			}
			rf.mu.Unlock()		//别忘了跳出后要解锁
		}else {
			term3:=rf.term
			for !rf.killed()&&rf.ident==follower&&rf.term==term3 {
				rf.mu.Unlock()
				select {
				case <-time.After((time.Duration(rand.Int63()%15*60+500))*time.Millisecond):
					rf.mu.Lock()
					if !rf.killed()&&rf.ident==follower{
						rf.term++
						rf.voteFor=true
						rf.ident=candidate
						rf.persist()
					}
				case <-rf.bcfollower:
					rf.mu.Lock()
				}
			}
			rf.mu.Unlock()
		}
	}
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
	rf.bcfollower=make(chan bcflld)
	rf.bcleader=make(chan bcflld)
	rf.peers = peers
	rf.persister = persister
	rf.voteFor=false
	rf.me = me
	rf.ident=follower
	rf.sendch=make(chan bool,100)
	rf.applyCh=applyCh
	rf.commitindex=0
	rf.readPersist(persister.ReadRaftState())	// initialize from state persisted before a crash
	if rf.log==nil{
		rf.log= append(rf.log, entrry{0,nil})
	}
	go rf.Worker()
	

	return rf
}
