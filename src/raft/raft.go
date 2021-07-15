package raft
//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
// A Go object implementing a single Raft peer.

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
	nilVote  =100000
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type entrry struct {
	Term			int
	Command 		interface{}
}

type bcflld struct {
	term 	int
}

type Raft struct {
	mu          	sync.Mutex
	peers       	[]*labrpc.ClientEnd // 各个节点的rpc-client
	persister   	*Persister          // 持久化状态
	me          	int                 // 此几点在peers数组中的index
	dead        	int32               // set by Kill()
	term        	int
	voteFor       	int
	Commitindex   	int					//可以向状态机发送的最高日志的index
	lastapplied   	[]int				//对leader有用，下面一个也是
	nextindex     	[]int
	Identity        int					//3种身份之一
	Log           	[]entrry
	bcfollower    	chan bcflld			//become follower or leader,
	bcleader      	chan bcflld			//两者共用
	sendch        	chan bool			//加快发送的信号
	applyCh       	chan ApplyMsg		//向状态机传递已经被大多数节点保存的稳定entry
}

//return currentTerm and whether this server
//believes it is the leader.

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term=rf.term
	isleader= rf.Identity ==leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err1:=e.Encode(rf.Log)
	err2:=e.Encode(rf.term)
	err3:=e.Encode(rf.voteFor)
	if err1!=nil||err2!=nil||err3!=nil{
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	err1:=d.Decode(&rf.Log)
	err2:=d.Decode(&rf.term)
	err3:=d.Decode(&rf.voteFor)
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
}

func (rf *Raft) Killed() bool {
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
	//fmt.Println("start")
	if rf.Killed(){
		return index,term,false
	}
	rf.mu.Lock()
	isLeader := rf.Identity ==leader
	if isLeader{
		//for i:=0;i< len(rf.log);i++{
		//	if rf.log[i].Command==command {
		//		index=i
		//		term=rf.log[i].Term
		//		rf.mu.Unlock()
		//		return index,term,isLeader
		//	}
		//}
		index=len(rf.Log)
		term=rf.term
		en1:=entrry{term,command}
		rf.Log =append(rf.Log,en1)
		//fmt.Println(rf.me,index)		//这个print帮了我很大的忙
		rf.persist()
		if len(rf.sendch)<10{
			rf.sendch<-true
		}
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

type InstallArgs struct {
	leaderTerm 		int
	leaderId		int
	lastIndex		int
	lastTerm		int
	offset 			int
	data  			[]byte
	done 			bool
}

type InstallReply struct {
	term 			int
}

func (rf Raft)InstallSnapshot(args *InstallArgs,reply *InstallReply)  {

}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term			int
	Candidateid 	int
	Lastlogindex	int
	Lastlogterm		int
}

type RequestVoteReply struct {
	Termcurrent 	int
	Granted			bool
	Requestterm     int

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
	if rf.Killed() {
		reply.Granted=false
		reply.Termcurrent=-1
		reply.Requestterm=args.Term
		return
	}
	rf.mu.Lock()
	//fmt.Println(rf.me,"Identity:",rf.Identity)
	if args.Term>rf.term||(args.Term==rf.term&&rf.Identity ==follower&&rf.voteFor==nilVote){
		cuindex:=len(rf.Log)-1
		cuterm:=rf.Log[cuindex].Term
		rf.Identity =follower
		rf.term=args.Term
		if args.Lastlogterm>cuterm||(args.Lastlogterm==cuterm&&args.Lastlogindex>=cuindex) {
			reply.Granted=true
			rf.voteFor=args.Candidateid
			reply.Termcurrent=rf.term
			rf.persist()
			rf.mu.Unlock()			//一定要记得先解锁，再传通道，锁和通道是死锁的唯二两个原因，在我写此项目中。
									//因为worker可能有未监听bcfollower通道的时候，处理完操作然后再占有锁才能继续监听bcfollower，
									//如果恰好这时候被别的goroutine持有锁，并且直到向bcfollower传入消息后才解锁，
									//这样worker会一直获取不到锁而等待，占有锁的goroutine也会一直卡在无缓冲的channel上，造成死结。
			rf.bcfollower<-bcflld{args.Term}
		}else {
			reply.Granted=false
			rf.voteFor=nilVote		//因为如果是同term的follower，之前就是nilVote，如果是term增加，也应该变成nilVote
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

type voteup struct {
	term 	int
}

func (rf *Raft)Vote() {
	up:=make(chan voteup,len(rf.peers)-1)
	vote:=make(chan int, len(rf.peers)-1)
	ctx0:=context.Background()
	ctx1,cancel:=context.WithCancel(ctx0)
	defer cancel()
	var votenum int
	var mu 		sync.Mutex	//在子方法中读写数据时加锁
	rf.mu.Lock()
	if rf.Identity !=candidate||rf.Killed(){
		rf.mu.Unlock()
		return
	}
	n:=len(rf.peers)
	me:=rf.me
	//对数据进行拷贝，每个子函数生成RequestVoteArgs结构时，
	//不用对raft加锁并调用其数据
	term1:=rf.term
	lastlogi:=len(rf.Log)-1
	lastlogt:=rf.Log[lastlogi].Term
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
				up1:=voteup{
					term: Voterpl.Termcurrent,
				}
				up<-up1
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
			if newterm.term>rf.term{
				rf.term=newterm.term
				rf.Identity =follower
				rf.voteFor=nilVote
				rf.persist()
			}
			rf.mu.Unlock()
			rf.bcfollower<-bcflld{newterm.term}
			return
		case <-vote:
			votenum++
			if votenum>=n/2{
				select {
				case <-time.After(5*time.Millisecond):
				//因为不知道worker的状态，可能不是candidate了，就没有bcleader通道
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
	Id 			int
	Term 		int
	Prevlogi	int
	Prevlogt	int
	Leadercmit	int
	Entries 	[]entrry//如果需要大量传enttry的话，启用
}

type AppendReply struct {
	Nextindex 	int
	Baseterm 	int
	CurTerm 	int
	ReqTerm		int
}

func (rf *Raft)AppendEntries(request AppendEntries,reply *AppendReply){
	if rf.Killed() {
		return
	}
	rf.mu.Lock()
	if request.Term<rf.term{
		reply.CurTerm=rf.term
		reply.Nextindex=1
		reply.ReqTerm=request.Term
		rf.mu.Unlock()
		return
	}
	if len(rf.Log)>request.Prevlogi {
		if request.Prevlogt<rf.Log[request.Prevlogi].Term{
			for j:=request.Prevlogi-1;j>=0;j--{
				if rf.Log[j].Term<=request.Prevlogt {
					reply.Nextindex=j+1
					break
				}
			}
		}else if request.Prevlogt>rf.Log[request.Prevlogi].Term {
			reply.Baseterm=rf.Log[request.Prevlogi].Term
			reply.Nextindex=1
		}else{
			if request.Entries==nil{
				reply.Nextindex=request.Prevlogi+1
			}else {
				if len(rf.Log)-1-request.Prevlogi> len(request.Entries){
					//如果接受者的log中在一致后的剩余长度比appendentries中的log要长，就进行鉴定，
					//否则万一是延迟过期的append，就可能把已完成的log给截断
					for index:=0;index< len(request.Entries);index++{
						//一个一个鉴定，如果有不同，则直接截断更新（判断后截取）
						if request.Entries[index].Term!=rf.Log[request.Prevlogi+1+index].Term {
							rf.Log =rf.Log[:request.Prevlogi+1+index]
							for t:=index;t< len(request.Entries);t++{
								rf.Log =append(rf.Log,request.Entries[t])
							}
							rf.persist()
							break
						}
					}
				}else {
					rf.Log =rf.Log[:request.Prevlogi+1] //直接截断
					for t:=0;t< len(request.Entries);t++{
						rf.Log =append(rf.Log,request.Entries[t])
					}
					rf.persist()
				}
				reply.Nextindex=request.Prevlogi+ len(request.Entries)+1
				//因为保证了log中从0到Entries末尾所对应的index的entry是一致的
				// 所以用共同的的Nextindex
			}
		}
	}else{
		reply.Nextindex= len(rf.Log)
	}
	if request.Leadercmit>rf.Commitindex &&reply.Nextindex>request.Prevlogi&&
		reply.Nextindex-1>=request.Leadercmit{
		for i:=rf.Commitindex +1;i<=request.Leadercmit;i++{
			if rf.Log[i].Command!="nil" {
				msg:=ApplyMsg{
					CommandIndex: i,
					Command: rf.Log[i].Command,
					CommandValid: true,
				}
				rf.applyCh<-msg
			}
		}
		rf.Commitindex =request.Leadercmit
	}
	rf.term=request.Term
	rf.Identity =follower
	rf.voteFor=request.Id
	reply.ReqTerm=request.Term
	fl:=bcflld{request.Term}
	rf.persist()
	rf.mu.Unlock()
	rf.bcfollower<-fl
	reply.CurTerm=request.Term

}
func  (rf *Raft)AE (i int) {
	rf.mu.Lock()
	if rf.Identity !=leader||rf.Killed(){
		rf.mu.Unlock()
		return
	}
	term0:=rf.term	//方法执行时leader的term，如果rpc返回之后的term发生改变，则return
	var ok 			bool
	var nexti 		int
	var request 	AppendEntries
	var reply	 	AppendReply
	var newnext		int
	if rf.lastapplied[i]>=0{
		nexti=rf.lastapplied[i]+1
	}else {
		nexti=rf.nextindex[i]
	}
	request=AppendEntries{
		Id: 		rf.me,
		Term: 		rf.term,
		Leadercmit: rf.Commitindex,
		Prevlogi: 	nexti-1,
		Prevlogt: 	rf.Log[nexti-1].Term,
		Entries: nil,
	}

	if rf.lastapplied[i]!=-1&&nexti< len(rf.Log){ //否则传的Entries为nil
		var m  int		//m是Entries中index最大的log 对应在rf.log的entry的index
		//如果是已经和follower达成之前的（index小的）日志一致，那么就一次全传过去;
		//否则寻找一致的时候，只传空的log切片
		m= int(math.Min(float64(nexti+50), float64(len(rf.Log)-1))) //最多传200个
		for k:=nexti;k<=m;k++{
			request.Entries=append(request.Entries,rf.Log[k])
		}
	}
	rf.mu.Unlock()
	t1:=time.Now()
	ok=rf.peers[i].Call("Raft.AppendEntries",request,&reply)

	t2:=time.Now()
	if t2.Sub(t1)>1*time.Second||!ok {
		if len(rf.sendch)<10{
			rf.sendch<-true		//超时重传
		}
		return
	}
	rf.mu.Lock()
	if reply.CurTerm>rf.term{
		rf.term=reply.CurTerm
		rf.Identity =follower
		rf.persist()
		rf.voteFor=nilVote
		rf.mu.Unlock()
		rf.bcfollower<-bcflld{reply.CurTerm}
		return
	}
	if rf.Identity !=leader||rf.term!=term0||rf.Killed(){
		rf.mu.Unlock()
		return
	}
	if reply.ReqTerm==request.Term {
		//用newnext进行对两种情况的整合
		if reply.Baseterm>0 {
			for d:=request.Prevlogi-1;d>=0;d-- {
				if rf.Log[d].Term<=reply.Baseterm {
					newnext=d+1
					break
				}
			}
		}else {
			newnext=reply.Nextindex
		}
		if newnext>rf.lastapplied[i] {
			//如果newnext<rf.lastapplied[i]，说明是延迟的rpc，忽略
			if rf.lastapplied[i]<0{
				if newnext<=rf.nextindex[i]{
					if newnext>request.Prevlogi {
						rf.lastapplied[i]=newnext-1
					}
					rf.nextindex[i]=newnext
				}
				//在未同步情况下，即rf.lastapplied[i]<0，如果这次达成同步了，
				//newnext就等于rf.nextindext[i]，
				//否则newnext小于rf.nextindext[i]。
				//所以本次rpc的newnext一定<=rf.nextindext[i]，如果>，则为延迟rpc
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
		if len(rf.sendch)<10{
			rf.sendch<-true
		}
	}
}

func (rf *Raft)Worker()  {
	me:=rf.me
	for {
		rand.Seed(time.Now().UnixNano())//随机值
		rf.mu.Lock()
		n:=len(rf.peers)
		if rf.Killed(){
			rf.mu.Unlock()
			return
		}
		//接下来分三种身份，leader，candidate和follower，只能选择一种。
		if rf.Identity ==leader {
			apply:=make([]int,len(rf.peers))	//为了排序
			rf.lastapplied[rf.me]=len(rf.Log)-1
			copy(apply,rf.lastapplied)
			sort.Ints(apply)
			max:=apply[(n-1)/2]		//max为大多数中最小的log提交高度(index)
			if max>rf.Commitindex &&rf.Log[max].Term==rf.term{
				for i:=rf.Commitindex +1;i<=max;i++{
					if rf.Log[i].Command!="nil" {
						msg:=ApplyMsg{
							CommandIndex: i,
							Command: rf.Log[i].Command,
							CommandValid: true,
						}
						rf.applyCh<-msg
					}
				}
				rf.Commitindex =max
				rf.persist()
			}
			rf.mu.Unlock()
			for i:=0;i<n ;i++ {
				if i==me{
					continue
				}
				go rf.AE(i)
			}
			select {
			case <-rf.bcfollower:
			case <-time.After(140*time.Millisecond):
			case <-rf.sendch:	//加速模式，两种情况：1.收到新entry了。
			// 2.发送rpc超时了。3.收到过期的bcflld
				time.Sleep(50*time.Millisecond)
				//稍等一下，要不然在one很多次的情况下，太快会产生大量无意义的较短append
				if len(rf.sendch)>5{
					for i:=0;i<5;i++{
						<-rf.sendch
					}
				}
			}
		} else if rf.Identity ==candidate{
			term2:=rf.term
			go rf.Vote()
			for  !rf.Killed()&&rf.Identity ==candidate&&rf.term==term2{
				rand.Seed(time.Now().UnixNano())
				//除了leader以外，candidate和follower设置了for循环。
				//因为在select中break没法跳出for，
				//所以for来检测raft中的状态达到和内层for的共享
				rf.mu.Unlock()
				select {
				case <-time.After((time.Duration(rand.Int63()%10*40+200))*time.Millisecond):
					rf.mu.Lock()
					if rf.Identity ==candidate&&!rf.Killed(){
						rf.term++
						rf.voteFor=rf.me
						rf.persist()
					}
				case <-rf.bcfollower:
					rf.mu.Lock()
				case bc2:=<-rf.bcleader:
					rf.mu.Lock()
					if rf.Identity ==candidate&&rf.term==bc2.term&&term2==bc2.term&&!rf.Killed(){
						rf.voteFor=rf.me
						rf.Identity =leader
						rf.persist()
						rf.lastapplied=nil
						rf.nextindex=nil
						for i:=0;i<n;i++{
							rf.lastapplied=append(rf.lastapplied,-1)
							rf.nextindex= append(rf.nextindex,len(rf.Log))
						}
					}
				}
			}
			rf.mu.Unlock()		//别忘了跳出后要解锁
		}else {
			term3:=rf.term
			for !rf.Killed()&&rf.Identity ==follower&&rf.term==term3 {
				rand.Seed(time.Now().UnixNano())
				rf.mu.Unlock()
				select {
				case <-time.After((time.Duration(rand.Int63()%15*60+300))*time.Millisecond):
					//fmt.Println("followerlock")	//这两个print帮了我很大的忙
					rf.mu.Lock()
					//fmt.Println("follUN")
					if !rf.Killed()&&rf.Identity ==follower{
						rf.term++
						rf.voteFor=rf.me
						rf.Identity =candidate
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
	rf.bcfollower=make(chan bcflld)
	rf.bcleader=make(chan bcflld)
	rf.peers = peers
	rf.persister = persister
	rf.voteFor=nilVote
	rf.me = me
	rf.Identity =follower
	rf.sendch=make(chan bool,100)
	rf.applyCh=applyCh
	rf.Commitindex =0
	rf.readPersist(persister.ReadRaftState())
	if rf.Log ==nil{
		rf.Log = append(rf.Log, entrry{0,nil})
	}
	go rf.Worker()
	return rf
}