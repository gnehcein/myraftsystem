package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Method 		string
	Key 		string
	Value  		string
	Cid 		int64
	Seq			int32
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      	sync.Mutex
	me      	int
	rf      	*raft.Raft
	applyCh 	chan raft.ApplyMsg
	dead    	int32 // set by Kill()
	// fetch the current value for a key.
	keyValue     map[string]string
	getChan      map[int]chan Op
	cidSeq       map[int64]int32
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer)makechan(index int) chan Op {
	ch,ok:=kv.getChan[index]
	if ok{
		delete(kv.getChan,index)
	}
	kv.getChan[index]=make(chan Op,1)
	ch=kv.getChan[index]
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	defer func() {
		reply.Seq=args.Seq
		reply.Cid=args.Cid
	}()
	kv.mu.Lock()
	if kv.cidSeq[args.Cid]>args.Seq{
		//fmt.Println(kv.keyValue[args.Key],"GETBBBBBBBBBB",args.N,args.Cid%100)
		var ok bool
		reply.Value,ok=kv.keyValue[args.Key]
		if !ok {
			reply.Err=ErrNoKey
		}else {
			reply.Err=OK
		}
		kv.mu.Unlock()
		return
	}else {
		kv.mu.Unlock()
		op:=Op{
			Method: "Get",
			Key: args.Key,
			Value: "",
			Cid: args.Cid,
			Seq: args.Seq,
		}
		index,_,islead:=kv.rf.Start(op)
		if islead {
			kv.mu.Lock()
			ch:=kv.makechan(index)
			kv.mu.Unlock()
			select {
			case <-time.After(300*time.Millisecond):
				reply.Err=ErrTimeOut
			case op1:=<-ch:
				if op1.Seq==op.Seq&&op1.Cid==op.Cid&&op1.Method==op.Method&&op1.Key==op.Key{
					if op1.Value=="NOKEY" {
						reply.Err=ErrNoKey
					}else {
						reply.Value=op1.Value
						reply.Err=OK
					}
				}else {
					reply.Err=ErrWrongOp
				}
			}
		}else {
			reply.Err=ErrWrongLeader
		}
	}


}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply1 *PutAppendReply) {
	defer func() {
		reply1.Seq=args.Seq
		reply1.Cid=args.Cid
	}()
	kv.mu.Lock()
	var times int

		if _,ok:=kv.cidSeq[args.Cid];!ok {
		kv.cidSeq[args.Cid]=0
	}
	if kv.cidSeq[args.Cid]>=args.Seq{
		kv.mu.Unlock()
		reply1.Err=OK
		return
	}
	kv.mu.Unlock()
	var _,lead =kv.rf.GetState()
	for lead&&times<15 {
		kv.mu.Lock()
		//fmt.Println(kv.cidSeq)		//这两个print帮了我很大的忙
		//fmt.Println(args.Cid,args.Seq)
		if kv.rf.Killed() {
			kv.mu.Unlock()
			reply1.Err=ErrWrongLeader
			return
		}else if kv.cidSeq[args.Cid]==args.Seq-1{
			op:=Op{
				Method: args.Op,
				Key: args.Key,
				Value: args.Value,
				Cid: args.Cid,
				Seq: args.Seq,
			}
			kv.mu.Unlock()
			index,_,isleader:=kv.rf.Start(op)
			if !isleader{
				reply1.Err=ErrWrongLeader
			}else {
				kv.mu.Lock()
				ch1:=kv.makechan(index)
				kv.mu.Unlock()
				select {
				case <-time.After(500*time.Millisecond):
					reply1.Err=ErrTimeOut
				case op1:=<-ch1:
					if  op==op1{//op1.Seq==op.Seq&&op1.Cid==op.Cid&&op1.Method==op.Method
						reply1.Err=OK
						//fmt.Println(kv.keyValue[args.Key],"ok...........",args.Value)
					}else {
						reply1.Err=ErrWrongOp
					}
				}
				kv.mu.Lock()
				delete(kv.getChan,index)
				kv.mu.Unlock()
			}
			return
		}else if kv.cidSeq[args.Cid]>=args.Seq {
			reply1.Err=OK
			kv.mu.Unlock()
			return
		}else {
			kv.mu.Unlock()
			if times==10{
				kv.rf.Start("1000000000000dsafsdf")
			}
			time.Sleep(40*time.Millisecond)
		}
		_,lead=kv.rf.GetState()
		times++
	}
	//if _,lead:=kv.rf.GetState();!lead {
	//	reply.Err=ErrWrongLeader
	//	return
	//}
	reply1.Err=ErrWrongLeader
}


//ServerWorker is a function that handle the Applymsg form the rf's applyCh
//and execute the command,pass the Op to the chan which is in the kv.getChan


func (kv *KVServer)ServerWorker()  {
	for {
		if kv.killed() {
			return
		}
		select {
		case applyMsg :=<-kv.applyCh:
			index:= applyMsg.CommandIndex
			op,ok:= applyMsg.Command.(Op)
			if ok{
				kv.mu.Lock()
				if op.Method=="Get"{
					var ok1 bool
					op.Value,ok1=kv.keyValue[op.Key]
					if !ok1 {
						op.Value="NOKEY"
					}
				}else{		//PUT和APPEND情况
					if kv.cidSeq[op.Cid]==op.Seq-1 {
						switch op.Method {
						case "Put":
							kv.keyValue[op.Key]=op.Value
						case "Append":
							kv.keyValue[op.Key]+=op.Value
						}
						kv.cidSeq[op.Cid]=op.Seq
						//fmt.Println(kv.cidSeq,"put")
						//fmt.Println(".....................")
					}else if kv.cidSeq[op.Cid]<op.Seq-1 {
						fmt.Println(kv.cidSeq[op.Cid],op.Seq,"jianxiaole")
					}
				}
				ch,ok2:=kv.getChan[index]
				kv.mu.Unlock()
				if ok2{
					if len(ch)==0{
						ch<-op
					}else {
						fmt.Println("block1")
					}
				}
			}else {
				DPrintf("!OP")
			}
		case <-time.After(10*time.Second):
			//fmt.Println("pause")
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.getChan=make(map[int]chan Op)
	kv.cidSeq=make(map[int64]int32)
	kv.keyValue=make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ServerWorker()
	return kv
}
