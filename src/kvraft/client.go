package kvraft

import (
	"../labrpc"
	"fmt"
	"sync"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	cid 		int64
	seq  		int32
	sendTo 		int
	mu 			sync.Mutex
	getn		int32
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.cid =nrand()

	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	//seq:=atomic.AddInt32(&ck.seq, 1)
	if key!="" {
		ck.mu.Lock()
		len1:= len(ck.servers)
		ck.mu.Unlock()
		seq:=atomic.LoadInt32(&ck.seq)
		for  {
			arg:=GetArgs{
				Key: key,
				Cid: atomic.LoadInt64(&ck.cid),
				Seq: seq,
			}
			var reply GetReply
			ck.mu.Lock()
			var sendt =ck.sendTo
			ck.mu.Unlock()
			var ok bool
			ok=ck.servers[sendt].Call("KVServer.Get",&arg,&reply)
			if ok&&reply.Cid==arg.Cid&&reply.Seq==arg.Seq{
				if reply.Err==ErrTimeOut||reply.Err==ErrWrongOp||reply.Err==ErrWrongLeader{
					ck.mu.Lock()
					if ck.sendTo==sendt {
						ck.sendTo++
						ck.sendTo%=len1
					}
					ck.mu.Unlock()
				}else if reply.Err==ErrNoKey{
					return ""
				}else if reply.Err==OK{
					return reply.Value
				}else {
					fmt.Println("eljewlewjowf")
				}
			}else {
				ck.mu.Lock()
				if ck.sendTo==sendt {
					ck.sendTo++
					ck.sendTo%=len1
				}
				ck.mu.Unlock()
			}
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	len1:= len(ck.servers)
	cid:=ck.cid
	ck.mu.Unlock()
	seq:=atomic.AddInt32(&ck.seq, 1)
	for  {
		//fmt.Println("append")
		args:=PutAppendArgs{
			Key: 	key,
			Value: 	value,
			Op: 	op,
			Cid: 	cid,
			Seq:	seq,
		}
		var reply PutAppendReply
		ck.mu.Lock()
		var sendt =ck.sendTo
		ck.mu.Unlock()
		ok:=ck.servers[sendt].Call("KVServer.PutAppend",&args,&reply)
		if ok&&reply.Cid==args.Cid&&reply.Seq==args.Seq{
			if reply.Err==OK{
				return
			}else if reply.Err==ErrWrongLeader||reply.Err==ErrWrongOp||reply.Err==ErrTimeOut {
				ck.mu.Lock()
				if ck.sendTo==sendt {
					ck.sendTo++
					ck.sendTo%=len1
				}
				ck.mu.Unlock()
			}
		}else {
			ck.mu.Lock()
			if ck.sendTo==sendt {
				ck.sendTo++
				ck.sendTo%=len1
			}
			ck.mu.Unlock()
		}
	}
	fmt.Println("err!!!!!!!!!!!!")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
