package kvraft

import (
	"../labrpc"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
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
	val:=make(chan string,5)
	//seq:=atomic.AddInt32(&ck.seq, 1)
	if key!="" {
		ck.mu.Lock()
		len1:= len(ck.servers)
		ck.mu.Unlock()
		seq:=atomic.LoadInt32(&ck.seq)
		n:=atomic.AddInt32(&ck.getn,1)
		for  {
			ctx:=context.Background()
			ctx1,cancel:=context.WithCancel(ctx)
			arg:=GetArgs{
				Key: key,
				Cid: ck.cid,
				Seq: seq,
				N: n,
			}
			var reply GetReply
			var ok bool
			go func() {
				ok=ck.servers[ck.sendTo%len1].Call("KVServer.Get",&arg,&reply)
				select {
				case <-ctx1.Done():
					return
				default:
				}
				if ok&&reply.N==arg.N{
					if reply.Err==ErrTimeOut||reply.Err==ErrWrongOp||reply.Err==ErrWrongLeader{
						ck.mu.Lock()
						ck.sendTo++
						ck.sendTo%=len1
						ck.mu.Unlock()
					}else if reply.Err==ErrNoKey{
						val<-"nokey"
					}else if reply.Err==OK{
						val<- reply.Value
					}else {
						fmt.Println("eljewlewjowf")
					}
				}else {
					ck.mu.Lock()
					ck.sendTo++
					ck.sendTo%=len1
					ck.mu.Unlock()
				}
			}()
			select {
			case va:=<-val:
				if va=="nokey"{
					return ""
				}else {
					return va
				}
			case <-time.After(1*time.Second):
				cancel()
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
	over:=make(chan bool,5)
	ck.mu.Unlock()
	seq:=atomic.AddInt32(&ck.seq, 1)
	for  {
		ctx:=context.Background()
		ctx1,cancel:=context.WithCancel(ctx)
		args:=PutAppendArgs{
			Key: 	key,
			Value: 	value,
			Op: 	op,
			Cid: 	cid,
			Seq:	seq,
		}
		var reply PutAppendReply
		var ok bool
		go func() {
			select {
			case <-ctx1.Done():
				return
			default:
			}
			ok=ck.servers[ck.sendTo%len1].Call("KVServer.PutAppend",&args,&reply)
			if ok&&reply.N==args.Seq{
				if reply.Err==OK{
					over<-true
				}else if reply.Err==ErrWrongLeader||reply.Err==ErrWrongOp {
					ck.mu.Lock()
					ck.sendTo++
					ck.sendTo%=len1
					ck.mu.Unlock()
				}
			}else {
				ck.mu.Lock()
				ck.sendTo++
				ck.sendTo%=len1
				ck.mu.Unlock()
			}
			over<-false
		}()
		select {
		case b:=<-over:
			if b{
				return
			}
		case <-time.After(1*time.Second):
			cancel()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
