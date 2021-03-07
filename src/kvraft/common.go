package kvraft

const (
	OK             	= "OK"
	ErrNoKey       	= "ErrNoKey"
	ErrWrongLeader 	= "ErrWrongLeader"
	ErrTimeOut		= "ErrTimeOut"
	ErrWrongOp		= "ErrWrongOp"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   	string
	Value 	string
	Op    	string
	Cid   	int64
	Seq	  	int32
	// "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Cid 	int64
	Seq 	int32
	Err 	Err
}

type GetArgs struct {
	Key 	string
	Cid 	int64
	Seq 	int32

	// You'll have to add definitions here.
}

type GetReply struct {
	Cid 	int64
	Seq 	int32
	Err   	Err
	Value 	string
}
