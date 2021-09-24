package kvraft

import "6.824/raft"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrLateLog     = "ErrLateLog"
	GET            = "Get"
	PUT            = "Put"
	APPEND         = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id       int64
	ClientId int64
}

type PutAppendReply struct {
	Err    Err
	Server int
}

type GetArgs struct {
	Key      string
	Id       int64
	ClientId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err    Err
	Value  string
	Server int
	Msg    raft.ApplyMsg
}
