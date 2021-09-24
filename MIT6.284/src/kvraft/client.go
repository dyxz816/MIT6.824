package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me        int64
	leader    int
	requestid int64
	mu        sync.Mutex
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
	// You'll have to add code here.
	ck.me = nrand()
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

	// You will have to modify this function.
	ck.requestid++
	args := GetArgs{Key: key, Id: ck.requestid, ClientId: ck.me}
	reply := GetReply{}
	ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
	if ok && len(reply.Err) == 0 {
		return reply.Value
	}
	for {
		for i := range ck.servers {
			reply = GetReply{}
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok && len(reply.Err) == 0 {
				ck.leader = i
				return reply.Value
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
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
	ck.requestid++
	args := PutAppendArgs{Key: key, Value: value, Op: op, Id: ck.requestid, ClientId: ck.me}
	reply := PutAppendReply{}
	ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
	if ok && len(reply.Err) == 0 {
		return
	}
	for {
		for i, _ := range ck.servers {
			reply := PutAppendReply{}
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok && len(reply.Err) == 0 {
				ck.leader = i
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
