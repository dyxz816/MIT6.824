package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpId     int64
	ClientId int64
	Index    int
	Term     int
	OpType   string
	OpKey    string
	OpValue  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sequenceMapper map[int64]int64
	requestMapper  map[int]chan Op
	KvStore        map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var isLeader bool
	clientOp := Op{OpType: "Get", OpKey: args.Key, OpId: args.Id, ClientId: args.ClientId}
	clientOp.Index, clientOp.Term, isLeader = kv.rf.Start(clientOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getChannel(clientOp.Index)
	defer func() {
		kv.mu.Lock()
		delete(kv.requestMapper, clientOp.Index)
		kv.mu.Unlock()
	}()
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case op := <-ch:
		kv.mu.Lock()
		opTerm := op.Term
		kv.mu.Unlock()
		if clientOp.Term != opTerm {
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.KvStore[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var isLeader bool
	clientOp := Op{OpType: args.Op, OpKey: args.Key, OpId: args.Id, ClientId: args.ClientId, OpValue: args.Value}
	clientOp.Index, clientOp.Term, isLeader = kv.rf.Start(clientOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getChannel(clientOp.Index)
	defer func() {
		kv.mu.Lock()
		delete(kv.requestMapper, clientOp.Index)
		kv.mu.Unlock()
	}()
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case op := <-ch:
		kv.mu.Lock()
		opTerm := op.Term
		kv.mu.Unlock()
		if clientOp.Term != opTerm {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) getChannel(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.requestMapper[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.requestMapper[index] = ch
	}
	return ch
}
func (kv *KVServer) serverMonitor() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			index := msg.CommandIndex
			term := msg.CommandTerm
			op := msg.Command.(Op)
			kv.mu.Lock()
			sequenceInMapper, hasSequence := kv.sequenceMapper[op.ClientId]
			op.Term = term
			if !hasSequence || op.OpId > sequenceInMapper {
				switch op.OpType {
				case "Put":
					kv.KvStore[op.OpKey] = op.OpValue
				case "Append":
					kv.KvStore[op.OpKey] += op.OpValue
				}
			}
			kv.mu.Unlock()
			kv.getChannel(index) <- op
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
	// Your code here, if desired.
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sequenceMapper = make(map[int64]int64)
	kv.requestMapper = make(map[int]chan Op)
	kv.KvStore = make(map[string]string)
	// You may need initialization code here.
	go kv.serverMonitor()
	return kv
}
