package kvraft

import (
	"bytes"
	"log"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"github.com/sasha-s/go-deadlock"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 客户端操作描述结构体
type Op struct {
	Operate     string
	Key         string
	Value       string
	ClientId    int64
	SequenceNum int
}

type KVServer struct {
	mu           deadlock.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big

	kvdata    map[string]string
	clientseq map[int64]int
	applyidx  int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[%v] : Recived GetRPC, key : %v \n", kv.me, args.Key)
	cmd := Op{
		Operate:     GET,
		Key:         args.Key,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	_, _, ok := kv.rf.Start(cmd)
	if !ok {
		reply.Err = "NOTLEADER"
		DPrintf("[%v] : GetRPC error : NotLeader\n", kv.me)
		return
	}

	t := time.Now()
	timer := t.Add(time.Second)

	for {
		if time.Now().After(timer) {
			reply.Err = "TIMEOUT"
			DPrintf("[%v] : GetRPC error : Timeout\n", kv.me)
			return
		}
		kv.mu.Lock()
		if kv.clientseq[args.ClientId] >= args.SequenceNum {
			reply.Value = kv.kvdata[args.Key]
			DPrintf("[%v] : GetRPC Success \n", kv.me)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[%v] : Recived GetRPC, %v : %v \n", kv.me, args.Key, args.Value)
	cmd := Op{
		Operate:     args.Op,
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	_, _, ok := kv.rf.Start(cmd)
	if !ok {
		reply.Err = "NOTLEADER"
		DPrintf("[%v] : AppendRPC error : NotLeader\n", kv.me)
		return
	}

	t := time.Now()
	timer := t.Add(time.Second)

	for {
		if time.Now().After(timer) {
			reply.Err = "TIMEOUT"
			DPrintf("[%v] : AppendRPC error : NotLeader\n", kv.me)
			return
		}

		kv.mu.Lock()
		if kv.clientseq[args.ClientId] >= args.SequenceNum {
			DPrintf("[%v] : AppendRPC Success \n", kv.me)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
// 当一个KVServer不被需要后，测试者会调用Kill()
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

// killed()用来检查当前KVServer是否已经被杀死
func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 从applyCh中读取raft发送来的日志条目
func (kv *KVServer) readApplyCh() {
	// 从applyCh中读取ApplyMsg
	for msg := range kv.applyCh {
		kv.mu.Lock()
		if kv.killed() {
			kv.mu.Unlock()
			return
		}
		if msg.CommandValid {
			if msg.CommandIndex > kv.applyidx {
				// unmarshal为Op struct
				op := msg.Command.(Op)
				// 如果此命令之前已经被执行过，则不进行任何操作
				if op.SequenceNum > kv.clientseq[op.ClientId] {
					switch op.Operate {
					case GET:
						DPrintf("[%v] applied Get [ %v ]", kv.me, op.Key)
					case PUT:
						kv.kvdata[op.Key] = op.Value
						DPrintf("[%v] applied Put [ %v ]", kv.me, op.Key)
					case APPEND:
						kv.kvdata[op.Key] += op.Value
						DPrintf("[%v] applied Append [ %v ]", kv.me, op.Key)
					default:
						panic("applied wrong command")
					}
					kv.clientseq[op.ClientId] = op.SequenceNum
				}
				kv.applyidx++
			}
		} else if msg.SnapshotValid {
			kv.readSnapshot(msg.Snapshot)
			kv.applyidx = msg.SnapshotIndex
			DPrintf("[%v] : received Snapshot!", kv.me)
		}
		kv.mu.Unlock()
	}

}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	// 解析快照
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvdata map[string]string
	var clientseq map[int64]int
	if d.Decode(&kvdata) != nil || d.Decode(&clientseq) != nil {
		panic("readSnapshot error!!!")
	} else {
		kv.kvdata = kvdata
		kv.clientseq = clientseq
	}

}

func (kv *KVServer) Snapshot() {
	if kv.maxraftstate > 0 && kv.rf.RaftStateSize() > kv.maxraftstate*8/10 {
		// 调用raft中的Snapshot()函数进行快照
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.kvdata)
		e.Encode(kv.clientseq)
		snapshot := w.Bytes()
		kv.rf.Snapshot(kv.applyidx, snapshot)
		DPrintf("[%v] : Snapshot!, applyidx : %v", kv.me, kv.applyidx)
	}
}

func (kv *KVServer) trySnapshot() {
	for kv.killed() == false {
		kv.mu.Lock()
		// 当raft持久性保存的state超过maxraftstate字节数时，K/V服务需要进行快照
		kv.Snapshot()
		kv.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

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
// servers[]中包含一组服务器端口，这组服务器通过Raft协作提供容错的key/value服务
// me是servers[]中当前服务器的索引，K/V服务器通过底层的Raft实现快照的存储
// 当Raft保存的状态超过maxraftstate字节数时，K/V服务需要进行快照，以允许Raft对其日志进行垃圾收集
// 如果maxraftstate为-1,则不考虑快照功能
// StartKVServer()应迅速返回，因此使用gorottines处理需要长时间执行的工作
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.applyidx = 0
	kv.kvdata = make(map[string]string, 0)
	kv.clientseq = make(map[int64]int, 0)

	// DPrintf("[%v] : kvserver is building, number of server is : %v", me, len(servers))

	// 解析快照
	kv.readSnapshot(persister.ReadSnapshot())
	kv.applyidx = kv.rf.LastIncludedIndex
	DPrintf("[%v] : kvserver is building, applyidx : %v logs : %v", me, kv.applyidx, kv.rf.Logs)

	go kv.readApplyCh()
	go kv.trySnapshot()

	return kv
}
