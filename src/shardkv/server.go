package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"github.com/sasha-s/go-deadlock"
)

// 客户端操作描述结构体
type Op struct {
	Operate     string
	Key         string
	Value       string
	Cfg         shardctrler.Config
	ClientId    int64
	SequenceNum int
	CfgNum      int
	ShardNum    int
	ShardData   map[string]string
	ShardSeq    map[int64]int
}

type ShardKV struct {
	mu           deadlock.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	ctrc        *shardctrler.Clerk
	cfg         shardctrler.Config
	kvdata      []map[string]string // 将数据以分片为单位分开存储
	clientseq   []map[int64]int     // 将clientseq以分片为单位分开存储
	shardstatus []int               // 当前Group负责管理的切片
	applyidx    int
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
// servers[] 包含该组服务器的端口，me为servers[]中当前server的索引
// GID为本组的groupid，用于与shardctrler进行交互
// 将ctrlers[]传递给shardctrler.MakeClerk()，这样就可以向shardctrler发送RPC
// make_end(servername) 将 Config.Groups[gid][i] 中的服务器名称变成 labrpc.ClientEnd
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.ctrc = shardctrler.MakeClerk(kv.ctrlers)
	kv.cfg = shardctrler.Config{}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.applyidx = 0
	kv.shardstatus = make([]int, shardctrler.NShards)
	kv.kvdata = make([]map[string]string, shardctrler.NShards)
	kv.clientseq = make([]map[int64]int, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.kvdata[i] = make(map[string]string)
		kv.clientseq[i] = make(map[int64]int)
	}

	// 解析快照
	kv.readSnapshot(persister.ReadSnapshot())
	kv.applyidx = kv.rf.LastIncludedIndex
	DPrintf("[%v.%v] : kvserver is building, applyidx : %v logs : %v", kv.gid, kv.me, kv.applyidx, kv.rf.Logs)

	// 创建协程
	go kv.updataConfig()
	go kv.readApplyCh()
	go kv.pushShards()
	go kv.trySnapshot()

	return kv
}
