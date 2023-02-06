package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongCfgnum = "ErrWrongCfgnum"
	ErrTimeOut     = "ErrTimeOut"
)

const (
	GET       = "Get"
	PUT       = "Put"
	APPEND    = "Append"
	UPDATACFG = "UpdataCfg"
	APSHARD   = "AppendShard"
	RESHARD   = "ReleaseShard"
)

const (
	INVALID = 0
	WORKING = 1
	EXPIRED = 2
	ACQUIRE = 3
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key         string
	Value       string
	Op          string // "Put" or "Append"
	ClientId    int64  // 客户端id
	SequenceNum int    // 客户端发送命令的序列号
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key         string
	ClientId    int64 // 客户端id
	SequenceNum int   // 客户端发送命令的序列号
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardsArgs struct {
	CfgNum    int
	ShardNum  int
	Datas     map[string]string
	ClientSeq map[int64]int
}

type GetShardReply struct {
	Err Err
}
