package shardctrler

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	JOIN  = "join"
	LEAVE = "leave"
	MOVE  = "move"
	QUERY = "query"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu          sync.Mutex
	me          int
	rf          *raft.Raft
	dead        int32
	applyCh     chan raft.ApplyMsg
	configs     []Config // indexed by config num
	clientseq   map[int64]int
	queryBuffer map[int64]Config
	applyidx    int
}

type Op struct {
	Operate     string
	ClientId    int64
	SequenceNum int
	// Join
	Servers map[int][]string
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
	// Query
	Num int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	cmd := Op{
		Operate:     JOIN,
		Servers:     args.Servers,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	reply.Finish = false
	_, _, ok := sc.rf.Start(cmd)
	if !ok {
		reply.Err = "NOTLEADER"
		DPrintf("[%v] : JoinRPC error : NotLeader\n", sc.me)
		return
	}

	t := time.Now()
	timer := t.Add(time.Second)

	for {
		if time.Now().After(timer) {
			reply.Err = "TIMEOUT"
			DPrintf("[%v] : JoinRPC error : Timeout\n", sc.me)
			return
		}
		sc.mu.Lock()
		if sc.clientseq[args.ClientId] >= args.SequenceNum {
			reply.Finish = true
			DPrintf("[%v] : JoinRPC Success \n", sc.me)
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	cmd := Op{
		Operate:     LEAVE,
		GIDs:        args.GIDs,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	reply.Finish = false
	_, _, ok := sc.rf.Start(cmd)
	if !ok {
		reply.Err = "NOTLEADER"
		DPrintf("[%v] : LeaveRPC error : NotLeader\n", sc.me)
		return
	}

	t := time.Now()
	timer := t.Add(time.Second)

	for {
		if time.Now().After(timer) {
			reply.Err = "TIMEOUT"
			DPrintf("[%v] : LeaveRPC error : Timeout\n", sc.me)
			return
		}
		sc.mu.Lock()
		if sc.clientseq[args.ClientId] >= args.SequenceNum {
			reply.Finish = true
			DPrintf("[%v] : LeaveRPC Success \n", sc.me)
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	cmd := Op{
		Operate:     MOVE,
		Shard:       args.Shard,
		GID:         args.GID,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	reply.Finish = false
	_, _, ok := sc.rf.Start(cmd)
	if !ok {
		reply.Err = "NOTLEADER"
		DPrintf("[%v] : MoveRPC error : NotLeader\n", sc.me)
		return
	}

	t := time.Now()
	timer := t.Add(time.Second)

	for {
		if time.Now().After(timer) {
			reply.Err = "TIMEOUT"
			DPrintf("[%v] : MoveRPC error : Timeout\n", sc.me)
			return
		}
		sc.mu.Lock()
		if sc.clientseq[args.ClientId] >= args.SequenceNum {
			reply.Finish = true
			DPrintf("[%v] : MoveRPC Success \n", sc.me)
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	cmd := Op{
		Operate:     QUERY,
		Num:         args.Num,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	reply.Finish = false
	_, _, ok := sc.rf.Start(cmd)
	if !ok {
		reply.Err = "NOTLEADER"
		DPrintf("[%v] : QueryRPC error : NotLeader\n", sc.me)
		return
	}

	t := time.Now()
	timer := t.Add(time.Second)

	for {
		if time.Now().After(timer) {
			reply.Err = "TIMEOUT"
			DPrintf("[%v] : QueryRPC error : Timeout\n", sc.me)
			return
		}
		sc.mu.Lock()
		if sc.clientseq[args.ClientId] >= args.SequenceNum {
			reply.Config = sc.queryBuffer[args.ClientId]
			reply.Finish = true
			DPrintf("[%v] : QueryRPC Success \n", sc.me)
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
	}

}

// 从applyCh中读取raft发送来的日志条目
func (sc *ShardCtrler) readApplyCh() {
	// 从applyCh中读取ApplyMsg
	for msg := range sc.applyCh {
		sc.mu.Lock()
		if sc.killed() {
			sc.mu.Unlock()
			return
		}
		if msg.CommandValid {
			if msg.CommandIndex > sc.applyidx {
				// unmarshal为Op struct
				op := msg.Command.(Op)
				// 如果此命令之前已经被执行过，则不进行任何操作
				if op.SequenceNum > sc.clientseq[op.ClientId] {
					switch op.Operate {
					case JOIN:
						sc.joinoperate(op.Servers)
					case LEAVE:
						sc.leaveoperate(op.GIDs)
					case MOVE:
						sc.moveoperate(op.Shard, op.GID)
					case QUERY:
						sc.queryoperate(op.Num, op.ClientId)
					default:
						panic("applied wrong operation")
					}
					sc.clientseq[op.ClientId] = op.SequenceNum
				}
				sc.applyidx = msg.CommandIndex
			}

		}
		sc.mu.Unlock()
	}

}

// 负载均衡待优化
func shardbalance(cfg *Config) {
	if len(cfg.Groups) == 0 {
		for i := 0; i < len(cfg.Shards); i++ {
			cfg.Shards[i] = 0
		}
		return
	}

	gids := make([]int, 0)
	for gid, _ := range cfg.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	for i := 0; i < len(cfg.Shards); i++ {
		idx := i % len(gids)
		cfg.Shards[i] = gids[idx]
	}

}

func (sc *ShardCtrler) joinoperate(servers map[int][]string) {
	nidx := len(sc.configs)
	newcfg := Config{}
	newcfg.Num = nidx
	groups := make(map[int][]string, 0)

	// 复制old config中的Groups
	for k, v := range sc.configs[nidx-1].Groups {
		groups[k] = v
	}

	// 添加new Groups
	for k, v := range servers {
		groups[k] = v
	}

	newcfg.Groups = groups
	shardbalance(&newcfg)
	sc.configs = append(sc.configs, newcfg)
}

func (sc *ShardCtrler) leaveoperate(gids []int) {
	nidx := len(sc.configs)
	newcfg := Config{}
	newcfg.Num = nidx
	groups := make(map[int][]string, 0)

	// 复制old config中的Groups
	for k, v := range sc.configs[nidx-1].Groups {
		groups[k] = v
	}

	for _, gid := range gids {
		delete(groups, gid)
	}

	newcfg.Groups = groups
	shardbalance(&newcfg)
	sc.configs = append(sc.configs, newcfg)
}

func (sc *ShardCtrler) moveoperate(shard int, gid int) {
	nidx := len(sc.configs)
	newcfg := Config{}
	newcfg.Num = nidx
	groups := make(map[int][]string, 0)

	// 复制old config中的Groups
	for k, v := range sc.configs[nidx-1].Groups {
		groups[k] = v
	}
	newcfg.Groups = groups

	for i := 0; i < len(newcfg.Shards); i++ {
		newcfg.Shards[i] = sc.configs[nidx-1].Shards[i]
	}
	newcfg.Shards[shard] = gid

	sc.configs = append(sc.configs, newcfg)
}

func (sc *ShardCtrler) queryoperate(num int, cid int64) {
	nidx := len(sc.configs)
	if num == -1 || num >= nidx {
		sc.queryBuffer[cid] = sc.configs[nidx-1]
		return
	}

	sc.queryBuffer[cid] = sc.configs[num]
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
// 当一个ShardCtrler不被需要后，测试者会调用Kill()
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

// killed()用来检查当前ShardCtrler是否已经被杀死
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.clientseq = make(map[int64]int, 0)
	sc.queryBuffer = make(map[int64]Config, 0)
	sc.applyidx = 0

	go sc.readApplyCh()

	return sc
}
