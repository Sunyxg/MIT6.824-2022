package shardkv

import "time"

func (kv *ShardKV) pushShards() {
	for {
		time.Sleep(100 * time.Millisecond)
		// 只有leader可以进行RPC通信
		_, isleader := kv.rf.GetState()
		if !isleader {
			continue
		}

		kv.mu.Lock()
		for i, statu := range kv.shardstatus {
			if statu == EXPIRED {
				args := GetShardsArgs{
					CfgNum:    kv.cfg.Num,
					ShardNum:  i,
					ClientSeq: make(map[int64]int),
					Datas:     make(map[string]string),
				}
				for k, v := range kv.kvdata[i] {
					args.Datas[k] = v
				}
				for k, v := range kv.clientseq[i] {
					args.ClientSeq[k] = v
				}
				DPrintf("[%v.%v] : Push Shard [%v : %v] to [GID: %v]\n", kv.gid, kv.me, args.CfgNum, i, kv.cfg.Shards[i])
				go kv.pushShardto(args, kv.cfg.Groups[kv.cfg.Shards[i]])
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) pushShardto(args GetShardsArgs, servers []string) {
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			reply := GetShardReply{}
			ok := srv.Call("ShardKV.GetShards", &args, &reply)
			if ok && reply.Err == OK {
				// 更改shardstatus并CG
				kv.mu.Lock()
				op := Op{
					Operate:  RESHARD,
					CfgNum:   args.CfgNum,
					ShardNum: args.ShardNum,
				}
				kv.rf.Start(op)
				kv.mu.Unlock()
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (kv *ShardKV) GetShards(args *GetShardsArgs, reply *GetShardReply) {
	// 接收方cfg滞后
	kv.mu.Lock()
	DPrintf("[%v.%v] : recived GetShards RPC, cfgnum : %v, shardnum : %v\n", kv.gid, kv.me, args.CfgNum, args.ShardNum)
	if kv.cfg.Num < args.CfgNum {
		kv.mu.Unlock()
		reply.Err = ErrWrongCfgnum
		return
	}
	kv.mu.Unlock()

	op := Op{
		Operate:   APSHARD,
		CfgNum:    args.CfgNum,
		ShardNum:  args.ShardNum,
		ShardData: args.Datas,
		ShardSeq:  args.ClientSeq,
	}

	_, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		DPrintf("[%v.%v] : GetShards error : NotLeader\n", kv.gid, kv.me)
		return
	}

	t := time.Now()
	timer := t.Add(500 * time.Millisecond)

	for {
		if time.Now().After(timer) {
			reply.Err = ErrTimeOut
			DPrintf("[%v.%v] : APSHARDRPC error : Timeout\n", kv.gid, kv.me)
			return
		}
		kv.mu.Lock()
		if kv.cfg.Num > args.CfgNum || kv.shardstatus[args.ShardNum] == WORKING {
			kv.mu.Unlock()
			reply.Err = OK
			DPrintf("[%v.%v] : APSHARDRPC Success, ShardNum : %v \n", kv.gid, kv.me, args.ShardNum)
			return
		}
		kv.mu.Unlock()
	}

}
