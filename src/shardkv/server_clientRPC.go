package shardkv

import "time"

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shard := key2shard(args.Key)
	kv.mu.Lock()
	DPrintf("[%v.%v] : Recived GetRPC, clientId : %d, seq : %d, Shard : %d, key : %v \n", kv.gid, kv.me, args.ClientId, args.SequenceNum, shard, args.Key)

	// 如果key不属于当前Gid负责的分片
	if kv.gid != kv.cfg.Shards[shard] || kv.shardstatus[shard] != WORKING {
		kv.mu.Unlock()
		DPrintf("[%v.%v] : GetRPC error : WrongGroup key : %v \n", kv.gid, kv.me, args.Key)
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	cmd := Op{
		Operate:     GET,
		Key:         args.Key,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		ShardNum:    shard,
	}

	_, _, ok := kv.rf.Start(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		DPrintf("[%v.%v] : GetRPC error : NotLeader key : %v \n", kv.gid, kv.me, args.Key)
		return
	}

	t := time.Now()
	timer := t.Add(time.Second)

	for {
		if time.Now().After(timer) {
			reply.Err = ErrTimeOut
			DPrintf("[%v.%v] : GetRPC error : Timeout key : %v \n", kv.gid, kv.me, args.Key)
			return
		}
		kv.mu.Lock()
		if kv.clientseq[shard][args.ClientId] >= args.SequenceNum {
			reply.Value = kv.kvdata[shard][args.Key]
			kv.mu.Unlock()
			reply.Err = OK
			DPrintf("[%v.%v] : GetRPC Success clientId : %d seq : %d,key : %s \n", kv.gid, kv.me, args.ClientId, args.SequenceNum, args.Key)
			return
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shard := key2shard(args.Key)
	kv.mu.Lock()
	DPrintf("[%v.%v] : Recived AppendRPC, clientId : %d, seq : %d, Shard : %d, %v : %v , Op : %s\n", kv.gid, kv.me, args.ClientId,
		args.SequenceNum, shard, args.Key, args.Value, args.Op)

	// 如果key不属于当前Gid负责的分片
	if kv.shardstatus[shard] != WORKING || kv.gid != kv.cfg.Shards[shard] {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	cmd := Op{
		Operate:     args.Op,
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		ShardNum:    shard,
	}

	_, _, ok := kv.rf.Start(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		DPrintf("[%v.%v] : AppendRPC error : NotLeader key : %v  \n", kv.gid, kv.me, args.Key)
		return
	}

	t := time.Now()
	timer := t.Add(time.Second)

	for {
		if time.Now().After(timer) {
			reply.Err = ErrTimeOut
			DPrintf("[%v.%v] : AppendRPC error : NotLeader key : %v \n", kv.gid, kv.me, args.Key)
			return
		}

		kv.mu.Lock()
		if kv.clientseq[shard][args.ClientId] >= args.SequenceNum {
			kv.mu.Unlock()
			reply.Err = OK
			DPrintf("[%v.%v] : AppendRPC Success clientId : %d,seq : %d,key : %s, val : %s, Op : %s\n", kv.gid, kv.me, args.ClientId, args.SequenceNum, args.Key, args.Value, args.Op)
			return
		}
		kv.mu.Unlock()
	}
}
