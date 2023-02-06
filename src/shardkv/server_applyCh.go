package shardkv

func (kv *ShardKV) clientoperate(op Op) {
	// 如果此命令之前已经被执行过，则不进行任何操作
	if op.SequenceNum <= kv.clientseq[op.ShardNum][op.ClientId] {
		return
	}

	if kv.shardstatus[op.ShardNum] != WORKING {
		DPrintf("[%v.%v] SHARD %v NOT WORKING !!!!!! shardstatus[%v]", kv.gid, kv.me, op.ShardNum, kv.shardstatus)
		return
	}

	if op.Operate == PUT {
		kv.kvdata[op.ShardNum][op.Key] = op.Value
		// DPrintf("[%v.%v] : applied Put [ %v ]\n", kv.gid, kv.me, op.Key)
	} else if op.Operate == APPEND {
		kv.kvdata[op.ShardNum][op.Key] += op.Value
		// DPrintf("[%v.%v] : applied Append [ %v ]\n", kv.gid, kv.me, op.Key)
	} else if op.Operate == GET {
		// DPrintf("[%v.%v] : applied Get [ %v ]\n", kv.gid, kv.me, op.Key)
	}

	kv.clientseq[op.ShardNum][op.ClientId] = op.SequenceNum
}

func (kv *ShardKV) updatacfg(op Op) {
	// 保证cfg更新的有序性且避免重复操作
	if op.Cfg.Num == kv.cfg.Num+1 {
		oldcfg := kv.cfg
		kv.cfg = op.Cfg
		// 更新shardstatus
		for i := 0; i < len(kv.cfg.Shards); i++ {
			// 有新的shard需要当前服务器负责
			if oldcfg.Shards[i] != kv.gid && kv.cfg.Shards[i] == kv.gid {
				if oldcfg.Shards[i] != 0 {
					kv.shardstatus[i] = ACQUIRE
				} else {
					kv.shardstatus[i] = WORKING
				}
			} else if oldcfg.Shards[i] == kv.gid && kv.cfg.Shards[i] != kv.gid {
				kv.shardstatus[i] = EXPIRED
			}
		}

		DPrintf("[%v.%v] Updata Cfg : cfgnum : %v, shardstatus[%v]\n", kv.gid, kv.me, kv.cfg.Num, kv.shardstatus)
	}
}

func (kv *ShardKV) appendshard(op Op) {
	if kv.cfg.Num == op.CfgNum && kv.shardstatus[op.ShardNum] == ACQUIRE {
		// 更新shard
		kv.kvdata[op.ShardNum] = map[string]string{}
		kv.clientseq[op.ShardNum] = map[int64]int{}
		for k, v := range op.ShardData {
			kv.kvdata[op.ShardNum][k] = v
		}
		for k, v := range op.ShardSeq {
			kv.clientseq[op.ShardNum][k] = v
		}
		kv.shardstatus[op.ShardNum] = WORKING
		DPrintf("[%v.%v] Append Shards : cfgnum : %v, shardstatus[%v]\n", kv.gid, kv.me, kv.cfg.Num, kv.shardstatus)
	}
}

func (kv *ShardKV) releaseshard(op Op) {
	if kv.cfg.Num == op.CfgNum && kv.shardstatus[op.ShardNum] == EXPIRED {
		kv.kvdata[op.ShardNum] = map[string]string{}
		kv.clientseq[op.ShardNum] = map[int64]int{}
		kv.shardstatus[op.ShardNum] = INVALID
		DPrintf("[%v.%v] Release Shards : cfgnum : %v, shardstatus[%v]\n", kv.gid, kv.me, kv.cfg.Num, kv.shardstatus)
	}
}

// 从applyCh中读取raft发送来的日志条目
func (kv *ShardKV) readApplyCh() {
	// 从applyCh中读取ApplyMsg
	for msg := range kv.applyCh {
		kv.mu.Lock()
		if msg.CommandValid {
			if msg.CommandIndex > kv.applyidx {
				//unmarshal为Op struct
				op := msg.Command.(Op)
				switch op.Operate {
				case GET, PUT, APPEND:
					kv.clientoperate(op)
				case UPDATACFG:
					kv.updatacfg(op)
				case APSHARD:
					kv.appendshard(op)
				case RESHARD:
					kv.releaseshard(op)
				default:
					panic("applied wrong command")
				}
				kv.applyidx = msg.CommandIndex
			}
		} else if msg.SnapshotValid {
			kv.readSnapshot(msg.Snapshot)
			kv.applyidx = msg.SnapshotIndex
			DPrintf("[%v] : received Snapshot!", kv.me)
		}
		kv.mu.Unlock()
	}

}
