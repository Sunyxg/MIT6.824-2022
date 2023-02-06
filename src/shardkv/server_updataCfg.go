package shardkv

import "time"

func (kv *ShardKV) updataConfig() {
	for {
		time.Sleep(50 * time.Millisecond)

		kv.mu.Lock()
		if !kv.checkStatus() {
			kv.mu.Unlock()
			continue
		}
		next_cfgnum := kv.cfg.Num + 1
		kv.mu.Unlock()

		new_cfg := kv.ctrc.Query(next_cfgnum)

		kv.mu.Lock()
		// 如果获取到新的cfg
		if new_cfg.Num > kv.cfg.Num {
			cmd := Op{
				Operate: UPDATACFG,
				Cfg:     new_cfg,
			}
			DPrintf("[%v.%v] try Updata Cfg : cfgnum : %v\n", kv.gid, kv.me, new_cfg.Num)
			kv.rf.Start(cmd)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) checkStatus() bool {
	_, isleader := kv.rf.GetState()
	if !isleader {
		return false
	}

	// 判断当前config是否更新完毕
	for _, v := range kv.shardstatus {
		if v == ACQUIRE || v == EXPIRED {
			return false
		}
	}
	return true
}
