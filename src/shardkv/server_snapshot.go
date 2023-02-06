package shardkv

import (
	"bytes"
	"time"

	"6.824/labgob"
	"6.824/shardctrler"
)

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	// 解析快照
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvdata []map[string]string
	var clientseq []map[int64]int
	var shardstatus []int
	var cfg shardctrler.Config
	if d.Decode(&kvdata) != nil || d.Decode(&clientseq) != nil || d.Decode(&shardstatus) != nil || d.Decode(&cfg) != nil {
		panic("readSnapshot error!!!")
	} else {
		kv.kvdata = kvdata
		kv.clientseq = clientseq
		kv.shardstatus = shardstatus
		kv.cfg = cfg
	}

}

func (kv *ShardKV) Snapshot() {
	if kv.maxraftstate > 0 && kv.rf.RaftStateSize() > kv.maxraftstate*8/10 {
		// 调用raft中的Snapshot()函数进行快照
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.kvdata)
		e.Encode(kv.clientseq)
		e.Encode(kv.shardstatus)
		e.Encode(kv.cfg)
		snapshot := w.Bytes()
		kv.rf.Snapshot(kv.applyidx, snapshot)
		DPrintf("[%v.%v] : Snapshot!, applyidx : %v\n", kv.gid, kv.me, kv.applyidx)
	}
}

func (kv *ShardKV) trySnapshot() {
	for {
		kv.mu.Lock()
		// 当raft持久性保存的state超过maxraftstate字节数时，K/V服务需要进行快照
		kv.Snapshot()
		kv.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}
