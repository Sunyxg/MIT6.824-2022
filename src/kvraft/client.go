package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers     []*labrpc.ClientEnd
	clientId    int64 // 客户端id
	sequenceNum int   // 客户端当前发送命令的序列号
	leaderId    int   // 客户端上一条命令发往的leaderid
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.sequenceNum = 1
	DPrintf("[%v] : create client sussess!!", ck.clientId)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// 获取键key的值，如果key不存在则返回为“ ”,(在出错或成功执行之前一直尝试)
func (ck *Clerk) Get(key string) string {
	// 第一次发送命令，应随机选取leaderId
	args := GetArgs{
		Key:         key,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}

	for {
		var reply GetReply
		// 如果调用RPC发送失败
		// DPrintf("[%v] : send Get RPC to [%v], Get: %v\n", ck.clientId, ck.leaderId, key)
		if !ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply) || reply.Err != "" {
			// DPrintf("[%v] ERROR : %v\n", ck.leaderId, reply.Err)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			// 如果调用RPC成功
			ck.sequenceNum++
			// DPrintf("[%v] Get key success\n", ck.leaderId)
			return reply.Value
		}

	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// 在此首先Put与Append功能,(在出错或成功执行之前一直尝试)
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}

	for {
		var reply PutAppendReply
		// 如果调用RPC发送失败
		// DPrintf("[%v] : send PutAppend RPC to [%v], %v [ %v : %v ] \n", ck.clientId, ck.leaderId, op, key, value)
		if !ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply) || reply.Err != "" {
			// DPrintf("[%v] ERROR : %v\n", ck.leaderId, reply.Err)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			// 如果调用RPC成功
			ck.sequenceNum++
			// DPrintf("[%v] PutAppend success\n", ck.leaderId)
			break
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
