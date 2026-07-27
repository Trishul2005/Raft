package shardgrp

import (
	"bytes"
	"6.5840/shardkv1/shardcfg"
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)


type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	data       map[string]rpc.GetReply
	shardState [shardcfg.NShards]ShardState
	shardNum   [shardcfg.NShards]shardcfg.Tnum // updated by InstallShard and DeleteShard
	frozenNum  [shardcfg.NShards]shardcfg.Tnum // updated by FreezeShard only
}

type ShardState string
const (
	Active ShardState = "Active"
	Frozen ShardState = "Frozen"
	Absent ShardState = "Absent"
)


func (kv *KVServer) DoOp(req any) any {
	// Your code here

	switch r := req.(type) {
	case rpc.GetArgs:

		if kv.shardState[shardcfg.Key2Shard(r.Key)] != Active {
			return &rpc.GetReply{Err: rpc.ErrWrongGroup}
		}

		// look up r.Key in kv.data, return rpc.GetReply
		if val, ok := kv.data[r.Key]; ok {
			return &rpc.GetReply{Value: val.Value, Version: val.Version, Err: rpc.OK}
		} else {
			return &rpc.GetReply{Err: rpc.ErrNoKey}
		}
		

	case rpc.PutArgs:

		if kv.shardState[shardcfg.Key2Shard(r.Key)] != Active {
			return &rpc.PutReply{Err: rpc.ErrWrongGroup}
		}

		// check version, update if matches, return rpc.PutReply
		if val, ok := kv.data[r.Key]; ok {
			if val.Version == r.Version {
				kv.data[r.Key] = rpc.GetReply{Value: r.Value, Version: val.Version + 1, Err: rpc.OK}
				return &rpc.PutReply{Err: rpc.OK}
			} else {
				return &rpc.PutReply{Err: rpc.ErrVersion}
			}
		} else {
			if r.Version == 0 {
				kv.data[r.Key] = rpc.GetReply{Value: r.Value, Version: 1, Err: rpc.OK}
				return &rpc.PutReply{Err: rpc.OK}
			} else {
				return &rpc.PutReply{Err: rpc.ErrVersion}
			}
		}
	
	case shardrpc.FreezeShardArgs:

		if r.Num <= kv.frozenNum[r.Shard] {
			// already frozen: re-extract whatever keys remain for idempotent reply
			shardData := make(map[string]rpc.GetReply)
			for k, v := range kv.data {
				if shardcfg.Key2Shard(k) == r.Shard {
					shardData[k] = v
				}
			}
			w := new(bytes.Buffer)
			labgob.NewEncoder(w).Encode(shardData)
			return &shardrpc.FreezeShardReply{State: w.Bytes(), Num: r.Num, Err: rpc.OK}
		}

		kv.shardState[r.Shard] = Frozen
		shardData := make(map[string]rpc.GetReply)
		for k, v := range kv.data {
			if shardcfg.Key2Shard(k) == r.Shard {
				shardData[k] = v
			}
		}
		w := new(bytes.Buffer)
		labgob.NewEncoder(w).Encode(shardData)
		kv.frozenNum[r.Shard] = r.Num
		return &shardrpc.FreezeShardReply{State: w.Bytes(), Num: r.Num, Err: rpc.OK}

	case shardrpc.InstallShardArgs:

		if r.Num <= kv.shardNum[r.Shard] {
			return &shardrpc.InstallShardReply{Err: rpc.OK}
		}

		var shardData map[string]rpc.GetReply
		labgob.NewDecoder(bytes.NewBuffer(r.State)).Decode(&shardData)

		for k, v := range shardData {
			kv.data[k] = v
		}

		kv.shardState[r.Shard] = Active
		kv.shardNum[r.Shard] = r.Num

		return &shardrpc.InstallShardReply{Err: rpc.OK}

	case shardrpc.DeleteShardArgs:

		if r.Num <= kv.shardNum[r.Shard] {
			return &shardrpc.DeleteShardReply{Err: rpc.OK}
		}

		for k := range kv.data {
			if shardcfg.Key2Shard(k) == r.Shard {
				delete(kv.data, k)
			}
		}

		kv.shardState[r.Shard] = Absent
		kv.shardNum[r.Shard] = r.Num
		kv.rsm.ForceSnapshot()

		return &shardrpc.DeleteShardReply{Err: rpc.OK}

	default:
		return nil
	}

}


func (kv *KVServer) Snapshot() []byte {
	// Your code here

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.data)
	e.Encode(kv.shardState)
	e.Encode(kv.shardNum)
	e.Encode(kv.frozenNum)
	return w.Bytes()

}

func (kv *KVServer) Restore(data []byte) {
	// Your code here

	if data == nil || len(data) == 0 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData map[string]rpc.GetReply
	if d.Decode(&kvData) != nil {
		panic("Failed to decode KVServer data")
	} else {
		kv.data = kvData
	}
	if d.Decode(&kv.shardState) != nil {
		panic("Failed to decode KVServer shardState")
	}
	if d.Decode(&kv.shardNum) != nil {
		panic("Failed to decode KVServer shardNum")
	}
	if d.Decode(&kv.frozenNum) != nil {
		panic("Failed to decode KVServer frozenNum")
	}

}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here

	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = *(rep.(*rpc.GetReply))

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here

	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = *(rep.(*rpc.PutReply))

}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here

    err, rep := kv.rsm.Submit(*args)

    if err == rpc.ErrWrongLeader {
        reply.Err = rpc.ErrWrongLeader
        return
    }

    *reply = *(rep.(*shardrpc.FreezeShardReply))

}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here

    err, rep := kv.rsm.Submit(*args)

    if err == rpc.ErrWrongLeader {
        reply.Err = rpc.ErrWrongLeader
        return
    }

    *reply = *(rep.(*shardrpc.InstallShardReply))

}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here

    err, rep := kv.rsm.Submit(*args)

    if err == rpc.ErrWrongLeader {
        reply.Err = rpc.ErrWrongLeader
        return
    }
	
    *reply = *(rep.(*shardrpc.DeleteShardReply))

}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me, data: make(map[string]rpc.GetReply)}

	if gid == shardcfg.Gid1 {
		for i := 0; i < shardcfg.NShards; i++ {
			kv.shardState[i] = Active
		}
	} else {
		for i := 0; i < shardcfg.NShards; i++ {
			kv.shardState[i] = Absent
		}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
