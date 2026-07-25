package kvraft

import (
	"bytes"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	data map[string]rpc.GetReply // key-value store
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here

	switch r := req.(type) {
	case rpc.GetArgs:
		// look up r.Key in kv.data, return rpc.GetReply
		if val, ok := kv.data[r.Key]; ok {
			return &rpc.GetReply{Value: val.Value, Version: val.Version, Err: rpc.OK}
		} else {
			return &rpc.GetReply{Err: rpc.ErrNoKey}
		}
		

	case rpc.PutArgs:
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
		
	default:
		return nil
	}

}

func (kv *KVServer) Snapshot() []byte {
	// Your code here

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.data)
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

}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = *(rep.(*rpc.GetReply))

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = *(rep.(*rpc.PutReply))

}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, data: make(map[string]rpc.GetReply)}


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
