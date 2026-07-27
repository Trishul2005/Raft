package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


// dupEntry records the most recent Put a client has issued, so a
// retransmission of that same request can be answered without
// re-executing it. Callers of Put issue requests one at a time (a
// Clerk never has two Puts in flight simultaneously), so remembering
// only the latest sequence number per client is enough: any incoming
// Seq older than what's recorded here must already be resolved as far
// as that client is concerned.
type dupEntry struct {
	seq   int64
	reply rpc.PutReply
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	values   map[string]string
	versions map[string]rpc.Tversion
	dup      map[int64]dupEntry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		values:   make(map[string]string),
		versions: make(map[string]rpc.Tversion),
		dup:      make(map[int64]dupEntry),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key

	if value, ok := kv.values[key]; ok {
		reply.Value = value
		reply.Version = kv.versions[key]
		reply.Err = rpc.OK

	} else {
		reply.Err = rpc.ErrNoKey
	}

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if d, ok := kv.dup[args.ClientId]; ok && args.Seq <= d.seq {
		// Retransmission of a request we've already executed (or an
		// even older one that no longer matters to this client):
		// replay the recorded outcome instead of re-executing.
		*reply = d.reply
		return
	}
	defer func() { kv.dup[args.ClientId] = dupEntry{seq: args.Seq, reply: *reply} }()

	key := args.Key

	// get current version of the key, if it exists
	if currentVersion, ok := kv.versions[key]; ok {

		// version matches
		if args.Version == currentVersion {
			kv.values[key] = args.Value
			kv.versions[key] = args.Version + 1
			reply.Err = rpc.OK

		} else { // version doesn't match
			reply.Err = rpc.ErrVersion
		}
	
	} else if args.Version == 0 { // key doesn't exist and args.Version is 0 
		kv.values[key] = args.Value
		kv.versions[key] = 1
		reply.Err = rpc.OK

	} else { // key doesn't exist and args.Version is not 0
		reply.Err = rpc.ErrNoKey
	}

}



// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
