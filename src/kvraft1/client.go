package kvraft

import (
	crand "crypto/rand"
	"math/big"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	return bigx.Int64()
}

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader int // last successful leader (index into servers[])
	// You can add to this struct.

	// id identifies this Clerk uniquely for the lifetime of the test;
	// seq is a per-Clerk request counter. Together they let the
	// replicated state machine recognize a retransmitted Put -- even
	// after a leader change -- and replay its original outcome instead
	// of re-executing it or leaving the Clerk with an ambiguous
	// ErrMaybe. See server.go's dup table.
	id  int64
	seq int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, id: nrand()}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.

	for {

		var reply rpc.GetReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &rpc.GetArgs{Key: key}, &reply)
		if !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == rpc.OK {
			return reply.Value, reply.Version, rpc.OK

		} else if reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey

		} else if reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

	}

	return "", 0, rpc.ErrNoKey
}

// Put updates key with value only if the version in the request
// matches the version of the key at the server. If the versions
// numbers don't match, the server returns ErrVersion.
//
// Every attempt of this Put -- the original send and any retries after
// a lost request, a lost reply, or a leader change -- carries the same
// (ClientId, Seq) pair. The replicated state machine's dup table (see
// server.go's DoOp) recognizes a retry of a request it already
// committed and replays that original reply verbatim, so Put's result
// is always the true, definitive outcome: it never needs to fall back
// to ErrMaybe.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	ck.seq++
	args := rpc.PutArgs{Key: key, Value: value, Version: version, ClientId: ck.id, Seq: ck.seq}

	for {

		var reply rpc.PutReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == rpc.OK {
			return rpc.OK

		} else if reply.Err == rpc.ErrVersion {
			return rpc.ErrVersion

		} else if reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

	}

	return rpc.ErrNoKey
}
