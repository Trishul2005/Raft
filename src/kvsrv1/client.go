package kvsrv

import (
	crand "crypto/rand"
	"math/big"
	"time"

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
	clnt   *tester.Clnt
	server string

	// id identifies this Clerk uniquely across the lifetime of the
	// test; seq is a per-Clerk request counter. Together they let the
	// server recognize a retransmitted Put and replay its original
	// outcome (see server.go's dup table) instead of re-executing it
	// or leaving the Clerk with an ambiguous ErrMaybe.
	id  int64
	seq int64
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server, id: nrand()}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	for {
		args := rpc.GetArgs{Key: key}
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if !ok {
			continue
		}
		if reply.Err == rpc.OK {
			return reply.Value, reply.Version, rpc.OK
		}
		if reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}
	}

}

// Put updates key with value only if the version in the request
// matches the version of the key at the server. If the versions
// numbers don't match, the server returns ErrVersion.
//
// Every attempt of this Put -- the original send and any retries after
// a lost request or reply -- carries the same (ClientId, Seq) pair. The
// server's dup table (see server.go) recognizes a retry of a request
// it already executed and replays that original reply verbatim, so
// Put's result is always the true, definitive outcome: it never needs
// to fall back to ErrMaybe.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {

	ck.seq++
	args := rpc.PutArgs{Key: key, Value: value, Version: version, ClientId: ck.id, Seq: ck.seq}

	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		return reply.Err
	}

}
