package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// callTimeout bounds how long Get/Put will hammer this group's servers
// within a single call before giving up and returning control to the
// caller. Two things are in tension here: the bound must comfortably
// outlast a normal leader election (this implementation's raft.go
// randomizes its election timeout in [300, 600)ms), but it must also
// be short, because the caller (shardkv1.Clerk) re-reads the current
// shard configuration every time it retries -- the shorter this bound,
// the sooner a stale, in-progress call notices that the group it's
// talking to no longer owns the shard (or has been torn down entirely
// after leaving the configuration) instead of blindly committing to
// that group for the whole bound. The caller retries indefinitely on a
// give-up, so a short bound here just means more, cheaper rounds -- it
// doesn't reduce overall patience.
const callTimeout = 500 * time.Millisecond

// moveTimeout bounds FreezeShard/InstallShard/DeleteShard, called only
// by the shardctrler while moving a shard. Unlike Get/Put, a single
// ChangeConfigTo call in Part A/B has no outer retry loop re-checking
// anything if this gives up too early -- moveShards just reports
// failure and the whole reconfiguration silently doesn't commit. So
// this needs to reliably outlast a normal election on the first try,
// with real margin, rather than being tuned to minimize staleness
// exposure the way callTimeout is; the shorter callTimeout already
// covers that job for the group-torn-down case that motivated bounding
// these calls at all (see moveShards in shardctrler.go).
const moveTimeout = 4 * time.Second

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here

	deadline := time.Now().Add(callTimeout)
	for time.Now().Before(deadline) {

		var reply rpc.GetReply
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.Get", &rpc.GetArgs{Key: key}, &reply)
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
		} else if reply.Err == rpc.ErrWrongGroup {
			return "", 0, rpc.ErrWrongGroup
		}

	}

	// Couldn't get a definitive answer from any server in this group
	// within a bounded number of attempts; let the caller re-query the
	// configuration and retry (possibly against a different group).
	return "", 0, rpc.ErrWrongLeader

}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here

	// maybeApplied tracks genuine ambiguity only: a request we sent but
	// never got a reply for (!ok), so we can't tell whether the server
	// received and applied it before we moved on. An explicit
	// ErrWrongLeader reply is NOT ambiguous -- the server is telling us
	// outright that it did not apply the request -- so it must not set
	// this, or we'd claim ErrMaybe (and thus never let the caller retry
	// cleanly) any time a leader election merely happened to occur
	// during the call, which is routine and not a sign anything was
	// actually left in doubt.
	maybeApplied := false
	deadline := time.Now().Add(callTimeout)
	for time.Now().Before(deadline) {

		var reply rpc.PutReply
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.Put", &rpc.PutArgs{Key: key, Value: value, Version: version}, &reply)
		if !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			maybeApplied = true
			continue
		}

		if reply.Err == rpc.OK {
			return rpc.OK

		} else if reply.Err == rpc.ErrVersion {
			if maybeApplied {
				return rpc.ErrMaybe
			}
			return rpc.ErrVersion

		} else if reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		} else if reply.Err == rpc.ErrWrongGroup {
			return rpc.ErrWrongGroup
		}

	}

	// Gave up after the deadline. If any attempt left real ambiguity
	// (maybeApplied == true), the Put may have been applied, so it's
	// unsafe to claim otherwise. Otherwise every reply we got was a
	// clean, definite "not applied" -- safe for the caller to re-query
	// the configuration and retry from scratch.
	if maybeApplied {
		return rpc.ErrMaybe
	}
	return rpc.ErrWrongLeader

}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here

	deadline := time.Now().Add(moveTimeout)
	for time.Now().Before(deadline) {
		var reply shardrpc.FreezeShardReply

		ok := ck.Call(ck.servers[ck.leader], "KVServer.FreezeShard",
			&shardrpc.FreezeShardArgs{Shard: s, Num: num}, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

		return reply.State, reply.Err
	}

	// The group is unreachable (e.g. it has already left the
	// configuration and been torn down). Give up so the caller
	// (shardctrler) can back off instead of blocking forever.
	return nil, rpc.ErrWrongLeader
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here

	deadline := time.Now().Add(moveTimeout)
	for time.Now().Before(deadline) {
		var reply shardrpc.InstallShardReply

		ok := ck.Call(ck.servers[ck.leader], "KVServer.InstallShard",
			&shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

		return reply.Err
	}

	return rpc.ErrWrongLeader
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here

	deadline := time.Now().Add(moveTimeout)
	for time.Now().Before(deadline) {
		var reply shardrpc.DeleteShardReply

		ok := ck.Call(ck.servers[ck.leader], "KVServer.DeleteShard",
			&shardrpc.DeleteShardArgs{Shard: s, Num: num}, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

		return reply.Err
	}

	return rpc.ErrWrongLeader
}
