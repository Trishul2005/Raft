package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardcfg"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks   map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}


// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.

	for {

		shard := shardcfg.Key2Shard(key)
		cfg := ck.sck.Query()
		gid, servers, _ := cfg.GidServers(shard)

		if _, ok := ck.rcks[gid]; !ok {
			ck.rcks[gid] = shardgrp.MakeClerk(ck.clnt, servers)
		}

		val, ver, err := ck.rcks[gid].Get(key)
		if err == rpc.ErrWrongGroup {
			delete(ck.rcks, gid)
			time.Sleep(10 * time.Millisecond)
			continue
		} else if err == rpc.ErrWrongLeader {
			// The shardgrp clerk gave up after a bounded number of
			// attempts (e.g. the group's servers are gone for good
			// because it left the configuration, or it just hasn't
			// elected a leader yet). Re-query the configuration and
			// retry, which will route to whichever group now owns the
			// shard. The short sleep avoids busy-spinning while a
			// brand-new group is still electing its first leader.
			time.Sleep(10 * time.Millisecond)
			continue
		}

		return val, ver, err

	}

}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	for {

		shard := shardcfg.Key2Shard(key)
		cfg := ck.sck.Query()
		gid, servers, _ := cfg.GidServers(shard)

		if _, ok := ck.rcks[gid]; !ok {
			ck.rcks[gid] = shardgrp.MakeClerk(ck.clnt, servers)
		}

		err := ck.rcks[gid].Put(key, value, version)
		if err == rpc.ErrWrongGroup {
			delete(ck.rcks, gid)
			time.Sleep(10 * time.Millisecond)
			continue
		} else if err == rpc.ErrWrongLeader {
			// The shardgrp clerk gave up without any real ambiguity
			// (every reply was a clean "not applied"); safe to
			// re-query the configuration and retry from scratch.
			time.Sleep(10 * time.Millisecond)
			continue
		} else if err == rpc.ErrMaybe {
			// Don't hand ambiguity to the caller if we can resolve it
			// ourselves. Put is a CAS, so reading the key back can
			// prove definitively whether our write landed: if version
			// is still <= the one we attempted, it didn't, and it's
			// safe to retry with the same version; if version is
			// exactly ours+1, either our value is there (we won) or
			// someone else's is (we lost) -- both definite. Only if
			// the version has moved further ahead than that can we no
			// longer tell, since that transition is no longer visible.
			if applied, definite := ck.putLanded(key, value, version); definite {
				if applied {
					return rpc.OK
				}
				continue
			}
			return rpc.ErrMaybe
		}

		return err

	}

}

// putLanded checks whether an ambiguous Put(key, value, version)
// actually took effect, by reading the key back. definite is false
// when the read can't prove it either way (the version has already
// advanced past what a single read can distinguish); callers must
// treat that case as genuinely ambiguous.
func (ck *Clerk) putLanded(key, value string, version rpc.Tversion) (applied bool, definite bool) {
	curVal, curVer, err := ck.Get(key)
	if err == rpc.ErrNoKey {
		// Nothing was ever written at this key, so our put (which
		// would have created it) definitely didn't apply.
		return false, true
	}
	if curVer <= version {
		// The CAS precondition our put needed is still open: it
		// hasn't happened yet.
		return false, true
	}
	if curVer == version+1 {
		// Exactly one write won this transition; it's ours iff the
		// value matches.
		return curVal == value, true
	}
	// The version moved further than we can account for; can't tell.
	return false, false
}
