package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

const key = "config"
const nextKey = "nextconfig"


// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {

	valCurr, _, _ := sck.IKVClerk.Get(key)
	valNext, _, _ := sck.IKVClerk.Get(nextKey)

	// If both keys are empty, this is the first time the controller is
	// starting, so nothing to recover.
	if valCurr == "" && valNext == "" {
		return
	}

	currCfg := shardcfg.FromString(valCurr)
	nextCfg := shardcfg.FromString(valNext)

	if nextCfg.Num > currCfg.Num {
		// The controller was in the middle of a configuration change
		// when it crashed. Finish the change by moving shards from
		// currCfg to nextCfg, and then persisting nextCfg as the
		// current configuration.
		if sck.moveShards(currCfg, nextCfg) {
			sck.commitConfig(nextCfg)
		}
	}

}

// proposeNext tries to install new as the pending "next" configuration
// for new.Num. If another controller has already posted a
// configuration for new.Num (or a later one), that controller's
// proposal wins: proposeNext gives up on new and returns whichever
// configuration is now durably stored under nextKey instead, so the
// caller can adopt it. Only one racing controller's config is ever
// used to move shards and become the committed config for a given Num.
func (sck *ShardCtrler) proposeNext(new *shardcfg.ShardConfig) *shardcfg.ShardConfig {
	for {
		val, ver, _ := sck.IKVClerk.Get(nextKey)
		stored := shardcfg.FromString(val)
		if stored.Num >= new.Num {
			return stored
		}
		if err := sck.IKVClerk.Put(nextKey, new.String(), ver); err == rpc.OK {
			return new
		}
		// lost the version race; re-read and check again
	}
}

// commitConfig writes cfg under key, but only if key doesn't already
// hold cfg.Num or a later one. This guards against a slow/superseded
// controller overwriting progress that another controller already
// committed.
func (sck *ShardCtrler) commitConfig(cfg *shardcfg.ShardConfig) {
	for {
		val, ver, _ := sck.IKVClerk.Get(key)
		stored := shardcfg.FromString(val)
		if stored.Num >= cfg.Num {
			return
		}
		if err := sck.IKVClerk.Put(key, cfg.String(), ver); err == rpc.OK {
			return
		}
		// lost the version race; re-read and check again
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here

	sck.IKVClerk.Put(key, cfg.String(), 0)
	sck.IKVClerk.Put(nextKey, cfg.String(), 0)

}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.

	// Persist new under nextconfig, so a successor can resume this
	// reconfiguration if we crash before finishing it. If another
	// controller already claimed new.Num with a different config,
	// adopt theirs instead of ours.
	agreed := sck.proposeNext(new)

	// read current config from config
	val, _, _ := sck.IKVClerk.Get(key)
	oldcfg := shardcfg.FromString(val)

	// commit: agreed becomes the current config, but only if every
	// shard's move actually completed.
	if sck.moveShards(oldcfg, agreed) {
		sck.commitConfig(agreed)
	}

}

// helper function to move shards from old config to new config.
// Returns false if any shard's move couldn't be completed (e.g. a
// group is unreachable because a different, already-committed
// transition already tore it down). Callers must not commit new as
// the current config when this returns false: leaving it pending lets
// a later retry, with a freshly re-read oldcfg, pick up where this one
// left off -- shards already moved are safe to redo (fenced by Num at
// the shardgrp), and shards whose source/dest is now gone will simply
// no longer show up as needing a move.
func (sck *ShardCtrler) moveShards(oldcfg, new *shardcfg.ShardConfig) bool {

	ok := true

	for i := 0; i < shardcfg.NShards; i++ {

		shard := shardcfg.Tshid(i)
		srcGid := oldcfg.Shards[i]
		dstGid := new.Shards[i]

		if srcGid == dstGid {
			continue
		}

		srcClerk := shardgrp.MakeClerk(sck.clnt, oldcfg.Groups[srcGid])
		dstClerk := shardgrp.MakeClerk(sck.clnt, new.Groups[dstGid])

		state, err := srcClerk.FreezeShard(shard, new.Num)
		if err != rpc.OK {
			ok = false
			continue
		}
		if err := dstClerk.InstallShard(shard, state, new.Num); err != rpc.OK {
			ok = false
			continue
		}
		srcClerk.DeleteShard(shard, new.Num)

	}

	return ok
}


// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.

	val, _, _ := sck.IKVClerk.Get(key)
	if val != "" {
		return shardcfg.FromString(val)
	}

	return nil
}

