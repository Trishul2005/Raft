package lock

import (
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key string
	id string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck,
		key:   lockname,
		id:    kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {

	for {

		value, version, err := lk.ck.Get(lk.key)

		// key doesn't exist
		if err == rpc.ErrNoKey {

			// try to acquire lock
			putReply := lk.ck.Put(lk.key, lk.id, 0)
			if putReply == rpc.OK {
				// acquired lock
				return
			}

		} else if err == rpc.OK { // key exists

			// check if we own the lock
			if value == lk.id {
				return

			} else if value == "" { // check if lock is free
				// try to acquire lock
				putReply := lk.ck.Put(lk.key, lk.id, version)
				if putReply == rpc.OK {
					return
				}
			}
		}
		// wait and retry

	}
	
}

func (lk *Lock) Release() {

	for {

		value, version, err := lk.ck.Get(lk.key)

		// key exists
		if err == rpc.OK {

			// check if we own the lock
			if value == lk.id {
				// try to release it
				putReply := lk.ck.Put(lk.key, "", version)

				if putReply == rpc.OK {
					return
				} else if putReply == rpc.ErrVersion {
					// version mismatch, retry
					continue
				}

			} else {
				// don't own the lock, can't release it
				return
			}
		} else if err == rpc.ErrNoKey {
			// key doesn't exist, lock is already released
			return
		}
	}

}
