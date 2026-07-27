package kvsrv

import (
	//"log"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

// Test Put with a single client and a reliable network
func TestReliablePut(t *testing.T) {
	const Val = "6.5840"
	const Ver = 0

	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("One client and reliable Put")

	ck := ts.MakeClerk()
	if err := ck.Put("k", Val, Ver); err != rpc.OK {
		t.Fatalf("Put err %v", err)
	}

	if val, ver, err := ck.Get("k"); err != rpc.OK {
		t.Fatalf("Get err %v; expected OK", err)
	} else if val != Val {
		t.Fatalf("Get value err %v; expected %v", val, Val)
	} else if ver != Ver+1 {
		t.Fatalf("Get wrong version %v; expected %v", ver, Ver+1)
	}

	if err := ck.Put("k", Val, 0); err != rpc.ErrVersion {
		t.Fatalf("expected Put to fail with ErrVersion; got err=%v", err)
	}

	if err := ck.Put("y", Val, rpc.Tversion(1)); err != rpc.ErrNoKey {
		t.Fatalf("expected Put to fail with ErrNoKey; got err=%v", err)
	}

	if _, _, err := ck.Get("y"); err != rpc.ErrNoKey {
		t.Fatalf("expected Get to fail with ErrNoKey; got err=%v", err)
	}
}

// Many clients putting on same key.
func TestPutConcurrentReliable(t *testing.T) {
	const (
		PORCUPINETIME = 10 * time.Second
		NCLNT         = 10
		NSEC          = 1
	)

	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("Test: many clients racing to put values to the same key")

	rs := ts.SpawnClientsAndWait(NCLNT, NSEC*time.Second, func(me int, ck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
		return ts.OneClientPut(me, ck, []string{"k"}, done)
	})
	ck := ts.MakeClerk()
	ts.CheckPutConcurrent(ck, "k", rs, &kvtest.ClntRes{}, ts.IsReliable())
	ts.CheckPorcupineT(PORCUPINETIME)
}

// Check if memory used on server is reasonable
func TestMemPutManyClientsReliable(t *testing.T) {
	const (
		NCLIENT = 20_000
		MEM     = 1000
	)

	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	v := kvtest.RandValue(MEM)

	cks := make([]kvtest.IKVClerk, NCLIENT)
	for i, _ := range cks {
		cks[i] = ts.MakeClerk()
	}

	// force allocation of ends for server in each client
	for i := 0; i < NCLIENT; i++ {
		if err := cks[i].Put("k", "", 1); err != rpc.ErrNoKey {
			t.Fatalf("Put failed %v", err)
		}
	}

	ts.Begin("Test: memory use many put clients")

	// allow threads started by labrpc to start
	time.Sleep(1 * time.Second)

	m0 := ts.Config.Group(0).MemSize()

	for i := 0; i < NCLIENT; i++ {
		if err := cks[i].Put("k", v, rpc.Tversion(i)); err != rpc.OK {
			t.Fatalf("Put failed %v", err)
		}
	}

	m1 := ts.Config.Group(0).MemSize()
	f := (float64(m1) - float64(m0)) / NCLIENT
	if m1 > m0+(NCLIENT*10) {
		t.Fatalf("error: server using too much memory %d %d (%.2f byte per client)\n", m0, m1, f)
	}
}

// Test with one client and an unreliable network. Before exactly-once
// semantics, a dropped reply left Clerk.Put unable to tell whether its
// request had been applied, so it returned the ambiguous ErrMaybe and
// the caller had to retry and disambiguate via a follow-up Get. With
// server-side dup detection (see server.go), the server itself
// recognizes a retransmitted request and replays its original outcome,
// so Put always returns a single, definitive answer -- it should never
// return ErrMaybe -- no matter how many times the underlying RPC had
// to be retried after a lost request or a lost reply.
func TestUnreliableNet(t *testing.T) {
	const NTRY = 100

	ts := MakeTestKV(t, false)
	defer ts.Cleanup()

	ts.Begin("One client")

	ck := ts.MakeClerk()

	for try := 0; try < NTRY; try++ {
		if err := ts.PutJson(ck, "k", try, rpc.Tversion(try), 0); err != rpc.OK {
			t.Fatalf("Put err %v; expected OK (exactly-once semantics should never surface ErrMaybe)", err)
		}
		v := 0
		if ver := ts.GetJson(ck, "k", 0, &v); ver != rpc.Tversion(try+1) {
			t.Fatalf("Wrong version %d expect %d", ver, try+1)
		}
		if v != try {
			t.Fatalf("Wrong value %d expect %d", v, try)
		}
	}

	ts.CheckPorcupine()
}

// Directly exercises server-side dup detection: sending the exact same
// PutArgs (same ClientId/Seq) twice -- simulating a client retrying
// because it never saw the first reply -- must apply the Put only
// once, and both RPCs must return the identical, definitive reply.
func TestExactlyOnce(t *testing.T) {
	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("Retransmitted Put is applied exactly once")

	clnt := ts.Config.MakeClient()
	defer ts.DeleteClient(clnt)
	server := tester.ServerName(tester.GRP0, 0)

	args := rpc.PutArgs{Key: "k", Value: "v1", Version: 0, ClientId: 12345, Seq: 1}

	var r1 rpc.PutReply
	if ok := clnt.Call(server, "KVServer.Put", &args, &r1); !ok {
		t.Fatalf("first Put RPC failed")
	}
	if r1.Err != rpc.OK {
		t.Fatalf("first Put err %v; expected OK", r1.Err)
	}

	// Simulate the reply having been lost: the client retransmits the
	// identical request (same ClientId, same Seq).
	var r2 rpc.PutReply
	if ok := clnt.Call(server, "KVServer.Put", &args, &r2); !ok {
		t.Fatalf("retransmitted Put RPC failed")
	}
	if r2.Err != rpc.OK {
		t.Fatalf("retransmitted Put err %v; expected the replayed OK", r2.Err)
	}

	ck := ts.MakeClerk()
	if val, ver, err := ck.Get("k"); err != rpc.OK {
		t.Fatalf("Get err %v", err)
	} else if val != "v1" || ver != 1 {
		t.Fatalf("Put executed more than once: got value=%q version=%d, want value=%q version=1", val, ver, "v1")
	}

	// A genuinely new request (new Seq) from the same client must not
	// be treated as a duplicate.
	args2 := rpc.PutArgs{Key: "k", Value: "v2", Version: 1, ClientId: 12345, Seq: 2}
	var r3 rpc.PutReply
	if ok := clnt.Call(server, "KVServer.Put", &args2, &r3); !ok {
		t.Fatalf("second Put RPC failed")
	}
	if r3.Err != rpc.OK {
		t.Fatalf("second Put err %v; expected OK", r3.Err)
	}
	if val, ver, err := ck.Get("k"); err != rpc.OK {
		t.Fatalf("Get err %v", err)
	} else if val != "v2" || ver != 2 {
		t.Fatalf("got value=%q version=%d, want value=%q version=2", val, ver, "v2")
	}
}
