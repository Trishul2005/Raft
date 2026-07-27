package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion

	// ClientId/Seq identify this Put uniquely so a server can recognize
	// a retransmission of the same logical request (e.g. after its
	// original reply was dropped) and replay the original outcome
	// instead of re-executing or returning an ambiguous error. Get
	// needs no equivalent, since it has no side effects to duplicate.
	ClientId int64
	Seq      int64
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}

