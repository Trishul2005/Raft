# Extension: Exactly-Once Semantics for Put

I added exactly-once semantics for `Put` to both `kvsrv1` and `kvraft1`, replacing the
existing at-least-once-with-ambiguity design. Previously, if a client's RPC to `Put`
timed out (because either the request or the reply was lost), the Clerk had no way to
tell whether its request had actually been applied: a retry with the same version
would get a legitimate `ErrVersion` from the server whether or not the *original*
attempt was the one that had set that version, so `Put` had to return the ambiguous
`ErrMaybe` and push the problem onto the caller. I closed that gap with the classic
duplicate-detection design from earlier iterations of this course's Lab 2: each
`Clerk` generates a random 63-bit `ClientId` at creation and tags every `Put` with a
monotonically increasing `Seq`; every retransmission of one logical `Put` (whether
due to a dropped request, a dropped reply, or — in `kvraft1` — a leader change)
carries the *same* `(ClientId, Seq)` pair. The server keeps a table of the highest
`Seq` it has seen per client together with the reply it returned for it; on a
`Put` whose `Seq` is less than or equal to what's recorded, it replays that exact
reply instead of re-executing the request. This is enough to give a definitive answer
because a `Clerk` never has two `Put`s in flight at once, so only the latest sequence
number per client needs to be remembered. In `kvraft1`, the table lives inside the
replicated state machine itself (checked and updated inside `DoOp`, and included in
`Snapshot`/`Restore`) rather than at the RPC handler, so every replica — and any future
leader — agrees on which requests have already been applied. With this in place,
`Clerk.Put` in both packages no longer needs to fall back to `ErrMaybe` at all: it
always returns the true, definitive outcome of the request, even after an arbitrary
number of retries over an unreliable network or across a leader change. `Get` needed
no equivalent change since it has no side effects to duplicate. I updated
`kvsrv1`'s existing `TestUnreliableNet` (which previously *required* `ErrMaybe` to
occur) to instead assert it never occurs, and added a new `TestExactlyOnce` to both
packages that manually retransmits an identical `PutArgs` and confirms the request
is applied exactly once and both RPCs return the identical reply, plus
`TestUnreliableNetExactlyOnce` in `kvraft1` exercising the same property against a
real unreliable network with leader elections in play.
