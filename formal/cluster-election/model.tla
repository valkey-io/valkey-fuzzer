---------------------------------- MODULE model ----------------------------------
(***************************************************************************)
(* Failover election on the Valkey cluster bus (src/cluster_legacy.c).     *)
(*                                                                         *)
(* Replicas of a failed primary ask the voting primaries for a vote in a   *)
(* new epoch. A primary grants at most one vote per epoch, tracked by      *)
(* lastVoteEpoch, which is saved to nodes.conf. A replica that collects    *)
(* votes from a majority of primaries wins the epoch and takes the slots.  *)
(*                                                                         *)
(* Faults:                                                                 *)
(*   - Messages stay in `msgs` once sent and may be received any number of *)
(*     times or never, so the network drops, delays and reorders freely.   *)
(*   - A primary may crash and restart from what nodes.conf holds.         *)
(*                                                                         *)
(* Order of a granted vote, as in clusterSendFailoverAuthIfNeeded: set     *)
(* lastVoteEpoch, schedule the save (CLUSTER_TODO_SAVE_CONFIG), and queue  *)
(* the FAILOVER_AUTH_ACK on the link. clusterBeforeSleep then saves, and   *)
(* the event loop writes the queued ACK after it. With `sync` the save is  *)
(* on disk when the ACK leaves; with `best-effort` it is only handed to    *)
(* the BIO thread, which writes it later, so the ACK can leave before it   *)
(* lands. FlushBeforeSave lets a queued ACK leave before that save: the    *)
(* event loop flushes a link in the same iteration when its write handler  *)
(* is already armed (output was queued before the poll), or from a QoS     *)
(* poll of cluster links (9.2+). Valkey can flush early, so `sync` alone   *)
(* does not save every vote before it leaves: the safe config also sets    *)
(* FlushBeforeSave to FALSE.                                               *)
(*                                                                         *)
(* Not modeled: failure detection (the failed primary is assumed FAIL),    *)
(* the slot configEpoch check, ranking delays, and the 2 s safe_to_join    *)
(* window after restart, which is timing-based.                            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS
    Primaries,        \* voting primaries (each serves at least one slot)
    Candidates,       \* replicas of the failed primary
    MaxEpoch,         \* bound on epochs, keeps the state space finite
    MaxCrashes,       \* bound on primary crashes
    PersistBeforeAck, \* TRUE:  the save in clusterBeforeSleep is on disk when it
                      \*        returns (cluster-config-save-behavior sync, the default)
                      \* FALSE: it queues a BIO job that writes later
                      \*        (cluster-config-save-behavior best-effort)
    FlushBeforeSave   \* TRUE: a queued vote may also leave before the next save

VARIABLES
    currentEpoch,     \* node -> currentEpoch in memory
    lastVote,         \* primary -> lastVoteEpoch in memory
    diskEpoch,        \* primary -> currentEpoch in nodes.conf
    diskVote,         \* primary -> lastVoteEpoch in nodes.conf
    held,             \* primary -> votes queued on the link, not yet sent
    bio,              \* primary -> nodes.conf snapshots <<epoch, vote>> queued
                      \*            for the BIO thread (always empty under sync)
    msgs,             \* every message ever sent
    electionEpoch,    \* candidate -> epoch of its current election, 0 = none
    granted,          \* candidate -> primaries whose vote it counted
    winners,          \* <<candidate, epoch>> pairs that reached a majority
    crashes

vars == <<currentEpoch, lastVote, diskEpoch, diskVote, held, bio, msgs,
          electionEpoch, granted, winners, crashes>>

Nodes  == Primaries \cup Candidates
Epochs == 0..MaxEpoch
Max(a, b) == IF a > b THEN a ELSE b
IsMajority(S) == 2 * Cardinality(S) > Cardinality(Primaries)

Request(c, e) == [type |-> "request", from |-> c, epoch |-> e]
Vote(p, c, e) == [type |-> "vote", from |-> p, to |-> c, epoch |-> e]
Requests == [type : {"request"}, from : Candidates, epoch : Epochs]
Votes == [type : {"vote"}, from : Primaries, to : Candidates, epoch : Epochs]

TypeOK ==
    /\ currentEpoch \in [Nodes -> Epochs]
    /\ lastVote \in [Primaries -> Epochs]
    /\ diskEpoch \in [Primaries -> Epochs]
    /\ diskVote \in [Primaries -> Epochs]
    /\ held \in [Primaries -> SUBSET Votes]
    /\ \A p \in Primaries : bio[p] \in Seq(Epochs \X Epochs)
    /\ msgs \subseteq Requests \cup Votes
    /\ electionEpoch \in [Candidates -> Epochs]
    /\ granted \in [Candidates -> SUBSET Primaries]
    /\ winners \subseteq Candidates \X Epochs
    /\ crashes \in 0..MaxCrashes

Init ==
    /\ currentEpoch = [n \in Nodes |-> 0]
    /\ lastVote = [p \in Primaries |-> 0]
    /\ diskEpoch = [p \in Primaries |-> 0]
    /\ diskVote = [p \in Primaries |-> 0]
    /\ held = [p \in Primaries |-> {}]
    /\ bio = [p \in Primaries |-> <<>>]
    /\ msgs = {}
    /\ electionEpoch = [c \in Candidates |-> 0]
    /\ granted = [c \in Candidates |-> {}]
    /\ winners = {}
    /\ crashes = 0

-----------------------------------------------------------------------------
(* Candidate actions *)

\* clusterHandleReplicaFailover: bump currentEpoch, then
\* clusterRequestFailoverAuth broadcasts FAILOVER_AUTH_REQUEST.
StartElection(c) ==
    LET e == currentEpoch[c] + 1 IN
    /\ e <= MaxEpoch
    /\ currentEpoch' = [currentEpoch EXCEPT ![c] = e]
    /\ electionEpoch' = [electionEpoch EXCEPT ![c] = e]
    /\ granted' = [granted EXCEPT ![c] = {}]
    /\ msgs' = msgs \cup {Request(c, e)}
    /\ UNCHANGED <<lastVote, diskEpoch, diskVote, held, bio, winners, crashes>>

\* clusterProcessPacket: any message from a higher epoch bumps currentEpoch.
LearnEpoch(c) ==
    \E m \in msgs :
        /\ m.epoch > currentEpoch[c]
        /\ currentEpoch' = [currentEpoch EXCEPT ![c] = m.epoch]
        /\ UNCHANGED <<lastVote, diskEpoch, diskVote, held, bio, msgs,
                       electionEpoch, granted, winners, crashes>>

\* clusterProcessPacket, FAILOVER_AUTH_ACK: counted when the sender's epoch
\* is >= failover_auth_epoch. TCP does not duplicate, so count each primary once.
ReceiveVote(c) ==
    \E m \in msgs :
        /\ m.type = "vote"
        /\ m.to = c
        /\ electionEpoch[c] > 0
        /\ m.epoch >= electionEpoch[c]
        /\ m.from \notin granted[c]
        /\ granted' = [granted EXCEPT ![c] = @ \cup {m.from}]
        /\ UNCHANGED <<currentEpoch, lastVote, diskEpoch, diskVote, held, bio, msgs,
                       electionEpoch, winners, crashes>>

\* clusterHandleReplicaFailover: failover_auth_count >= needed_quorum.
Win(c) ==
    /\ electionEpoch[c] > 0
    /\ IsMajority(granted[c])
    /\ <<c, electionEpoch[c]>> \notin winners
    /\ winners' = winners \cup {<<c, electionEpoch[c]>>}
    /\ UNCHANGED <<currentEpoch, lastVote, diskEpoch, diskVote, held, bio, msgs,
                   electionEpoch, granted, crashes>>

-----------------------------------------------------------------------------
(* Primary actions *)

\* clusterProcessPacket bumps currentEpoch to the request's epoch, then
\* clusterSendFailoverAuthIfNeeded decides. A granted vote sets lastVoteEpoch,
\* schedules the save, and queues the ACK on the link (clusterSendMessage only
\* appends to the link's send queue); it leaves in SaveConfig or FlushEarly.
HandleRequest(p) ==
    \E m \in msgs :
        /\ m.type = "request"
        /\ LET epoch == Max(currentEpoch[p], m.epoch)
               grant == /\ m.epoch >= epoch       \* else "reqEpoch < curEpoch"
                        /\ lastVote[p] # epoch    \* else "already voted"
           IN
           /\ currentEpoch' = [currentEpoch EXCEPT ![p] = epoch]
           /\ IF grant
                THEN /\ lastVote' = [lastVote EXCEPT ![p] = epoch]
                     /\ held' = [held EXCEPT ![p] = @ \cup {Vote(p, m.from, epoch)}]
                ELSE UNCHANGED <<lastVote, held>>
        /\ UNCHANGED <<diskEpoch, diskVote, bio, msgs, electionEpoch, granted, winners, crashes>>

\* What nodes.conf will hold once every queued BIO save has run.
Saved(p) == IF bio[p] = <<>> THEN <<diskEpoch[p], diskVote[p]>> ELSE bio[p][Len(bio[p])]

\* clusterBeforeSleep: save nodes.conf (sync: clusterSaveConfigOrDie writes and
\* fsyncs it; best-effort: clusterSaveConfigBackground snapshots the config and
\* queues a BIO job), then the event loop writes the queued ACKs.
SaveConfig(p) ==
    LET now == <<currentEpoch[p], lastVote[p]>> IN
    /\ Saved(p) # now \/ held[p] # {}
    /\ IF PersistBeforeAck
         THEN /\ diskEpoch' = [diskEpoch EXCEPT ![p] = currentEpoch[p]]
              /\ diskVote' = [diskVote EXCEPT ![p] = lastVote[p]]
              /\ UNCHANGED bio
         ELSE /\ bio' = IF Saved(p) = now THEN bio ELSE [bio EXCEPT ![p] = Append(@, now)]
              /\ UNCHANGED <<diskEpoch, diskVote>>
    /\ msgs' = msgs \cup held[p]
    /\ held' = [held EXCEPT ![p] = {}]
    /\ UNCHANGED <<currentEpoch, lastVote, electionEpoch, granted, winners, crashes>>

\* The BIO thread writes the oldest queued snapshot (best-effort only).
BioWrite(p) ==
    /\ bio[p] # <<>>
    /\ diskEpoch' = [diskEpoch EXCEPT ![p] = Head(bio[p])[1]]
    /\ diskVote' = [diskVote EXCEPT ![p] = Head(bio[p])[2]]
    /\ bio' = [bio EXCEPT ![p] = Tail(@)]
    /\ UNCHANGED <<currentEpoch, lastVote, held, msgs, electionEpoch, granted, winners, crashes>>

\* A queued vote leaves before the next save: the link's write handler was
\* already armed in this event-loop iteration, or a QoS poll flushed it.
FlushEarly(p) ==
    /\ FlushBeforeSave
    /\ \E v \in held[p] :
          /\ msgs' = msgs \cup {v}
          /\ held' = [held EXCEPT ![p] = @ \ {v}]
    /\ UNCHANGED <<currentEpoch, lastVote, diskEpoch, diskVote, bio, electionEpoch, granted, winners, crashes>>

\* Process crash and restart: memory, the link queues, and queued BIO jobs are
\* lost; nodes.conf is reloaded.
Crash(p) ==
    /\ crashes < MaxCrashes
    /\ currentEpoch' = [currentEpoch EXCEPT ![p] = diskEpoch[p]]
    /\ lastVote' = [lastVote EXCEPT ![p] = diskVote[p]]
    /\ held' = [held EXCEPT ![p] = {}]
    /\ bio' = [bio EXCEPT ![p] = <<>>]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<diskEpoch, diskVote, msgs, electionEpoch, granted, winners>>

-----------------------------------------------------------------------------
Next ==
    \/ \E c \in Candidates :
          StartElection(c) \/ LearnEpoch(c) \/ ReceiveVote(c) \/ Win(c)
    \/ \E p \in Primaries :
          HandleRequest(p) \/ SaveConfig(p) \/ BioWrite(p) \/ FlushEarly(p) \/ Crash(p)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety properties *)

\* What operators care about: one election epoch never produces two primaries
\* for the same slots.
OneWinnerPerEpoch ==
    \A w1, w2 \in winners : w1[2] = w2[2] => w1[1] = w2[1]

\* Why it holds: no primary votes for two candidates in the same epoch.
VoteOncePerEpoch ==
    \A m1, m2 \in msgs :
        (/\ m1.type = "vote" /\ m2.type = "vote"
         /\ m1.from = m2.from /\ m1.epoch = m2.epoch) => m1.to = m2.to

\* Why VoteOncePerEpoch survives a crash: a vote that has left is already in
\* nodes.conf.  Holds when PersistBeforeAck and not FlushBeforeSave; this is
\* what a live run checks at a crash (valkey.oracle.cluster-election-safety).
VotesSavedBeforeSent ==
    \A m \in msgs : m.type = "vote" => diskVote[m.from] >= m.epoch

-----------------------------------------------------------------------------
(* Target states (claims.yaml targets): what a counterexample reaches. *)

\* A vote that has left its voter is no longer in the voter's memory: the
\* voter crashed before the vote reached nodes.conf and restarted without it,
\* so it may vote again in that epoch.
VoteLostInCrash ==
    \E m \in msgs : m.type = "vote" /\ lastVote[m.from] < m.epoch

\* What VotesSavedBeforeSent buys after a crash, stated over what a live run
\* observes exactly: the votes it sees leave, and the vars a crash restarts with.
NoVoteLostInCrash == ~VoteLostInCrash
=============================================================================
