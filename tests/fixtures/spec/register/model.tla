---- MODULE model ----
\* A toy register that acknowledges writes, saves them, and may crash: the
\* second spec bundle, which exercises spec check, translate, and conform
\* without any election specifics, and runs on Valkey through its own
\* experiment (experiments/toy.register.yaml).
EXTENDS Naturals

CONSTANTS
    SyncBeforeAck,  \* TRUE: a write is acknowledged only once it is saved
    MaxValue,
    MaxCrashes

VARIABLES mem, disk, acked, crashes

vars == <<mem, disk, acked, crashes>>

Init == mem = 0 /\ disk = 0 /\ acked = 0 /\ crashes = 0

Write == mem < MaxValue /\ mem' = mem + 1 /\ UNCHANGED <<disk, acked, crashes>>

Ack == /\ acked < mem
       /\ SyncBeforeAck => disk = mem
       /\ acked' = mem
       /\ UNCHANGED <<mem, disk, crashes>>

Save == disk # mem /\ disk' = mem /\ UNCHANGED <<mem, acked, crashes>>

Crash == /\ crashes < MaxCrashes
         /\ mem' = disk
         /\ crashes' = crashes + 1
         /\ UNCHANGED <<disk, acked>>

Next == Write \/ Ack \/ Save \/ Crash

Spec == Init /\ [][Next]_vars

\* An acknowledged write is never lost.
AckedDurable == acked <= mem
====
