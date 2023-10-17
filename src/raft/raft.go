package raft

// TODO: raft on mobile p2p networks (adapt concept of majority)
// TODO: requires infinitely increasing logical clock

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ElectionState int

type StateChangeCmd struct {
	Command interface{}
	Term    int
}

const (
	Follower ElectionState = iota
	Candidate
	Leader
)

const heartbeatGap = 40 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm    int                 // current election term of this peer
	state          ElectionState       // specifies the election state of this peer
	currentLeader  int                 // index of peer which is the election leader
	isLeaderAlive  bool                // true if heartbeat was received since last election timeout
	votedFor       int                 // index of peer for which this node voted in election
	candidateVotes []*RequestVoteReply // holds the vote received from each peer when current node is a Candidate

	logs            []StateChangeCmd // holds state change commands issued by users; indexing per RAFT starts at 1
	commitInd       int              // index of 'logs' upto which commands have been 'committed' by Leader
	lastApplied     int              // index of 'logs' upto which commands have been applied to the state machine by Raft node
	peerReplication []int            // holds index of the last replicated command (from 'logs') in each peer
	applyCh         chan ApplyMsg    // channel on which committed values are to be sent

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	LeaderID     int
	Term         int
	LeaderCommit int // index of last committed log on the Leader
	PrevLogInd   int // index of last replicated log on peer (as per Leader)
	PrevLogTerm  int // term of last replicated log on peer (as per Leader)
	Entries      []StateChangeCmd
}

type AppendEntriesReply struct {
	Success       bool // true iff both LastLogInd and LastLogTerm are ratified by peer
	NewLastLogInd int  // index of last replicated log after adding the 'NewLogs'
	Term          int  // term of the peer node receiving AppendEntries RPC
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		log.Printf("INFO - node %v: stale heartbeat from peer %v to node %v", rf.me, args.LeaderID, rf.me)
		reply.Success = false
		return
	}

	log.Printf("INFO - node %v: valid heartbeat from leader %v to node %v", rf.me, args.LeaderID, rf.me)
	if rf.state != Follower {
		log.Printf("INFO - node %v: reverting node to Follower state, of leader %v", rf.me, args.LeaderID)
		rf.state = Follower
	}
	rf.currentTerm = args.Term
	rf.isLeaderAlive = true
	rf.currentLeader = args.LeaderID
	rf.commitInd = args.LeaderCommit

	if args.PrevLogInd > len(rf.logs) {
		log.Printf("INFO - node %v: ignoring AppendEntries RPC from leader %v as its PrevLogInd (%d) is higher than length of peer's logs (%d)", rf.me, args.LeaderID, args.PrevLogInd, len(rf.logs))
		reply.Success = false
		return
	}

	if args.PrevLogInd != 0 && rf.logs[args.PrevLogInd-1].Term != args.PrevLogTerm {
		log.Printf("INFO - node %v: not appending new entries as node's log entry at index %v (term %v) is different from the leader's (term %v)", rf.me, args.PrevLogInd, rf.logs[args.PrevLogInd-1].Term, args.PrevLogTerm)
		reply.Success = false
		return
	}

	log.Printf("INFO - node %v: since election terms between leader and node match at index %d, appending %d new entries to the node's log", rf.me, args.PrevLogInd, len(args.Entries))
	rf.logs = rf.logs[:args.PrevLogInd] // overwrite to match leader's log
	rf.logs = append(rf.logs, args.Entries...)

	reply.Success = true
	reply.NewLastLogInd = len(rf.logs) // 'logs' indexing starts at 1 per RAFT
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply) // no locks as rf.peers is only written to at the beginning of program
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // election term for which the vote is requested
	CandidateID int // index of peer requesting vote
	LastLogInd  int // index of Leader's latest log entry
	LastLogTerm int // term of Leader's latest log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoterTerm   int  // current term of the solicited node
	VoteGranted bool // whether solicited node voted in favour of the Candidate
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	errorStr := "INFO - node %v: denying vote to Candidate %v as "
	reply.VoteGranted = false
	reply.VoterTerm = rf.currentTerm
	if args.Term < rf.currentTerm {
		log.Printf(errorStr+"it is in lower term %d", rf.me, args.CandidateID, args.Term)
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 {
		log.Printf(errorStr+"already voted in current term %d", rf.me, args.CandidateID, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		// acknowledge the new term
		rf.currentTerm = args.Term
		rf.currentLeader = -1
		rf.state = Follower // surrender candidacy/leadership (if any) for the new term
	}

	if len(rf.logs) > 0 && args.LastLogTerm < rf.logs[len(rf.logs)-1].Term {
		log.Printf(errorStr+"its latest log is of lower term %d than nodes's %d", rf.me, args.CandidateID, args.LastLogTerm, rf.logs[len(rf.logs)-1].Term)
		return
	}

	if len(rf.logs) > 0 && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogInd < len(rf.logs) {
		log.Printf(errorStr+"it has a shorter logs record (len=%d) than the node (len=%d)", rf.me, args.CandidateID, args.LastLogInd, len(rf.logs))
		return
	}

	reply.VoteGranted = true
	reply.VoterTerm = args.Term
	rf.votedFor = args.CandidateID
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply) // no locks as rf.peers is only written to at the beginning of program
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.currentLeader != rf.me {
		isLeader = false
		return len(rf.logs), rf.currentTerm, isLeader
	}

	// Save user command and trigger agreement
	log.Printf("INFO - node %v: adding new entry to leader's log: %v", rf.me, command)
	rf.logs = append(rf.logs, StateChangeCmd{command, rf.currentTerm})
	index = len(rf.logs) // 'logs' indexing starts at 1 per RAFT
	term = rf.currentTerm
	rf.peerReplication[rf.me] = index

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) tickerForHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		if rf.commitInd < len(rf.logs) {
			go func() {
				log.Printf("INFO - node %v: leader trying to commit logs", rf.me)
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Leader {
					log.Printf("INFO - node %v: aborting attempt to commit as no longer in Leader state", rf.me)
					return
				}

				// once an entry from the current term is committed, all preceeding ones are comitted indirectly by the Log Matching Property
				var checkIndex int
				for checkIndex = len(rf.logs); checkIndex > rf.commitInd; checkIndex-- {
					log.Printf("INFO - node %v: trying to commit index %d", rf.me, checkIndex)
					if rf.logs[checkIndex-1].Term < rf.currentTerm {
						log.Printf("INFO - node %v: term of log to be committed (term %d) is lower than the current term %d; aborting attempt to commit", rf.me, rf.logs[checkIndex-1].Term, rf.currentTerm)
						return
					}

					peersCt := 0
					for peer, index := range rf.peerReplication {
						log.Printf("INFO - node %v: checking peer %v for log index %d", rf.me, peer, checkIndex)
						if checkIndex <= index {
							peersCt++
						}
					}

					if peersCt > len(rf.peers)/2 {
						break
					} else {
						log.Printf("INFO - node %v: index %d log has not been sufficiently replicated", rf.me, checkIndex)
					}
				}

				if checkIndex == 0 || checkIndex == rf.commitInd {
					log.Printf("INFO - node %v: no new logs are sufficiently replicated; aborting attempt to commit logs above current commit index %d", rf.me, rf.commitInd)
					return
				}

				for ind := rf.lastApplied + 1; ind <= checkIndex; ind++ {
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[ind-1].Command,
						CommandIndex: ind,
					}
					rf.lastApplied++
					rf.commitInd++
				}

				log.Printf("INFO - node %v: changed leader's latest committed log index to %d", rf.me, rf.commitInd)
				log.Printf("INFO - node %v: committed logs - %v", rf.me, rf.logs[:rf.commitInd])
			}()
		}

		log.Printf("INFO - node %v: leader %v sending heartbeat for term %v to peers now...", rf.me, rf.me, rf.currentTerm)
		for peer := 0; peer < len(rf.peers); peer++ {
			if peer != rf.me {
				args := AppendEntriesArgs{
					LeaderID:     rf.me,
					Term:         rf.currentTerm,
					LeaderCommit: rf.commitInd,
					PrevLogInd:   rf.peerReplication[peer],
					PrevLogTerm:  -1,
					Entries:      rf.logs[rf.peerReplication[peer]:], // 'logs' indexing starts at 1 per RAFT
				}
				if rf.peerReplication[peer] != 0 {
					args.PrevLogTerm = rf.logs[rf.peerReplication[peer]-1].Term // 'logs' indexing starts at 1 per RAFT
				}

				reply := AppendEntriesReply{}
				go func(peerServer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
					log.Printf("INFO - node %v: sending heartbeat to peer %v from leader %v", rf.me, peerServer, rf.me)
					ok := rf.sendAppendEntries(peerServer, args, reply)
					if !ok {
						log.Printf("WARN - node %v: failed to receive heartbeat acknowledgement from peer %v", rf.me, peerServer)
						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.state != Leader {
						log.Printf("INFO - node %v: ignoring AppendEntries RPC response from peer %v as no longer in Leader state", rf.me, peerServer)
						return
					}

					if reply.Term > rf.currentTerm {
						log.Printf("INFO - node %v: found peer %v in newer term %v, reverting Leader to Follower state", rf.me, peerServer, reply.Term)
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						return
					}

					if reply.Success {
						log.Printf("INFO - node %v: peer %d logs now match leader %d upto index %d", rf.me, peerServer, rf.me, reply.NewLastLogInd)
						rf.peerReplication[peerServer] = reply.NewLastLogInd
					} else {
						if rf.peerReplication[peerServer] > 0 {
							log.Printf("INFO - node %v: reducing peer %v's log replication index by 1 since AppendEntries RPC failed", rf.me, peerServer)
							rf.peerReplication[peerServer]--
						}
					}
				}(peer, &args, &reply)
			}
		}

		rf.mu.Unlock()
		time.Sleep(heartbeatGap)
	}
}

func (rf *Raft) ticker() { //ticker for election timeouts
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		if rf.state != Leader && rf.commitInd > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= len(rf.logs) && i <= rf.commitInd; i++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i-1].Command,
					CommandIndex: i,
				}
				rf.lastApplied = i
			}
			log.Printf("INFO - node %v: changed peer's latest applied log index to %d", rf.me, rf.lastApplied)
			log.Printf("INFO - node %v: applied logs - %v", rf.me, rf.logs[:rf.lastApplied])
		}

		if rf.state == Follower {
			if !rf.isLeaderAlive {
				log.Printf("INFO - node %v: missing heartbeat from node %v's leader, initating new election term", rf.me, rf.me)
				rf.state = Candidate
				rf.currentTerm++
				args := RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateID: rf.me,
					LastLogInd:  len(rf.logs),
				}
				if len(rf.logs) > 0 {
					args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
				}

				for peer := 0; peer < len(rf.peers); peer++ {
					if peer != rf.me {
						rf.candidateVotes[peer] = &RequestVoteReply{}

						go func(peerServer int, args *RequestVoteArgs, reply *RequestVoteReply) {
							ok := rf.sendRequestVote(peerServer, args, reply)
							if !ok {
								log.Printf("WARN - node %v: failed to receive response for vote request to peer %v", rf.me, peerServer)
								return
							}
							log.Printf("INFO - node %v: received response for vote request from peer %v", rf.me, peerServer)
						}(peer, &args, rf.candidateVotes[peer])
					}
				}
				log.Printf("INFO - node %v: waiting to receive responses from peers", rf.me)
			}
		} else if rf.state == Candidate {
			receivedVotes := 1 // Candidate always votes for self
			rf.votedFor = rf.me
			latestTerm := rf.currentTerm
			for ind, reply := range rf.candidateVotes {
				if ind == rf.me {
					continue
				} else if reply.VoteGranted {
					receivedVotes++
				} else {
					if reply.VoterTerm >= latestTerm {
						latestTerm = reply.VoterTerm
					}
				}
			}

			if latestTerm > rf.currentTerm {
				log.Printf("INFO - node %v: election term %v is stale, reverting Candidate %v to Follower state", rf.me, rf.currentTerm, rf.me)
				rf.state = Follower
				rf.currentTerm = latestTerm
				rf.votedFor = -1
			} else {
				if receivedVotes > len(rf.peers)/2 {
					log.Printf("INFO - node %v: node %v elected as current term %v leader", rf.me, rf.me, rf.currentTerm)
					rf.state = Leader
					rf.currentLeader = rf.me
					rf.isLeaderAlive = true
					for i := range rf.peerReplication {
						rf.peerReplication[i] = 0 // 'logs' indexing starts at 1 per RAFT
					}
					go rf.tickerForHeartbeats()

				} else {
					log.Printf("INFO - node %v: split vote election in term %v for Candidate %v, trigerring a new election after timeout", rf.me, rf.currentTerm, rf.me)
					rf.state = Follower
				}
			}
		}

		rf.isLeaderAlive = false // reset toggle for next Leader heartbeat
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	rf.state = Follower
	rf.currentTerm = 0
	rf.currentLeader = -1
	rf.isLeaderAlive = false
	rf.votedFor = -1
	rf.candidateVotes = make([]*RequestVoteReply, len(peers))
	rf.commitInd = 0   // 'logs' indexing starts at 1 per RAFT
	rf.lastApplied = 0 // 'logs' indexing starts at 1 per RAFT
	rf.peerReplication = make([]int, len(peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
