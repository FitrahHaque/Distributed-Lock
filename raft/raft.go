package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const DEBUG = 0

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
	Dead
)

type CommitEntry struct {
	Command interface{}
	Term    uint64
	Index   uint64
}

type LogEntry struct {
	Command interface{}
	Term    uint64
}

type Node struct {
	id             uint64
	mu             sync.Mutex
	peerList       Set
	server         *Server
	db             *Database
	commitChan     chan CommitEntry
	newCommitReady chan struct{}
	trigger        chan struct{}

	currentTerm uint64
	votedFor    int
	log         []LogEntry

	commitLength       uint64
	lastApplied        uint64
	state              NodeState
	electionResetEvent time.Time

	nextIndex    map[uint64]uint64
	matchedIndex map[uint64]uint64
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateId  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppliedEntriesArgs struct {
	Term         uint64
	LeaderId     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppliedEntriesReply struct {
	Term    uint64
	Success bool
}

func (rn *Node) debug(format string, args ...interface{}) {
	if DEBUG > 0 {
		format = fmt.Sprintf("[%d] %s", rn.id, format)
		log.Printf(format, args...)
	}
}

func CreateNode(
	serverId uint64,
	peerList Set,
	server *Server,
	db *Database,
	ready <-chan interface{},
	commitChan chan CommitEntry,
) *Node {
	node := &Node{
		id:                 serverId,
		peerList:           peerList,
		server:             server,
		db:                 db,
		commitChan:         commitChan,
		newCommitReady:     make(chan struct{}, 16),
		trigger:            make(chan struct{}, 1),
		currentTerm:        0,
		votedFor:           -1,
		log:                make([]LogEntry, 0),
		commitLength:       0,
		lastApplied:        0,
		state:              Follower,
		electionResetEvent: time.Now(),
		nextIndex:          make(map[uint64]uint64),
		matchedIndex:       make(map[uint64]uint64),
	}
	if node.db.HasData() {
		node.restoreFromStorage()
	}

	go func() {
		<-ready
		node.mu.Lock()
		node.electionResetEvent = time.Now()
		node.mu.Unlock()
		node.runElectionTimer()
	}()

	go node.sendCommit()

	return node
}

func (node *Node) sendCommit() {
	for range node.newCommitReady {
		node.mu.Lock()
		lastAppliedSaved := node.lastApplied
		currentTermSaved := node.currentTerm
		var pendingCommitEntries []LogEntry
		if node.commitLength > node.lastApplied {
			pendingCommitEntries = node.log[node.lastApplied:node.commitLength]
			node.lastApplied = node.commitLength
		}
		node.mu.Unlock()
		for i, entry := range pendingCommitEntries {
			// fmt.Printf("Commited entry for node Id: %d, entry: %v\n", node.id, entry.Command)
			node.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   lastAppliedSaved + uint64(i) + 1,
				Term:    currentTermSaved,
			}
		}
	}
}

// study
func (node *Node) runElectionTimer() {
	timeoutDuration := node.electionTimeout()
	termStarted := node.currentTerm
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		node.mu.Lock()
		if node.state != Candidate && node.state != Follower {
			node.mu.Unlock()
			return
		}
		if termStarted != node.currentTerm {
			// fmt.Printf("Term changed from %v to %v for peer %v\n", termStarted, node.currentTerm, node.id)
			node.mu.Unlock()
			return
		}
		if elapsed := time.Since(node.electionResetEvent); elapsed >= timeoutDuration {
			node.startElection()
			node.mu.Unlock()
			return
		}
		node.mu.Unlock()
	}
}

func (node *Node) electionTimeout() time.Duration {
	if os.Getenv("RAFT_FORCE_MORE_REELECTION") == "true" && rand.Intn(3) > 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// study
func (node *Node) startElection() {
	// fmt.Printf("Calling startElection() by %d\n", node.id)
	node.state = Candidate
	node.currentTerm += 1
	candidacyTerm := node.currentTerm
	node.electionResetEvent = time.Now()
	node.votedFor = int(node.id)
	votesReceived := 1
	go func() {
		node.mu.Lock()
		defer node.mu.Unlock()
		if node.state == Candidate && votesReceived*2 > node.peerList.Size()+1 {
			node.becomeLeader()
		}
	}()

	for peer := range node.peerList.peerSet {
		go func(peer uint64) {
			node.mu.Lock()
			lastLogIndex, lastLogTerm := node.lastLogIndexAndTerm()
			node.mu.Unlock()
			args := RequestVoteArgs{
				Term:         candidacyTerm,
				CandidateId:  node.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			if err := node.server.RPC(peer, "RaftNode.RequestVote", args, &reply); err == nil {
				// fmt.Printf("Got reply back from RequestVote on %d from peer %v -> %v\n", node.id, peer, reply)
				node.mu.Lock()
				defer node.mu.Unlock()
				if node.state != Candidate {
					return
				}
				if reply.Term > candidacyTerm {
					node.becomeFollower(reply.Term)
					return
				}
				if reply.Term == candidacyTerm && reply.VoteGranted {
					votesReceived += 1
					if votesReceived*2 > node.peerList.Size()+1 {
						node.becomeLeader()
						return
					}
				}
			}
		}(peer)
	}

	go node.runElectionTimer()
}

func (node *Node) becomeLeader() {
	fmt.Printf("Calling becomeLeader() by %d\n", node.id)
	node.state = Leader
	for peer := range node.peerList.peerSet {
		node.nextIndex[peer] = uint64(len(node.log)) + 1
		node.matchedIndex[peer] = 0
	}
	go func(heartbeatTimeout time.Duration) {
		node.leaderSendAppendEntries()
		timer := time.NewTimer(heartbeatTimeout)
		defer timer.Stop()
		for {
			doSend := false
			select {
			case <-timer.C:
				doSend = true
				timer.Stop()
				timer.Reset(heartbeatTimeout)
			case _, ok := <-node.trigger:
				// fmt.Printf("becomeLeader running on node %d, leader, term = %v, %v\n", node.id, node.state, node.currentTerm)
				if ok {
					doSend = true
				} else {
					return
				}
				//study
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(heartbeatTimeout)
			}
			if doSend {
				if node.state != Leader {
					return
				}
				node.leaderSendAppendEntries()
			}
		}
	}(50 * time.Millisecond)
}

func (node *Node) leaderSendAppendEntries() {
	node.mu.Lock()
	leadershipTerm := node.currentTerm
	node.mu.Unlock()

	go func(peer uint64) {
		if node.peerList.Size() == 0 {
			if uint64(len(node.log)) > node.commitLength {
				commitLengthSaved := node.commitLength
				for i := node.commitLength + 1; i <= uint64(len(node.log)); i++ {
					if node.log[i-1].Term == node.currentTerm {
						node.commitLength = i
					}
				}
				if commitLengthSaved != node.commitLength {
					node.newCommitReady <- struct{}{}
					node.trigger <- struct{}{}
				}
			}
		}
	}(node.id)

	for peer := range node.peerList.peerSet {
		go func(peer uint64) {
			node.mu.Lock()
			nextIndexSaved := node.nextIndex[peer]
			prevLogIndexSaved := nextIndexSaved - 1
			prevLogTerm := uint64(0)
			if prevLogIndexSaved > 0 {
				prevLogTerm = node.log[prevLogIndexSaved-1].Term
			}
			entries := node.log[nextIndexSaved-1:]
			// fmt.Printf("[LeaderSendAppendEntries peer %d] entries size: %d\n", peer, len(entries))
			args := AppliedEntriesArgs{
				Term:         leadershipTerm,
				LeaderId:     node.id,
				PrevLogIndex: prevLogIndexSaved,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: node.commitLength,
			}
			node.mu.Unlock()
			var reply AppliedEntriesReply
			if err := node.server.RPC(peer, "RaftNode.AppendEntries", args, &reply); err == nil {
				node.mu.Lock()
				defer node.mu.Unlock()
				if reply.Term > leadershipTerm {
					node.becomeFollower(reply.Term)
					return
				}
				if node.state == Leader && leadershipTerm == reply.Term {
					if reply.Success {
						node.nextIndex[peer] = nextIndexSaved + uint64(len(entries))
						node.matchedIndex[peer] = node.nextIndex[peer] - 1
						commitLengthSaved := node.commitLength
						for i := node.commitLength + 1; i <= uint64(len(node.log)); i++ {
							if node.log[i-1].Term == node.currentTerm {
								matchCount := 1
								for peer := range node.peerList.peerSet {
									if node.matchedIndex[peer] >= i {
										matchCount++
									}
								}
								if matchCount*2 > node.peerList.Size()+1 {
									node.commitLength = i
								}
							}
						}
						if commitLengthSaved != node.commitLength {
							// fmt.Printf("[LeaderSendAppendEntries Reply from peer %d] matchIndex[peer], nextIndex[peer], rn.commitIndex = %v, %v, %v\n", peer, node.matchedIndex[peer], node.nextIndex[peer], node.commitLength)
							node.newCommitReady <- struct{}{}
							node.trigger <- struct{}{}
						}
					} else {
						//should it not be in a loop until we get a success?
					}
				}
			}
		}(peer)
	}

}

func (node *Node) lastLogIndexAndTerm() (uint64, uint64) {
	if len(node.log) > 0 {
		lastIndex := uint64(len(node.log) - 1)
		return lastIndex, node.log[lastIndex].Term
	}
	return 0, 0
}

func (node *Node) AppendEntries(args AppliedEntriesArgs, reply *AppliedEntriesReply) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.state == Dead || !node.peerList.Exists(args.LeaderId) {
		return nil
	}
	if args.Term > node.currentTerm {
		node.becomeFollower(args.Term)
	}
	reply.Success = false
	if args.Term == node.currentTerm {
		if node.state != Follower {
			node.becomeFollower(args.Term)
		}
		node.electionResetEvent = time.Now()
		if args.PrevLogIndex == 0 ||
			args.PrevLogIndex <= uint64(len(node.log)) && args.PrevLogTerm == node.log[args.PrevLogIndex-1].Term {
			reply.Success = true
			node.log = append(node.log[:args.PrevLogIndex], args.Entries...)
			if args.LeaderCommit > node.commitLength {
				node.commitLength = args.LeaderCommit
				node.newCommitReady <- struct{}{}
			}
		}
	}
	reply.Term = node.currentTerm
	// fmt.Printf("AppendEntries RPC on node %d, state, term = %v, %v\n", node.id, node.state, node.currentTerm)
	// for i, item := range node.log {
	// 	fmt.Printf("item %v: (command, term) = (%v,%v)\n", i, item.Command, item.Term)
	// }
	node.persistToStorage()
	return nil
}

func (node *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	// fmt.Printf("Calling RPC RequestVote on %d -> args: %v\n", node.id, args)
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.state == Dead || !node.peerList.Exists(args.CandidateId) {
		return nil
	}
	lastLogIndex, lastLogTerm := node.lastLogIndexAndTerm()
	if args.Term > node.currentTerm {
		node.becomeFollower(args.Term)
	}

	// fmt.Printf("[RequestVote %d]\n", node.id)
	// fmt.Printf("args.Term, node.currentTerm): (%v, %v)\n", args.Term, node.currentTerm)
	// fmt.Printf("args.CandidateId, node.votedFor): (%v, %v)\n", args.CandidateId, node.votedFor)
	// fmt.Printf("args.LastLogTerm, lastLogTerm): (%v, %v)\n", args.LastLogTerm, lastLogTerm)
	// fmt.Printf("args.LastLogIndex, lastLogindex): (%v, %v)\n\n", args.LastLogIndex, lastLogIndex)

	if args.Term == node.currentTerm &&
		(node.votedFor == -1 || node.votedFor == int(args.CandidateId)) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		node.votedFor = int(args.CandidateId)
		node.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = node.currentTerm
	node.persistToStorage()
	return nil
}

func (node *Node) becomeFollower(newTerm uint64) {
	node.state = Follower
	node.currentTerm = newTerm
	node.votedFor = -1
	node.electionResetEvent = time.Now()
	go node.runElectionTimer()
}

func (node *Node) persistToStorage() {
	for _, data := range []struct {
		name  string
		value interface{}
	}{
		{"currentTerm", node.currentTerm},
		{"votedFor", node.votedFor},
		{"log", node.log},
	} {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(data.value); err != nil {
			log.Fatal("encode error: ", err)
		}
		node.db.Set(data.name, buf.Bytes())
	}
}

func (node *Node) restoreFromStorage() {
	for _, data := range []struct {
		name  string
		value interface{}
	}{
		{"currentTerm", node.currentTerm},
		{"votedFor", node.votedFor},
		{"log", node.log},
	} {
		if value, found := node.db.Get(data.name); found {
			dec := gob.NewDecoder(bytes.NewBuffer(value))
			if err := dec.Decode(data.value); err != nil {
				log.Fatal("decode error: ", err)
			}
		} else {
			log.Fatal("No data found for", data.name)
		}
	}
}

func (node *Node) readFromStorage(key string, reply interface{}) error {
	if value, found := node.db.Get(key); found {
		dec := gob.NewDecoder(bytes.NewBuffer(value))
		if err := dec.Decode(reply); err != nil {
			return err
		}
		return nil
	} else {
		err := fmt.Errorf("KeyNotFound:%v", key)
		return err
	}
}

func (node *Node) Submit(command interface{}) (bool, interface{}, error) {
	node.mu.Lock()
	// fmt.Printf("[Submit %d] %v\n", node.id, command)
	// fmt.Printf("Running Submit on node %d (leader, term = %v %v)\n", node.id, node.state == Leader, node.currentTerm)
	if node.state == Leader {
		switch v := command.(type) {
		case Read:
			// fmt.Printf("READ v: %v", v)
			key := v.Key
			var value int
			readErr := node.readFromStorage(key, &value)
			// fmt.Printf("key, value = %v, %v\n", key, value)
			node.mu.Unlock()
			return true, value, readErr
		case AddServers:
			// fmt.Printf("AddServers v: %v\n", v)
			serverIds := v.ServerIds
			for i := 0; i < len(serverIds); i++ {
				if node.peerList.Exists(uint64(serverIds[i])) {
					node.mu.Unlock()
					return false, nil, errors.New("server with given serverID already exists")
				}
			}
			node.log = append(node.log, LogEntry{
				Command: command,
				Term:    node.currentTerm,
			})
			for i := 0; i < len(serverIds); i++ {
				node.peerList.Add(uint64(serverIds[i]))
				node.server.peerList.Add(uint64(serverIds[i]))
				node.nextIndex[uint64(serverIds[i])] = uint64(len(node.log)) + 1
				node.matchedIndex[uint64(serverIds[i])] = 0
			}
			node.persistToStorage()
			node.mu.Unlock()
			node.trigger <- struct{}{}
			return true, nil, nil
		case RemoveServers:
			// fmt.Printf("RemoveServers v: %v\n", v)
			serverIds := v.ServerIds
			for i := 0; i < len(serverIds); i++ {
				if !node.peerList.Exists(uint64(serverIds[i])) && node.id != uint64(serverIds[i]) {
					node.mu.Lock()
					return false, nil, errors.New("server with given serverID does not exist")
				}
			}
			node.log = append(node.log, LogEntry{
				Command: command,
				Term:    node.currentTerm,
			})
			for i := 0; i < len(serverIds); i++ {
				if node.id != uint64(serverIds[i]) {
					node.peerList.Remove(uint64(serverIds[i]))
					node.server.peerList.Remove(uint64(serverIds[i]))
				}
			}
			node.persistToStorage()
			node.mu.Unlock()
			node.trigger <- struct{}{}
			return true, nil, nil
		default:
			node.log = append(node.log, LogEntry{
				Command: command,
				Term:    node.currentTerm,
			})
			node.persistToStorage()
			// fmt.Printf("[Submit %d] log size: %v\n", node.id, len(node.log))
			// for i, itm := range node.log {
			// 	fmt.Printf("item %v: (command, term) = (%v,%v)\n", i, item.Command, item.Term)
			// }
			node.mu.Unlock()
			node.trigger <- struct{}{}
			return true, nil, nil
		}
	} else {
		switch v := command.(type) {
		case Read:
			fmt.Printf("READ v: %v", v)
			key := v.Key
			var value int
			readErr := node.readFromStorage(key, &value)
			fmt.Printf("key, value = %v, %v\n", key, value)
			node.mu.Unlock()
			return true, value, readErr
		}
	}

	node.mu.Unlock()
	return false, nil, nil
}

func (node *Node) Stop() {
	node.mu.Lock()
	defer node.mu.Unlock()

	node.state = Dead
	close(node.newCommitReady)
}

func (node *Node) Report() (id int, term int, isLeader bool) {
	node.mu.Lock()
	defer node.mu.Unlock()

	isLeader = node.state == Leader
	if isLeader {
		fmt.Printf("For the term %v, Node %v is Leader\n", node.currentTerm, node.id)
	}
	return int(node.id), int(node.currentTerm), isLeader
}

func (state NodeState) String() string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("Error: Unknown state")
	}
}
