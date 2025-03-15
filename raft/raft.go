package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

const DEBUG = 0

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
		id:                            serverId,
		peerList:                      peerList,
		server:                        server,
		db:                            db,
		commitChan:                    commitChan,
		newCommitReady:                make(chan struct{}, 16),
		trigger:                       make(chan struct{}, 1),
		currentTerm:                   0,
		votedFor:                      -1,
		potentialLeader:               -1,
		log:                           make([]LogEntry, 0),
		commitLength:                  0,
		lastApplied:                   0,
		state:                         Follower,
		electionResetEvent:            time.Now(),
		nextIndex:                     make(map[uint64]uint64),
		matchedIndex:                  make(map[uint64]uint64),
		activeLockExpiryMonitorCancel: make(map[string]context.CancelFunc),
	}
	if node.db.HasData() {
		// fmt.Printf("db has data Restoring from storage on node: %d\n", node.id)
		node.restoreFromStorage()
	}

	go func() {
		<-ready
		//RPC call to leader to join cluster assuming leader is automatically known - logic
		// if err := node.joinCluster(); err != nil {
		// 	log.Fatalf("Failed to join cluster: %v\n", err)
		// }
		node.mu.Lock()
		node.electionResetEvent = time.Now()
		node.mu.Unlock()
		// fmt.Printf("Starting election timer on node: %d\n", node.id)
		node.runElectionTimer()
	}()

	go node.sendCommit()

	return node
}

// func (node *Node) joinCluster() error {
// 	var reply JoinClusterReply
// 	args := JoinClusterArgs{ServerId: node.id, ServerAddr: node.server.listener.Addr()}
// 	// leaderAddr := node.peerList.GetRandomPeer()
// 	// if leaderAddr == nil {
// 	// 	return errors.New("no leader found")
// 	// }

// 	if err := node.server.RPC(leaderId, "RaftNode.JoinCluster", args, &reply); err != nil {
// 		return err
// 	}
// 	if !reply.Success {
// 		return fmt.Errorf("join cluster failed: leader %d, term %d", reply.LeaderId, reply.CurrentTerm)
// 	}
// 	return nil
// }

func (node *Node) addPeer(peerId uint64) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.peerList.Add(peerId)
	node.nextIndex[peerId] = uint64(len(node.log)) + 1
	node.matchedIndex[peerId] = 0
	fmt.Printf("[%d] Added peer %d to cluster\n", node.id, peerId)
}

func (node *Node) removePeer(peerId uint64) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.peerList.Exists(peerId) {
		node.peerList.Remove(peerId)
	}
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
			fmt.Printf("Committed entry for node Id: %d, index %d,  entry: %v\n", node.id, lastAppliedSaved+uint64(i)+1, entry.Command)
			node.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   lastAppliedSaved + uint64(i) + 1,
				Term:    currentTermSaved,
			}
		}
	}
}

// func (rn *Node) monitorLockExpiry(ctx context.Context, key string, expiryTime time.Time, clientID string) {
// 	duration := time.Until(expiryTime)
// 	select {
// 	case <-ctx.Done():
// 		rn.debug("Lock %q expiry monitoring canceled", key)
// 		return
// 	case <-time.After(duration):
// 		rn.mu.Lock()
// 		if entry, exists := rn.activeLocks[key]; exists {
// 			if time.Now().After(entry.ExpiryTime) {
// 				delete(rn.activeLocks, key)
// 				rn.debug("Lock %q automatically expired and released", key)
// 				rn.server.NotifyLockAcquire(clientID, ))
// 				// Optionally, append a log entry to record the auto-release.
// 			}
// 		}
// 		rn.mu.Unlock()
// 	}
// }

func (node *Node) runElectionTimer() {
	timeoutDuration := node.electionTimeout()
	node.mu.Lock()
	termStarted := node.currentTerm
	node.mu.Unlock()
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
		return time.Duration(1500) * time.Millisecond
	} else {
		return time.Duration(1500+rand.Intn(1500)) * time.Millisecond
	}
}

func (node *Node) startElection() {
	node.state = Candidate
	node.currentTerm += 1
	fmt.Printf("Calling startElection() by %d for term %d with peers: %v\n", node.id, node.currentTerm, node.peerList)
	candidacyTerm := node.currentTerm
	node.electionResetEvent = time.Now()
	node.votedFor = int64(node.id)
	node.potentialLeader = int64(node.id)
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
				//update nextIndex here?
				node.mu.Lock()
				defer node.mu.Unlock()
				if node.state != Candidate {
					return
				}
				if reply.Term > candidacyTerm {
					node.becomeFollower(reply.Term, -1)
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
	fmt.Printf("Node %d has become Leader for Term %d\n", node.id, node.currentTerm)
	node.state = Leader
	node.potentialLeader = int64(node.id)
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
				// fmt.Printf("Before Committing log entries on node %d\n", node.id)
				commitLengthSaved := node.commitLength
				for i := node.commitLength + 1; i <= uint64(len(node.log)); i++ {
					if node.log[i-1].Term == node.currentTerm {
						node.commitLength = i
					}
				}
				if commitLengthSaved != node.commitLength {
					// fmt.Printf("Committing log entries on node %d\n", node.id)
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
			lastLogIndexSaved := int(nextIndexSaved) - 1
			lastLogTermSaved := uint64(0)
			if lastLogIndexSaved > 0 {
				lastLogTermSaved = node.log[lastLogIndexSaved-1].Term
			}
			entries := node.log[nextIndexSaved-1:]

			// fmt.Printf("nextIndexSaved is %d and lastLogIndexSaved is %d for peer %d from node %d on term %d\n", nextIndexSaved, uint64(lastLogIndexSaved), peer, node.id, node.currentTerm)
			// fmt.Printf("[LeaderSendAppendEntries peer %d] entries size: %d\n", peer, len(entries))
			args := AppendEntriesArgs{
				Term:         leadershipTerm,
				LeaderId:     node.id,
				LastLogIndex: uint64(lastLogIndexSaved),
				LastLogTerm:  lastLogTermSaved,
				Entries:      entries,
				LeaderCommit: node.commitLength,
			}
			node.mu.Unlock()
			// if len(entries) > 0 {
			// 	fmt.Printf("Entries Sent on node %d: %v\n", peer, entries)
			// }
			var reply AppendEntriesReply
			if err := node.server.RPC(peer, "RaftNode.AppendEntries", args, &reply); err == nil {
				node.mu.Lock()
				defer node.mu.Unlock()
				if len(entries) > 0 {
					fmt.Printf("Entries Sent on node %d: %v\n", peer, entries)
					fmt.Printf("Reply from peer %d: success = %v\n", peer, reply.Success)
				}
				if reply.Term > leadershipTerm {
					node.becomeFollower(reply.Term, -1)
					return
				}
				if node.state == Leader && leadershipTerm == reply.Term {
					if reply.Success {
						node.nextIndex[peer] = nextIndexSaved + uint64(len(entries))
						node.matchedIndex[peer] = node.nextIndex[peer] - 1
						commitLengthSaved := node.commitLength
						for i := commitLengthSaved + 1; i <= uint64(len(node.log)); i++ {
							if node.log[i-1].Term == node.currentTerm {
								matchCount := 1
								for p := range node.peerList.peerSet {
									if node.matchedIndex[p] >= i {
										matchCount++
									}
								}
								if matchCount*2 > node.peerList.Size()+1 {
									node.commitLength = i
								}
							}
						}
						if commitLengthSaved != node.commitLength {
							fmt.Printf("commit length: %d\n", node.commitLength)
							// fmt.Printf("[LeaderSendAppendEntries Reply from peer %d] matchIndex[peer], nextIndex[peer], rn.commitIndex = %v, %v, %v\n", peer, node.matchedIndex[peer], node.nextIndex[peer], node.commitLength)
							node.newCommitReady <- struct{}{}
							node.trigger <- struct{}{}
						}
					} else {
						if reply.RecoveryTerm == 0 {
							node.nextIndex[peer] = reply.RecoveryIndex
						} else {
							lastLogIndex := uint64(0)
							for i := uint64(len(node.log)); i > 0; i-- {
								if node.log[i-1].Term == reply.RecoveryTerm {
									lastLogIndex = i
									break
								}
							}
							if lastLogIndex == 0 {
								node.nextIndex[peer] = reply.RecoveryIndex
							} else {
								node.nextIndex[peer] = lastLogIndex + 1
							}
						}
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

func (node *Node) joinAsPeer(leaderId uint64, term uint64, peerSet map[uint64]struct{}) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.peerList.Add(leaderId)
	fmt.Printf("[%d] Connected to leader %d\n", node.id, leaderId)
	node.becomeFollower(term, int64(leaderId))
	for peerId := range peerSet {
		if peerId != node.id {
			fmt.Printf("[%d] Connected to peer %d\n", node.id, peerId)
			node.peerList.Add(peerId)
		}
	}
}

func (node *Node) becomeFollower(newTerm uint64, leaderId int64) {
	node.state = Follower
	node.currentTerm = newTerm
	node.votedFor = -1
	node.potentialLeader = leaderId
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
	fmt.Printf("Restoring from storage on node: %d\n", node.id)
	for _, data := range []struct {
		name  string
		value interface{}
	}{
		{"currentTerm", &node.currentTerm},
		{"votedFor", &node.votedFor},
		{"log", &node.log},
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

func (node *Node) setData(key string, value interface{}) error {
	node.mu.Lock()
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	if err := enc.Encode(value); err != nil {
		node.mu.Unlock()
		return err
	}
	fmt.Printf("Set key: %s, value: %v\n", key, value)
	node.db.Set(key, buf.Bytes())
	node.mu.Unlock()
	return nil
}

func (node *Node) forwardToLeader(command interface{}) (bool, interface{}, error) {
	node.mu.Lock()
	leaderId := node.potentialLeader
	node.mu.Unlock()
	for leaderId != -1 || leaderId != int64(node.id) {
		fmt.Printf("Forwarding Data to Leader %d\n", leaderId)
		args := AppendDataArgs{Cmd: command, Term: node.currentTerm}
		var reply AppendDataReply
		if err := node.server.RPC(uint64(leaderId), "RaftNode.AppendData", args, &reply); err != nil {
			// fmt.Printf("RPC Call failed\n")
			return false, nil, err
		}
		if reply.Success {
			// fmt.Printf("Send data successful with leader %d\n", leaderId)
			return reply.Result.Success, reply.Result.Value, reply.Result.Error
		}
		node.mu.Lock()
		if reply.Term > node.currentTerm {
			node.mu.Unlock()
			return false, nil, errors.New("node not up to date to the most recent term")
		}
		node.mu.Unlock()
		if leaderId == reply.LeaderId {
			return false, nil, errors.New("Could not find the leader")
		}
		leaderId = reply.LeaderId
	}
	return false, nil, errors.New("write operation failed!")
}

func (node *Node) newLogEntry(command interface{}) (bool, interface{}, error) {
	node.mu.Lock()
	// fmt.Printf("[Query %d] %v\n", node.id, command)
	// fmt.Printf("Running Submit on node %d (leader, term = %v %v)\n", node.id, node.state == Leader, node.currentTerm)
	if node.state == Leader {
		switch cmd := command.(type) {
		case Read:
			// fmt.Printf("READ v: %v", v)
			key := cmd.Key
			var value LockInfo
			readErr := node.readFromStorage(key, &value)
			fmt.Printf("key, value = %v, %v\n", key, value)
			node.mu.Unlock()
			return true, value, readErr
		case AddServer:
			node.log = append(node.log, LogEntry{
				Command: command,
				Term:    node.currentTerm,
			})
			node.persistToStorage()
			node.mu.Unlock()
			node.trigger <- struct{}{}
			return true, nil, nil
		case RemoveServer:
			if !node.peerList.Exists(cmd.ServerId) || node.id == cmd.ServerId {
				node.mu.Unlock()
				return false, nil, errors.New("server with id " + fmt.Sprint(cmd.ServerId) + " cannot be removed from peer " + fmt.Sprint(node.id))
			}
			node.log = append(node.log, LogEntry{
				Command: command,
				Term:    node.currentTerm,
			})
			node.peerList.Remove(cmd.ServerId)
			// fmt.Printf("[%d] Removed peer %d from leader list\n", node.id, v.ServerId)
			node.persistToStorage()
			node.mu.Unlock()
			node.trigger <- struct{}{}
			return true, nil, nil
		case LockReleaseCommand:
			fmt.Printf("LockReleaseCommand for %v\n", cmd.Key)
			if !node.db.Exists(cmd.Key) {
				node.mu.Unlock()
				return false, nil, fmt.Errorf("cannot release lock %s. It is already released\n", cmd.Key)
			}
			var lockInfo LockInfo
			if readErr := node.readFromStorage(cmd.Key, &lockInfo); readErr != nil {
				node.mu.Unlock()
				return false, nil, errors.New("reading the lock info from db went wrong")
			}
			if lockInfo.Holder != cmd.ClientID {
				node.mu.Unlock()
				return false, nil, fmt.Errorf("lock %s is held by someone else\n", cmd.Key)
			}
			fmt.Printf("Should delete the lock key %s\n", cmd.Key)
			node.db.Delete(cmd.Key)
			if node.activeLockExpiryMonitorCancel[cmd.Key] == nil {
				node.mu.Unlock()
				return false, nil, fmt.Errorf("Expiry Monitor for Lock %s could not be cancelled", cmd.Key)
			}
			node.activeLockExpiryMonitorCancel[cmd.Key]()
			delete(node.activeLockExpiryMonitorCancel, cmd.Key)
			node.log = append(node.log, LogEntry{
				Command: cmd,
				Term:    node.currentTerm,
			})
			fmt.Printf("Successfully deleted data for the lock %s\n", cmd.Key)
			node.persistToStorage()
			node.mu.Unlock()
			node.trigger <- struct{}{}
			return true, nil, nil
		default:
			fmt.Printf("Data append on leader: %d, command: %v\n", node.id, command)
			node.log = append(node.log, LogEntry{
				Command: command,
				Term:    node.currentTerm,
			})
			node.persistToStorage()
			node.mu.Unlock()
			node.trigger <- struct{}{}
			return true, nil, nil
		}
	} else {
		switch cmd := command.(type) {
		case Read:
			// fmt.Printf("READ v: %v", v)
			return node.forwardToLeader(cmd)
		case Write:
			node.mu.Unlock()
			return node.forwardToLeader(cmd)
		}
	}

	node.mu.Unlock()
	return false, nil, nil
}

func (node *Node) applyLogEntry() error {
	for commit := range node.commitChan {
		// fmt.Printf("Collect Commits from node %d, entry: %+v\n", server.GetServerId(), commit)
		//do we need a lock? - logic
		// fmt.Printf("Collect Commits from node %d, entry: %+cmd\n", i, commit)
		// logtest(server.GetServerId(), "collectCommits (%d) got %+cmd", server.GetServerId(), commit)
		fmt.Printf("commit: %v\n", commit)
		switch cmd := commit.Command.(type) {
		case Write:
			return node.setData(cmd.Key, cmd.Val)
		case AddServer:
			// fmt.Printf("Add server\n")
			node.server.AddToCluster(cmd.ServerId)
		case RemoveServer:
			if node.id != cmd.ServerId {
				node.server.RemoveFromCluster(cmd.ServerId)
				// fmt.Printf("[%d] Server %d removed from cluster\n", server.GetServerId(), v.ServerId)
			}
		case LockAcquireCommand:
			fmt.Printf("Lock Acquire Command\n")
			now := time.Now()
			if node.db.Exists(cmd.Key) {
				fmt.Printf("Key %s already exists\n", cmd.Key)
				break
			}
			expiryTime := now.Add(cmd.TTL)
			lock := LockInfo{
				Holder:     cmd.ClientID,
				ExpiryTime: expiryTime,
			}
			node.setData(cmd.Key, lock)
			node.setData(cmd.FencingToken.Key, cmd.FencingToken.Value)
			fmt.Printf("Added lock key %s, ready to notify the client\n", cmd.Key)
			if node.state == Leader {
				ctx, cancel := context.WithCancel(context.Background())
				node.mu.Lock()
				node.activeLockExpiryMonitorCancel[cmd.Key] = cancel
				node.mu.Unlock()
				go node.monitorLockExpiry(ctx, cmd.Key, expiryTime)
				node.server.NotifyLockAcquire(cmd.ClientID, cmd.FencingToken)
				fmt.Printf("Notified Client about lock acquiring\n")
			}
		default:
			// fmt.Printf("default\n")
			break
		}
	}
	return nil
}

func (node *Node) Stop() {
	node.mu.Lock()
	defer node.mu.Unlock()

	node.state = Dead
	node.potentialLeader = -1
	close(node.newCommitReady)
}

func (node *Node) Report() (id int64, term uint64, isLeader bool) {
	node.mu.Lock()
	defer node.mu.Unlock()

	isLeader = node.state == Leader
	return node.potentialLeader, node.currentTerm, isLeader
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

func (node *Node) monitorLockExpiry(ctx context.Context, key string, expiryTime time.Time) {
	duration := time.Until(expiryTime)
	select {
	case <-ctx.Done():
		fmt.Printf("Lock %q expiry monitoring cancelled\n", key)
		return
	case <-time.After(duration):
		if node.db.Exists(key) {
			var lockInfo LockInfo
			readErr := node.readFromStorage(key, &lockInfo)
			if readErr != nil {
				fmt.Printf("Reading the lock info from db went wrong")
				return
			}
			if time.Now().After(lockInfo.ExpiryTime) {
				fmt.Printf("Lock %q automatically expired and called release lock\n", key)
				cmd := LockReleaseCommand{
					Key:      key,
					ClientID: lockInfo.Holder,
				}
				node.newLogEntry(cmd)
				return
			}
		}
	}
}
