package main

import (
	"context"
	"cuhk/asgn/raft"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

func main() {
	ports := os.Args[2]
	myport, _ := strconv.Atoi(os.Args[1])
	nodeID, _ := strconv.Atoi(os.Args[3])
	heartBeatInterval, _ := strconv.Atoi(os.Args[4])
	electionTimeout, _ := strconv.Atoi(os.Args[5])

	portStrings := strings.Split(ports, ",")

	// A map where
	// 		the key is the node id
	//		the value is the {hostname:port}
	nodeidPortMap := make(map[int]int)
	for i, portStr := range portStrings {
		port, _ := strconv.Atoi(portStr)
		nodeidPortMap[i] = port
	}

	// Create and start the Raft Node.
	_, err := NewRaftNode(myport, nodeidPortMap,
		nodeID, heartBeatInterval, electionTimeout)

	if err != nil {
		log.Fatalln("Failed to create raft node:", err)
	}

	// Run the raft node forever.
	select {}
}

type raftNode struct {
	peers              map[int32]raft.RaftNodeClient
	myId               int32
	serverState        raft.Role
	mu                 sync.Mutex
	leaderId           int32
	heartBeatInterval  int
	currentTerm        int32
	votedFor           int32
	log                []*raft.LogEntry
	kvStore            map[string]int32
	commitIndex        int32
	nextIndex          []int32
	matchIndex         []int32
	notifyHearBeat     chan bool
	waitOp             map[int32]chan bool
	stopCurElection    chan bool
	resetElectionChan  chan bool
	stopElectionChan   chan bool
	resetHeartBeatChan chan bool
	stopHeartBeatChan  chan bool
}

// Desc:
// NewRaftNode creates a new RaftNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes.
//
// Params:
// myport: the port of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than nodeidPortMap[nodeId]
// nodeidPortMap: a map from all node IDs to their ports.
// nodeId: the id of this node
// heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
// electionTimeout: The election timeout for this node. In millisecond.
func NewRaftNode(myport int, nodeidPortMap map[int]int, nodeId, heartBeatInterval,
	electionTimeout int) (raft.RaftNodeServer, error) {
	// TODO: Implement this!

	//remove myself in the hostmap
	delete(nodeidPortMap, nodeId)

	//a map for {node id, gRPCClient}
	hostConnectionMap := make(map[int32]raft.RaftNodeClient)

	rn := raftNode{
		peers:              hostConnectionMap,
		myId:               int32(nodeId),
		serverState:        raft.Role_Follower,
		leaderId:           -1,
		resetElectionChan:  make(chan bool),
		stopElectionChan:   make(chan bool),
		resetHeartBeatChan: make(chan bool),
		stopHeartBeatChan:  make(chan bool),
		currentTerm:        0,
		votedFor:           -1,
		kvStore:            make(map[string]int32),
		commitIndex:        0,
		nextIndex:          make([]int32, len(nodeidPortMap)+1),
		matchIndex:         make([]int32, len(nodeidPortMap)+1),
		stopCurElection:    make(chan bool),
		waitOp:             make(map[int32]chan bool),
	}
	rn.log = append(rn.log, &raft.LogEntry{Term: 0})
	for i := range rn.nextIndex {
		rn.nextIndex[i] = int32(len(rn.log)-1) + 1
		rn.matchIndex[i] = 0
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", myport))

	if err != nil {
		log.Println("Fail to listen port", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	raft.RegisterRaftNodeServer(s, &rn)

	log.Printf("Start listening to port: %d", myport)
	go s.Serve(l)

	//Try to connect nodes
	for tmpHostId, hostPorts := range nodeidPortMap {
		hostId := int32(tmpHostId)
		numTry := 0
		for {
			numTry++

			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", hostPorts), grpc.WithInsecure(), grpc.WithBlock())
			//defer conn.Close()
			client := raft.NewRaftNodeClient(conn)
			if err != nil {
				log.Println("Fail to connect other nodes. ", err)
				time.Sleep(1 * time.Second)
			} else {
				hostConnectionMap[hostId] = client
				break
			}
		}
	}
	log.Printf("[%d]: Successfully connect all nodes", myport)

	go rn.Election_(electionTimeout)
	go rn.HeartBeat_(heartBeatInterval)

	go func() {
		for {
			switch rn.serverState {
			case raft.Role_Follower:

			case raft.Role_Candidate:

			case raft.Role_Leader:
				rn.leaderId = rn.myId
				for index := range rn.nextIndex {
					rn.nextIndex[index] = int32(len(rn.log)-1) + 1
					rn.matchIndex[index] = 0
				}

				respCh := make(chan *raft.AppendEntriesReply, len(rn.peers))
				rn.notifyHearBeat = make(chan bool, 1)
				rn.resetHeartBeatChan <- true
				rn.notifyHearBeat <- true
				for rn.serverState == raft.Role_Leader {
					select {
					case <-rn.notifyHearBeat:
						go rn.sendHeartBeat(respCh)
					case reply := <-respCh:
						if reply.Term > rn.currentTerm {
							rn.mu.Lock()
							rn.serverState = raft.Role_Follower
							rn.mu.Unlock()
							rn.currentTerm = reply.Term
							rn.votedFor = -1
							return
						}

						rn.matchIndex[reply.From] = reply.MatchIndex
						if reply.Success {
							rn.nextIndex[reply.From] = reply.MatchIndex + 1
						} else {
							rn.nextIndex[reply.From] = rn.nextIndex[reply.From] - 1
						}

						commitId := rn.commitIndex
						commitNum := 1
						nextCommitId := int32(len(rn.log) - 1)
						for _, matchId := range rn.matchIndex {
							if matchId > commitId {
								commitNum += 1
								if matchId < nextCommitId {
									nextCommitId = matchId
								}
							}
						}
						if nextCommitId > commitId && commitNum >= (len(rn.peers)+1)/2+1 && rn.log[nextCommitId].Term == rn.currentTerm {
							startCommitId := rn.commitIndex
							rn.commitIndex = nextCommitId
							endCommitId := rn.commitIndex
							for i := startCommitId + 1; i <= endCommitId; i++ {
								status := false
								logEntry := rn.log[i]
								if logEntry.Op == raft.Operation_Put {
									rn.kvStore[logEntry.Key] = logEntry.Value
									status = true
								} else {
									_, status = rn.kvStore[logEntry.Key]
									delete(rn.kvStore, logEntry.Key)
								}
								_, success := rn.waitOp[i]
								if success {
									rn.waitOp[i] <- status
									delete(rn.waitOp, i)
								}
							}
						}
					}
				}

			}
		}
	}()

	return &rn, nil
}

func (rn *raftNode) StartLeaderElection() {
	// response channel
	respCh := make(chan *raft.RequestVoteReply, len(rn.peers))

	rn.currentTerm = rn.currentTerm + 1
	currentTerm := rn.currentTerm
	rn.mu.Lock()
	lastLogIndex := int32(len(rn.log) - 1)
	lastLogTerm := rn.log[len(rn.log)-1].Term
	rn.mu.Unlock()
	ctx := context.Background()

	for hostId, client := range rn.peers {

		go func(hostId int32, client raft.RaftNodeClient) {
			reply, _ := client.RequestVote(
				ctx,
				&raft.RequestVoteArgs{
					From:         rn.myId,
					To:           hostId,
					Term:         currentTerm,
					CandidateId:  rn.myId,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				},
			)
			respCh <- reply
		}(hostId, client)
	}

	var grantedVotes int32 = 0

	rn.votedFor = rn.myId
	grantedVotes++

	for rn.serverState == raft.Role_Candidate {
		select {
		case reply := <-respCh:
			if reply.Term > rn.currentTerm {
				rn.mu.Lock()
				rn.serverState = raft.Role_Follower
				rn.mu.Unlock()
				rn.currentTerm = reply.Term
				rn.votedFor = -1
				return
			}

			if reply.VoteGranted {
				rn.mu.Lock()
				grantedVotes++
				rn.mu.Unlock()
				if int(grantedVotes) >= (len(rn.peers)+1)/2+1 {
					rn.mu.Lock()
					rn.serverState = raft.Role_Leader
					rn.mu.Unlock()
					return
				}
			}
		case <-rn.stopCurElection:
			return
		}
	}
}

func (rn *raftNode) sendHeartBeat(respCh chan *raft.AppendEntriesReply) {

	currentTerm := rn.currentTerm
	commitIndex := rn.commitIndex
	ctx := context.Background()

	for hostId, client := range rn.peers {

		go func(hostId int32, client raft.RaftNodeClient) {
			prevLogEntry := rn.log[rn.nextIndex[hostId]-1]

			reply, _ := client.AppendEntries(
				ctx,
				&raft.AppendEntriesArgs{
					From:         rn.myId,
					To:           hostId,
					Term:         currentTerm,
					LeaderId:     rn.myId,
					PrevLogIndex: rn.nextIndex[hostId] - 1,
					PrevLogTerm:  prevLogEntry.Term,
					Entries:      rn.log[rn.nextIndex[hostId]:],
					LeaderCommit: commitIndex,
				},
			)

			respCh <- reply
		}(hostId, client)
	}
}

// Desc:
// Propose initializes proposing a new operation, and replies with the
// result of committing this operation. Propose should not return until
// this operation has been committed, or this node is not leader now.
//
// If the we put a new <k, v> pair or deleted an existing <k, v> pair
// successfully, it should return OK; If it tries to delete an non-existing
// key, a KeyNotFound should be returned; If this node is not leader now,
// it should return WrongNode as well as the currentLeader id.
//
// Params:
// args: the operation to propose
// reply: as specified in Desc
func (rn *raftNode) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	// TODO: Implement this!

	var ret raft.ProposeReply
	ret.CurrentLeader = rn.leaderId

	role := rn.serverState
	if role != raft.Role_Leader {
		ret.Status = raft.Status_WrongNode
	} else {
		rn.log = append(rn.log, &raft.LogEntry{
			Term:  rn.currentTerm,
			Op:    args.Op,
			Key:   args.Key,
			Value: args.V,
		})
		waitId := int32(len(rn.log) - 1)
		rn.waitOp[waitId] = make(chan bool)
		select {
		case status := <-rn.waitOp[waitId]:
			if status {
				ret.Status = raft.Status_OK
			} else {
				ret.Status = raft.Status_KeyNotFound
			}
		}
	}

	return &ret, nil
}

// Desc:GetValue
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (rn *raftNode) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	// TODO: Implement this!
	var ret raft.GetValueReply
	val, success := rn.kvStore[args.Key]
	if success {
		ret.V = val
		ret.Status = raft.Status_KeyFound
	} else {
		ret.Status = raft.Status_KeyNotFound
	}
	return &ret, nil
}

// Desc:
// Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the RequestVote Reply Message
func (rn *raftNode) RequestVote(ctx context.Context, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	// TODO: Implement this!
	var reply raft.RequestVoteReply

	currentTerm := rn.currentTerm

	reply.From = args.To
	reply.To = args.From
	reply.Term = currentTerm
	reply.VoteGranted = false

	if args.Term < currentTerm {
		return &reply, nil
	}

	if args.Term > currentTerm {
		rn.mu.Lock()
		rn.serverState = raft.Role_Follower
		rn.mu.Unlock()
		rn.currentTerm = args.Term
		rn.votedFor = -1
		reply.Term = args.Term
	}

	rn.mu.Lock()
	lastLogIndex := int32(len(rn.log) - 1)
	lastLogTerm := rn.log[len(rn.log)-1].Term
	rn.mu.Unlock()
	votedFor := rn.votedFor

	c1 := (lastLogTerm > args.LastLogTerm) || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex)
	if (votedFor == -1 || votedFor == args.CandidateId) && !c1 {
		rn.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

	if reply.VoteGranted {
		rn.resetElectionChan <- true
	}
	return &reply, nil
}

// Desc:
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the AppendEntries Reply Message
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	// TODO: Implement this

	var reply raft.AppendEntriesReply
	currentTerm := rn.currentTerm

	reply.From = args.To
	reply.To = args.From
	reply.Term = currentTerm
	reply.Success = false
	reply.MatchIndex = rn.commitIndex

	if args.Term < currentTerm {
		return &reply, nil
	}

	rn.resetElectionChan <- true
	rn.leaderId = args.LeaderId

	if (args.Term > currentTerm) || (rn.serverState != raft.Role_Follower) {
		rn.mu.Lock()
		rn.serverState = raft.Role_Follower
		rn.mu.Unlock()
		rn.currentTerm = args.Term
		rn.votedFor = -1
		reply.Term = args.Term
	}

	rn.mu.Lock()
	lastLogIndex := int32(len(rn.log) - 1)
	rn.mu.Unlock()

	log_match := lastLogIndex >= args.PrevLogIndex && rn.log[args.PrevLogIndex].Term == args.PrevLogTerm
	if !log_match {
		return &reply, nil
	}

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + int32(i) + 1
		if index > lastLogIndex {
			rn.log = append(rn.log, entry)
		} else if rn.log[index].Term != entry.Term {
			rn.log = rn.log[:index]
			rn.log = append(rn.log, args.Entries[index])
		}
	}

	if (args.LeaderCommit > rn.commitIndex) && (int32(len(rn.log)-1) > rn.commitIndex) {
		startCommitId := rn.commitIndex
		if args.LeaderCommit < int32(len(rn.log)-1) {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = int32(len(rn.log) - 1)
		}
		endCommitId := rn.commitIndex

		for i := startCommitId + 1; i <= endCommitId; i++ {
			logEntry := rn.log[i]
			if logEntry.Op == raft.Operation_Put {
				rn.kvStore[logEntry.Key] = logEntry.Value
			} else {
				delete(rn.kvStore, logEntry.Key)
			}

		}
	}
	reply.MatchIndex = int32(len(rn.log) - 1)
	reply.Success = true
	return &reply, nil
}

func (rn *raftNode) Election_(electionTimeout int) {
	for {

		select {
		case <-rn.resetElectionChan:
			continue
		case <-rn.stopElectionChan:
			return
		case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
			role := rn.serverState
			if role != raft.Role_Leader {
				if role != raft.Role_Candidate {
					rn.mu.Lock()
					rn.serverState = raft.Role_Candidate
					rn.mu.Unlock()
				} else {
					rn.stopCurElection <- true
				}
				go rn.StartLeaderElection()
			}
		}
	}
}

func (rn *raftNode) HeartBeat_(heartBeatInterval int) {
	rn.heartBeatInterval = heartBeatInterval
	for {
		select {
		case <-rn.stopHeartBeatChan:
			return
		case <-rn.resetHeartBeatChan:
			continue
		case <-time.After(time.Duration(heartBeatInterval) * time.Millisecond):
			if rn.serverState == raft.Role_Leader {
				rn.notifyHearBeat <- true
			}
		}
	}
}

// Desc:
// Set electionTimeOut as args.Timeout milliseconds.
// You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetElectionTimeout(ctx context.Context, args *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	// TODO: Implement this!

	rn.stopElectionChan <- true
	go rn.Election_(int(args.Timeout))

	var reply raft.SetElectionTimeoutReply
	return &reply, nil
}

// Desc:
// Set heartBeatInterval as args.Interval milliseconds.
// You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetHeartBeatInterval(ctx context.Context, args *raft.SetHeartBeatIntervalArgs) (*raft.SetHeartBeatIntervalReply, error) {
	// TODO: Implement this!

	rn.stopHeartBeatChan <- true
	go rn.HeartBeat_(int(args.Interval))

	var reply raft.SetHeartBeatIntervalReply
	return &reply, nil
}

//NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	return nil, nil
}
