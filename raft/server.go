package raft

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Write struct {
	Key string
	Val int
}

type Read struct {
	Key string
}

type AddServer struct {
	ServerId uint64
	Addr     net.Addr
}

type RemoveServers struct {
	ServerIds []int
}

type Server struct {
	id        uint64
	mu        sync.Mutex
	peerList  Set
	rpcServer *rpc.Server
	listener  net.Listener
	peers     map[uint64]*rpc.Client
	quit      chan interface{}
	wg        sync.WaitGroup

	node       *Node
	db         *Database
	commitChan chan CommitEntry
	ready      <-chan interface{}
}

func CreateServer(
	serverId uint64,
	db *Database,
	ready <-chan interface{},
	commitChan chan CommitEntry,
) (*Server, error) {
	server := new(Server)
	server.id = serverId
	server.peerList = makeSet()
	server.peers = make(map[uint64]*rpc.Client)
	server.db = db
	server.ready = ready
	server.commitChan = commitChan
	server.quit = make(chan interface{})
	return server, nil
}

func (server *Server) ConnectionAccept() {
	defer server.wg.Done()

	for {
		connection, err := server.listener.Accept()
		if err != nil {
			select {
			case <-server.quit:
				log.Printf("[%d] Accepting no more connection\n", server.id)
				return
			default:
				log.Fatalf("[%d] Error in accepting connection %s\n", server.id, err)
			}
		}
		server.wg.Add(1)
		go func() {
			server.rpcServer.ServeConn(connection)
			server.wg.Done()
		}()
	}
}

func (server *Server) Serve(port ...string) {
	server.mu.Lock()
	server.node = CreateNode(server.id, server.peerList, server, server.db, server.ready, server.commitChan)
	// server.node = NewRaftNode(server.id, server.peerList, server, server.db, server.ready, server.commitChan)
	server.rpcServer = rpc.NewServer()
	server.rpcServer.RegisterName("RaftNode", server.node)
	var err error
	var tcpPort string = ":"
	if len(port) == 1 {
		tcpPort = tcpPort + port[0]
	} else {
		tcpPort = tcpPort + "0"
	}
	server.listener, err = net.Listen("tcp", tcpPort)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] Listening at %s\n", server.id, server.listener.Addr())
	server.mu.Unlock()

	server.wg.Add(1)
	go server.ConnectionAccept()
}

func (server *Server) DisconnectAll() {
	server.mu.Lock()
	defer server.mu.Unlock()
	for id := range server.peers {
		if server.peers[id] != nil {
			server.peers[id].Close()
			server.peers[id] = nil
		}
	}
}

func (server *Server) Stop() {
	server.node.Stop()
	close(server.quit)
	server.listener.Close()
	log.Printf("[%d] Waiting for existing connections to close\n", server.id)
	server.wg.Wait()
	log.Printf("[%d] All conections closed. Stopping server\n", server.id)
}

func (server *Server) GetListenerAddr() net.Addr {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.listener.Addr()
}

func (server *Server) ConnectToPeer(peerId uint64, addr net.Addr) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.peers[peerId] == nil {
		fmt.Printf("[%d] Connecting to peer %d at %v: (%v, %v)\n", server.id, peerId, addr, addr.Network(), addr.String())
		peer, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		server.peers[peerId] = peer
	}
	return nil
}

func (server *Server) DisconnectPeer(peerId uint64) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	peer := server.peers[peerId]
	if peer != nil {
		err := peer.Close()
		server.peers[peerId] = nil
		return err
	}
	return nil
}

func (server *Server) RPC(peerId uint64, rpcCall string, args interface{}, reply interface{}) error {
	server.mu.Lock()
	peer := server.peers[peerId]
	server.mu.Unlock()
	if peer == nil {
		return fmt.Errorf("[%d] RPC Call to peer %d after it has been closed", server.id, peerId)
	} else {
		return peer.Call(rpcCall, args, reply)
	}
}

func (server *Server) GetServerId() uint64 {
	return server.id
}

func (server *Server) SetData(key string, value []byte) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.db.Set(key, value)
}

func (server *Server) GetData(key string) ([]byte, bool) {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.db.Get(key)
}

func (server *Server) AddToCluster(serverId uint64) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.peers[serverId] != nil {
		server.node.addToCluster(serverId)
	}
}

func (server *Server) JoinCluster(leaderId uint64, addr string) error {
	if leaderId < 0 {
		fmt.Printf("invalid leader id %d\n", leaderId)
		return errors.New("invalid leader id")
	}
	if server.GetServerId() == leaderId {
		return errors.New("cannot join own cluster")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.peers[leaderId] == nil {
		address, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return err
		}
		if err = server.ConnectToPeer(leaderId, address); err != nil {
			return err
		}
	}
	joinClusterArgs := JoinClusterArgs{ServerId: server.id, ServerAddr: server.listener.Addr()}
	var joinClusterReply JoinClusterReply
	if err := server.RPC(leaderId, "RaftNode.JoinCluster", joinClusterArgs, &joinClusterReply); err != nil {
		return err
	}
	if joinClusterReply.Success {
		server.peerList.Add(joinClusterReply.LeaderId)
		server.node.becomeFollower(joinClusterReply.Term)
		fetchPeerListArgs := FetchPeerListArgs{Term: server.node.currentTerm}
		var fetchPeerListReply FetchPeerListReply
		if err := server.RPC(joinClusterReply.LeaderId, "RaftNode.FetchPeerList", fetchPeerListArgs, &fetchPeerListReply); err != nil {
			return err
		}
		if fetchPeerListReply.Success {
			for peerId := range fetchPeerListReply.PeerList.peerSet {
				if peerId != server.id {
					server.peerList.Add(peerId)
				}
			}
		}
	}
	return nil
}
