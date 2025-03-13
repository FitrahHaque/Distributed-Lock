package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type LockCommandType int

const (
	LockAcquire LockCommandType = iota
	LockRelease
)

type LockAcquireRequest struct {
	CommandType LockCommandType `json:"commandType"`
	Key         string          `json:"key"`
	ClientID    string          `json:"clientId"`
	TTL         time.Duration   `json:"ttl"`
}

var Conn *websocket.Conn
var ClientID string

func connectToLockingService(serverId int) error {
	port := fmt.Sprintf("%d", 50050+serverId)
	url := fmt.Sprintf("ws://localhost:%s/ws", port)
	var err error
	Conn, _, err = websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}

	log.Printf("Connected to server %d at %v\n", serverId, url)
	err = Conn.WriteMessage(websocket.TextMessage, []byte(ClientID))
	if err != nil {
		return err
	}
	log.Printf("Sent client ID: %s", ClientID)
	return nil
}

func acquireLock(key string, n int64) {
	if Conn == nil {
		fmt.Printf("locking Service connection missing\n")
		return
	}
	defer Conn.Close()
	lockReq := LockAcquireRequest{
		CommandType: LockAcquire,
		Key:         key,
		ClientID:    ClientID,
		TTL:         time.Duration(n * int64(time.Second)),
	}

	data, err := json.Marshal(lockReq)
	if err != nil {
		fmt.Printf("json marshal error: %v\n", err)
		return
	}

	err = Conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		fmt.Printf("error sending lock command: %v\n", err)
		return
	}
	log.Printf("Sent lock acquire command: %s", data)

	for {
		_, message, err := Conn.ReadMessage()
		if err != nil {
			fmt.Printf("read error: %v\n", err)
			return
		}
		fmt.Printf("Received notification: %s\n", message)
		return
	}
}

func PrintMenu() {
	fmt.Println("\n\n           	                 CLIENT MENU:")
	fmt.Println("+--------------------------------------+------------------------------------+")
	fmt.Println("| Sr |  USER COMMANDS                  |      ARGUMENTS                     |")
	fmt.Println("+----+---------------------------------+------------------------------------+")
	fmt.Println("| 1  | create client                   |      clientId                      |")
	fmt.Println("| 2  | connect to locking service      |      serverId                      |")
	fmt.Println("| 3  | acquire lock                    |      lockKey, TTL (in secs)        |")
	fmt.Println("+----+---------------------------------+------------------------------------+")
	fmt.Println("+---------------------------------------------------------------------------+")
	fmt.Println("")
}

func ClientInput(sigCh chan os.Signal) {
	go func() {
		<-sigCh
		fmt.Println("SIGNAL RECEIVED")
		os.Exit(0)
	}()
	for {
		PrintMenu()
		fmt.Println("WAITING FOR INPUTS..")
		fmt.Println("")

		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		tokens := strings.Fields(input)
		command, err0 := strconv.Atoi(tokens[0])
		if err0 != nil {
			fmt.Println("Wrong input")
			continue
		}
		switch command {
		case 1:
			if len(tokens) < 2 {
				fmt.Println("ClientID not passed")
				break
			}
			ClientID = tokens[1]
			break
		case 2:
			if len(tokens) < 2 {
				fmt.Println("Locking Server port not passed")
				break
			}
			serverId, err := strconv.Atoi(tokens[1])
			if err != nil {
				fmt.Println("invalid number of peers")
				break
			}
			if err := connectToLockingService(serverId); err != nil {
				fmt.Printf("error sending client ID: %v", err)
				break
			}
		case 3:
			if len(tokens) < 3 {
				fmt.Printf("Lock Key and TTL not passed")
				break
			}
			ttl, err := strconv.Atoi(tokens[2])
			if err != nil {
				fmt.Println("invalid TTL")
				break
			}
			go acquireLock(tokens[1], int64(ttl))
		default:
			fmt.Printf("Invalid input")
		}
	}
}
