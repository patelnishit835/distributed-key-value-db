package main

import (
	"bufio"
	"dds/raft"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

const shardSize = 10

var maxNodeInd = 100

func PrintMenu() {
	fmt.Println("\n\n           	DDS MENU: [nodes are 0 indexed]")
	fmt.Println("+----+----------------------+------------------------------------+")
	fmt.Println("| Sr |  USER COMMANDS       |      ARGUMENTS                     |")
	fmt.Println("+----+----------------------+------------------------------------+")
	fmt.Println("| 1  | create clusters      |      cluster count, list of nodes  |")
	fmt.Println("| 2  | set data             |      key, value                    |")
	fmt.Println("| 3  | get data             |      key                           |")
	fmt.Println("| 4  | disconnect peer      |      clusterId, peerId             |")
	fmt.Println("| 5  | reconnect peer       |      clusterId, peerId             |")
	fmt.Println("| 6  | crash peer           |      clusterId, peerId             |")
	fmt.Println("| 7  | restart peer         |      clusterId, peerId             |")
	fmt.Println("| 8  | shutdown             |      clusterId                     |")
	fmt.Println("| 9  | check leader         |      clusterId                     |")
	fmt.Println("| 10 | stop execution       |      clusterId                     |")
	fmt.Println("| 11 | add servers          |      clusterId, [peerIds]          |")
	fmt.Println("| 12 | remove servers       |      clusterId, [peerIds]          |")
	fmt.Println("+----+----------------------+------------------------------------+")
	fmt.Println("")
	fmt.Println("+--------------------      USER      ----------------------------+")
	fmt.Println("+                                                                +")
	fmt.Println("+ User input should be of the format:  Sr ...Arguments           +")
	fmt.Println("+ Example:  2 4 1 3                                              +")
	fmt.Println("+----------------------------------------------------------------+")
	fmt.Println("")
}

// PrettyPrint prints an object in a pretty JSON format
func PP(v interface{}) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println(string(b))
}

// create cluster with peers
func CreateCluster(clusterId uint64, servers []uint64) (*raft.ClusterSimulator, error) {
	if len(servers) <= 0 {
		return nil, errors.New("number of peers must be greater than 0")
	}
	return raft.CreateNewCluster(nil, clusterId, servers), nil
}

// disconnect a peer from the cluster
func DisconnectPeer(cluster *raft.ClusterSimulator, peerId int) error {
	if cluster == nil {
		return errors.New("raft cluster not created")
	}
	if peerId < 0 {
		return errors.New("invalid peer id passed")
	}
	err := cluster.DisconnectPeer(uint64(peerId))
	return err
}

// reconnect a disconnected peer to the cluster
func ReconnectPeer(cluster *raft.ClusterSimulator, peerId int) error {
	if cluster == nil {
		return errors.New("raft cluster not created")
	}
	if peerId < 0 {
		return errors.New("invalid peer id passed")
	}
	err := cluster.ReconnectPeer(uint64(peerId))
	return err
}

// crash a server
func CrashPeer(cluster *raft.ClusterSimulator, peerId int) error {
	if cluster == nil {
		return errors.New("raft cluster not created")
	}
	if peerId < 0 {
		return errors.New("invalid peer id passed")
	}
	err := cluster.CrashPeer(uint64(peerId))
	return err
}

// restart a server
func RestartPeer(cluster *raft.ClusterSimulator, peerId int) error {
	if cluster == nil {
		return errors.New("raft cluster not created")
	}
	if peerId < 0 {
		return errors.New("invalid peer id passed")
	}
	err := cluster.RestartPeer(uint64(peerId))
	return err
}

// shutdown all servers in the cluster and stop raft
func Shutdown(cluster *raft.ClusterSimulator) error {
	if cluster == nil {
		return errors.New("raft cluster not created")
	}
	cluster.Shutdown()
	cluster = nil
	return nil
}

// check leader of raft cluster
func CheckLeader(cluster *raft.ClusterSimulator) (int, int, error) {
	if cluster == nil {
		return -1, -1, errors.New("raft cluster not created")
	}
	return cluster.CheckUniqueLeader()
}

// shutdown all servers in the cluster and stop raft and stop execution
func Stop(cluster *raft.ClusterSimulator) error {
	if cluster == nil {
		return nil
	}
	cluster.Shutdown()
	cluster = nil
	return nil
}

func SetData(clusterMap map[int]*raft.ClusterSimulator, keyStr string, val string, serverParam ...int) error {
	key, err := strconv.Atoi(keyStr)
	if err != nil {
		return err
	}

	commandToServer := raft.Write{Key: keyStr, Val: val}

	clusterId := key / 10

	var cluster *raft.ClusterSimulator
	var ok bool

	if _, ok = clusterMap[clusterId]; !ok {
		clusterMap[clusterId] = raft.CreateNewCluster(nil, uint64(clusterId), []uint64{uint64(maxNodeInd) + 1, uint64(maxNodeInd) + 2, uint64(maxNodeInd) + 3})
		maxNodeInd += 3
	}

	cluster = clusterMap[clusterId]

	serverId, _, err := cluster.CheckUniqueLeader()
	if err != nil {
		return err
	}

	if serverId < 0 {
		return errors.New("unable to submit command to any server")
	}
	success := false
	if success, _, _ = cluster.SubmitToServer(clusterId, serverId, commandToServer); success {
		return nil
	} else {
		return errors.New("command could not be submitted, try different server(leader)")
	}
}

func GetData(clusterMap map[int]*raft.ClusterSimulator, keyStr string, serverParam ...int) (string, error) {
	key, err := strconv.Atoi(keyStr)
	if err != nil {
		return "", err
	}

	commandToServer := raft.Read{Key: keyStr}

	clusterId := key / 10

	var cluster *raft.ClusterSimulator
	var ok bool
	if cluster, ok = clusterMap[clusterId]; !ok {
		return "", fmt.Errorf("key %s does not exist", keyStr)
	}

	serverId, _, err := cluster.CheckUniqueLeader()
	if err != nil {
		return "", err
	}

	if serverId < 0 {
		return "", errors.New("unable to submit command to any server")
	}

	if success, reply, err := cluster.SubmitToServer(clusterId, serverId, commandToServer); success {
		if err != nil {
			return "", err
		} else {
			value, _ := reply.(string)
			return value, nil
		}
	} else {
		return "", errors.New("command could not be submitted, try different server(leader)")
	}
}

// add new server to the raft cluster
func AddServers(clusterMap map[int]*raft.ClusterSimulator, clusterId int, serverIds []int) error {
	var cluster *raft.ClusterSimulator
	var ok bool

	if cluster, ok = clusterMap[clusterId]; !ok {
		return fmt.Errorf("key %d does not exist", clusterId)
	}

	commandToServer := raft.AddServers{ServerIds: serverIds}
	var err error
	serverId, _, err := cluster.CheckUniqueLeader()

	if err != nil {
		return err
	}

	if serverId < 0 {
		return errors.New("unable to submit command to any server")
	}

	if success, _, err := cluster.SubmitToServer(clusterId, serverId, commandToServer); success {
		if err != nil {
			return err
		} else {
			return nil
		}
	} else {
		return errors.New("command could not be submitted, try different server")
	}
}

// remove server from the raft cluster
func RemoveServers(clusterMap map[int]*raft.ClusterSimulator, clusterId int, serverIds []int) error {
	var cluster *raft.ClusterSimulator
	var ok bool

	if cluster, ok = clusterMap[clusterId]; !ok {
		return errors.New("raft cluster not created")
	}

	commandToServer := raft.RemoveServers{ServerIds: serverIds}
	var err error
	serverId, _, err := cluster.CheckUniqueLeader()

	if err != nil {
		return err
	}

	if serverId < 0 {
		return errors.New("unable to submit command to any server")
	}

	if success, _, err := cluster.SubmitToServer(clusterId, serverId, commandToServer); success {
		if err != nil {
			return err
		} else {
			return nil
		}
	} else {
		return errors.New("command could not be submitted, try different server")
	}
}

func main() {
	var input string
	var clusterMap = map[int]*raft.ClusterSimulator{}

	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	gob.Register(raft.Write{})
	gob.Register(raft.Read{})
	gob.Register(raft.AddServers{})
	gob.Register(raft.RemoveServers{})

	PrintMenu()

	// numClusters := 3
	// peers := []uint64{0, 1, 2}
	// clusterMap[0], _ = CreateCluster(0, peers)
	// peers = []uint64{3, 4, 5}
	// clusterMap[1], _ = CreateCluster(1, peers)
	// peers = []uint64{6, 7, 8}
	// clusterMap[2], _ = CreateCluster(2, peers)

	// fmt.Println("cmap ===> ")
	// PP(clusterMap[0])

	// err := SetData(clusterMap, "1", "Nishit")
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// err = SetData(clusterMap, "15", "Prateet")
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// err = SetData(clusterMap, "25", "Krunal")
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// err = SetData(clusterMap, "500", "Naitik")
	// err = SetData(clusterMap, "1024", "Nishit 1")

	// time.Sleep(2 * time.Second)

	// val, err := GetData(clusterMap, "1024")
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// fmt.Println("val ===> ", val)

	go func() {
		<-sigCh
		fmt.Println("SIGNAL RECEIVED")
		err := os.RemoveAll("db")
		if err != nil {
			fmt.Println("Error in removing db directory", err)
		}
		os.Exit(0)
	}()

	// os.Exit(0)

	for {
		fmt.Println("WAITING FOR INPUTS..")
		fmt.Println("")

		reader := bufio.NewReader(os.Stdin)
		input, _ = reader.ReadString('\n')
		tokens := strings.Fields(input)
		command, err0 := strconv.Atoi(tokens[0])
		if err0 != nil {
			fmt.Println("Wrong input")
			continue
		}

		switch command {
		case 1:
			if len(tokens) < 2 {
				fmt.Println("number of clusters not passed")
				break
			}
			numClusters, err := strconv.Atoi(tokens[1])
			if err != nil {
				fmt.Println("Invalid number of clusters")
				break
			}
			for i := 0; i < numClusters; i++ {
				fmt.Printf("Enter node IDs for cluster %d (space separated): ", i)
				nodeInput, _ := reader.ReadString('\n')
				nodeTokens := strings.Fields(nodeInput)
				var nodes []uint64
				for _, node := range nodeTokens {
					nodeId, err := strconv.ParseUint(node, 10, 64)
					if err != nil {
						fmt.Println("Invalid node ID")
						break
					}
					nodes = append(nodes, nodeId)
				}
				clusterMap[i], _ = CreateCluster(uint64(i), nodes)
			}
		case 2:
			if len(tokens) < 3 {
				fmt.Println("key or value not passed")
				break
			}
			err := SetData(clusterMap, tokens[1], tokens[2])
			if err == nil {
				fmt.Printf("WRITE TO KEY %s WITH VALUE %s SUCCESSFUL\n", tokens[1], tokens[2])
			} else {
				fmt.Printf("%v\n", err)
			}

		case 3:
			if len(tokens) < 2 {
				fmt.Println("key not passed")
				break
			}
			val, err := GetData(clusterMap, tokens[1])
			if err == nil {
				fmt.Printf("READ KEY %s VALUE %s\n", tokens[1], val)
			} else {
				fmt.Printf("%v\n", err)
			}

		case 4:
			if len(tokens) < 3 {
				fmt.Println("cluster id or peer id not passed")
				break
			}

			clusterId, err := strconv.Atoi(tokens[1])
			if err != nil /*|| clusterId >= numClusters*/ {
				fmt.Printf("invalid cluster id %d passed\n", tokens[1])
				break
			}

			fmt.Println("clusterId ===> ", clusterId)

			peer, err := strconv.Atoi(tokens[2])
			if err != nil {
				fmt.Printf("invalid server id %d passed\n", tokens[2])
				break
			}

			var cluster *raft.ClusterSimulator
			var ok bool
			if cluster, ok = clusterMap[peer/shardSize]; !ok {
				fmt.Println("cluster not found")
				break
			}

			err = DisconnectPeer(cluster, peer)
			if err == nil {
				fmt.Printf("PEER %d DISCONNECTED\n", peer)
			} else {
				fmt.Printf("%v\n", err)
			}

		case 5:
			if len(tokens) < 3 {
				fmt.Println("cluster id or peer id not passed")
				break
			}
			clusterId, err := strconv.Atoi(tokens[1])
			if err != nil /*|| clusterId >= numClusters*/ {
				fmt.Printf("invalid cluster id %d passed\n", clusterId)
				break
			}

			peer, err := strconv.Atoi(tokens[2])
			if err != nil /*|| peer >= peers*/ {
				fmt.Printf("invalid server id %d passed\n", peer)
				break
			}

			var cluster *raft.ClusterSimulator
			var ok bool
			if cluster, ok = clusterMap[peer/shardSize]; !ok {
				fmt.Println("cluster not found")
				break
			}
			err = ReconnectPeer(cluster, peer)
			if err == nil {
				fmt.Printf("PEER %d RECONNECTED\n", peer)
			} else {
				fmt.Printf("%v\n", err)
			}

		case 6:
			if len(tokens) < 3 {
				fmt.Println("cluster id or peer id not passed")
				break
			}
			clusterId, err := strconv.Atoi(tokens[1])
			if err != nil {
				fmt.Printf("invalid cluster id %d passed\n", clusterId)
				break
			}

			peer, err := strconv.Atoi(tokens[2])
			if err != nil {
				fmt.Printf("invalid server id %d passed\n", peer)
				break
			}

			var cluster *raft.ClusterSimulator
			var ok bool
			if cluster, ok = clusterMap[peer/shardSize]; !ok {
				fmt.Println("cluster not found")
				break
			}

			err = CrashPeer(cluster, peer)
			if err == nil {
				fmt.Printf("PEER %d CRASHED\n", peer)
			} else {
				fmt.Printf("%v\n", err)
			}

		case 7:
			if len(tokens) < 3 {
				fmt.Println("cluster id or peer id not passed")
				break
			}
			clusterId, err := strconv.Atoi(tokens[1])
			if err != nil {
				fmt.Printf("invalid cluster id %d passed\n", clusterId)
				break
			}

			peer, err := strconv.Atoi(tokens[2])
			if err != nil {
				fmt.Printf("invalid server id %d passed\n", peer)
				break
			}

			var cluster *raft.ClusterSimulator
			var ok bool
			if cluster, ok = clusterMap[peer/shardSize]; !ok {
				fmt.Println("cluster not found")
				break
			}

			err = RestartPeer(cluster, peer)
			if err == nil {
				fmt.Printf("PEER %d RESTARTED\n", peer)
			} else {
				fmt.Printf("%v\n", err)
			}

		case 8:
			if len(tokens) < 1 {
				fmt.Println("cluster id not passed")
				break
			}

			clusterId, err := strconv.Atoi(tokens[1])
			if err != nil {
				fmt.Printf("invalid cluster id %d passed\n", clusterId)
				break
			}

			err = Shutdown(clusterMap[clusterId])
			if err == nil {
				fmt.Printf("CLUSTER %d SHUTDOWN\n", clusterId)
			} else {
				fmt.Printf("%v\n", err)
			}

		case 9:
			if len(tokens) < 2 {
				fmt.Println("cluster id not passed")
				break
			}

			clusterId, err := strconv.Atoi(tokens[1])
			if err != nil {
				fmt.Printf("invalid cluster id %d passed\n", clusterId)
				break
			}

			leader, term, err := CheckLeader(clusterMap[clusterId])
			if err == nil {
				fmt.Printf("LEADER: %d TERM: %d\n", leader, term)
			} else {
				fmt.Printf("%v\n", err)
			}

		case 10:
			if len(tokens) < 2 {
				fmt.Println("cluster id not passed")
				break
			}

			clusterId, err := strconv.Atoi(tokens[1])
			if err != nil {
				fmt.Printf("invalid cluster id %d passed\n", clusterId)
				break
			}

			err = Stop(clusterMap[clusterId])
			if err == nil {
				fmt.Printf("CLUSTER %d STOPPED\n", clusterId)
			} else {
				fmt.Printf("%v\n", err)
			}

		case 11:
			if len(tokens) < 3 {
				fmt.Println("cluster id or peer id not passed")
				break
			}

			clusterId, err := strconv.Atoi(tokens[1])
			if err != nil {
				fmt.Printf("invalid cluster id %d passed\n", clusterId)
				break
			}

			serverIds := make([]int, len(tokens)-2)
			var val int
			for i := 2; i < len(tokens); i++ {
				val, err = strconv.Atoi(tokens[i])
				if err != nil {
					fmt.Println("Invalid server ID")
					break
				}
				serverIds[i-2] = val
			}

			err = AddServers(clusterMap, clusterId, serverIds)
			if err == nil {
				fmt.Printf("Added ServerIDs: %v to cluster", serverIds)
			} else {
				fmt.Printf("%v\n", err)
			}

		case 12:
			if len(tokens) < 3 {
				fmt.Println("cluster id or peer id not passed")
				break
			}

			clusterId, err := strconv.Atoi(tokens[1])
			if err != nil {
				fmt.Printf("invalid cluster id %d passed\n", clusterId)
				break
			}

			serverIds := make([]int, len(tokens)-2)
			var val int
			for i := 2; i < len(tokens); i++ {
				val, err = strconv.Atoi(tokens[i])
				if err != nil {
					fmt.Println("Invalid server ID")
					break
				}
				serverIds[i-2] = val
			}

			err = RemoveServers(clusterMap, clusterId, serverIds)
			if err == nil {
				fmt.Printf("Removed ServerIDs: %v to cluster", serverIds)
			} else {
				fmt.Printf("%v\n", err)
			}

		case 13:
			if len(tokens) < 2 {
				fmt.Println("key not passed")
				break
			}
			err := SetData(clusterMap, tokens[1], "delete")
			if err == nil {
				fmt.Printf("DELETION OF KEY %s SUCCESSFUL\n", tokens[1])
			} else {
				fmt.Printf("%v\n", err)
			}

		default:
			fmt.Println("Invalid Command")
		}

		fmt.Println("\n---------------------------------------------------------")
		// PrintMenu()
	}
}
