package main

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"testing"
)
import "time"
import "math/rand"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond


func TestBackup2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): leader backs up quickly over incorrect follower logs")
	cfg.one(rand.Int(), servers, true)
	// put leader and one follower in a partition
	leader1 := cfg.checkOneLeader()
	fmt.Println("le1",leader1)
	//fmt.Println(cfg.logs[0],cfg.logs[1],cfg.logs[2],cfg.logs[3],cfg.logs[4])

	//fmt.Println(cfg.rafts[0].log[len(cfg.rafts[0].log)-1].command,cfg.rafts[1].log[len(cfg.rafts[1].log)-1].term,
	//cfg.rafts[2].log[len(cfg.rafts[2].log)-1].term,cfg.rafts[3].log[len(cfg.rafts[3].log)-1].term,
		//len(cfg.rafts[4].log))
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	// submit lots of commands that won't commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int()%10000)
	}
	time.Sleep(500*time.Millisecond)
	fmt.Println(cfg.rafts[0].log)
	fmt.Println(cfg.rafts[1].log)
	fmt.Println(cfg.rafts[2].log)
	fmt.Println(cfg.rafts[3].log)
	fmt.Println(cfg.rafts[4].log)
	//fmt.Println(cfg.logs[0],cfg.logs[1],cfg.logs[2],cfg.logs[3],cfg.logs[4])
fmt.Println("pauseeeeeeeeeeeeeeeee")

	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)

	// allow other partition to recover
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)
	time.Sleep(RaftElectionTimeout /2)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}
	//fmt.Println(cfg.logs[0])
	//fmt.Println()
	//fmt.Println(cfg.logs[1])
	//fmt.Println()
	//fmt.Println(cfg.logs[2])
	//fmt.Println()
	//fmt.Println(cfg.logs[3])
	//fmt.Println()
	//fmt.Println(cfg.logs[4])


	// now another partitioned leader and one follower
	leader2 := cfg.checkOneLeader()
	fmt.Println("le2",leader2)
	//fmt.Println(cfg.rafts[0].log[len(cfg.rafts[0].log)-1].term,cfg.rafts[1].log[len(cfg.rafts[1].log)-1].term,
		//cfg.rafts[2].log[len(cfg.rafts[2].log)-1].term,cfg.rafts[3].log[len(cfg.rafts[3].log)-1].term,
		//cfg.rafts[4].log[len(cfg.rafts[4].log)-1].term)

	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	cfg.disconnect(other)
	fmt.Println("oth",other)
	time.Sleep(500*time.Millisecond)
	// lots more commands that won't commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(rand.Int()%10000)
	}
	fmt.Println(cfg.rafts[0].log)
	fmt.Println(cfg.rafts[1].log)
	fmt.Println(cfg.rafts[2].log)
	fmt.Println(cfg.rafts[3].log)
	fmt.Println(cfg.rafts[4].log)


	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)
	time.Sleep(RaftElectionTimeout /2)
	// lots of successful commands to new group.
	fmt.Println("lookwhathappen.................................................")
	fmt.Println(cfg.rafts[0].log)
	fmt.Println()
	fmt.Println(cfg.rafts[1].log)
	fmt.Println()
	fmt.Println(cfg.rafts[2].log)
	fmt.Println()
	fmt.Println(cfg.rafts[3].log)
	fmt.Println()
	fmt.Println(cfg.rafts[4].log)
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}
	led3:=cfg.checkOneLeader()
	fmt.Println("le3  dddddddddddddddddddddddddddd",led3)
	// now everyone
	fmt.Println(cfg.rafts[0].log)
	println()
	fmt.Println(cfg.rafts[1].log)
	println()
	fmt.Println(cfg.rafts[2].log)
	println()
	fmt.Println(cfg.rafts[3].log)
	println()
	fmt.Println(cfg.rafts[4].log)
	println()
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	led4:=cfg.checkOneLeader()
	fmt.Println("le4",led4)

	cfg.one(88888, servers, true)

	cfg.end()
}
