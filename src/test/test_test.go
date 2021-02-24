
package zz

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond
//func TestFailAgree2B(t *testing.T) {
//	servers := 3
//	cfg := make_config(t, servers, false)
//	defer cfg.cleanup()
//
//	cfg.begin("Test (2B): agreement despite follower disconnection")
//
//	cfg.one(101, servers, false)
//
//	// disconnect one follower from the network.
//	leader := cfg.checkOneLeader()
//	cfg.disconnect((leader + 1) % servers)
//
//	// the leader and remaining follower should be
//	// able to agree despite the disconnected follower.
//	cfg.one(102, servers-1, false)
//	cfg.one(103, servers-1, false)
//	time.Sleep(RaftElectionTimeout)
//	cfg.one(104, servers-1, false)
//	cfg.one(105, servers-1, false)
//
//	// re-connect
//	cfg.connect((leader + 1) % servers)
//
//	// the full set of servers should preserve
//	// previous agreements, and be able to agree
//	// on new commands.
//	time.Sleep(RaftElectionTimeout)
//	cfg.one(106, servers, true)
//
//	cfg.one(107, servers, true)
//
//	cfg.end()
//}

//
//func TestPersist22C(t *testing.T) {
//	servers := 5
//	cfg := make_config(t, servers, false)
//	defer cfg.cleanup()
//
//	cfg.begin("Test (2C): more persistence")
//
//	index := 1
//	for iters := 0; iters < 5; iters++ {
//		cfg.one(10+index, servers, true)
//		index++
//
//		leader1 := cfg.checkOneLeader()
//		//fmt.Println(cfg.logs[0],cfg.logs[1],cfg.logs[2],cfg.logs[3],cfg.logs[4])
//		cfg.disconnect((leader1 + 1) % servers)
//		cfg.disconnect((leader1 + 2) % servers)
//
//		cfg.one(10+index, servers-2, true)
//		index++
//		//fmt.Println(cfg.logs[0],"leader1:",leader1)
//		//fmt.Println(cfg.logs[1])
//		//fmt.Println(cfg.logs[2])
//		//fmt.Println(cfg.logs[3])
//		//fmt.Println(cfg.logs[4])
//		//fmt.Println()
//		cfg.disconnect((leader1 + 0) % servers)
//		cfg.disconnect((leader1 + 3) % servers)
//		cfg.disconnect((leader1 + 4) % servers)
//
//		cfg.start1((leader1 + 1) % servers)
//		cfg.start1((leader1 + 2) % servers)
//		cfg.connect((leader1 + 1) % servers)
//		cfg.connect((leader1 + 2) % servers)
//		//fmt.Println("dffsdf",cfg.rafts[0].ident,cfg.rafts[1].ident,cfg.rafts[2].ident,cfg.rafts[3].ident,cfg.rafts[4].ident)
//		time.Sleep(RaftElectionTimeout)
//		cfg.start1((leader1 + 3) % servers)
//		cfg.connect((leader1 + 3) % servers)
//		//fmt.Println("dffsdf",cfg.rafts[0].ident,cfg.rafts[1].ident,cfg.rafts[2].ident,cfg.rafts[3].ident,cfg.rafts[4].ident)
//		cfg.one(10+index, servers-2, true)
//		index++
//		//fmt.Println(cfg.logs[0],"leader1:",leader1)
//		//fmt.Println(cfg.logs[1])
//		//fmt.Println(cfg.logs[2])
//		//fmt.Println(cfg.logs[3])
//		//fmt.Println(cfg.logs[4])
//		//fmt.Println()
//		cfg.connect((leader1 + 4) % servers)
//		cfg.connect((leader1 + 0) % servers)
//		//fmt.Println(cfg.logs[0],"leader1:",leader1)
//		//fmt.Println(cfg.logs[1])
//		//fmt.Println(cfg.logs[2])
//		//fmt.Println(cfg.logs[3])
//		//fmt.Println(cfg.logs[4])
//		//fmt.Println()
//		//
//		//fmt.Println("end..............",cfg.rafts[0].ident,cfg.rafts[1].ident,cfg.rafts[2].ident,cfg.rafts[3].ident,cfg.rafts[4].ident)
//		//fmt.Println(cfg.logs[0],"leader1:",leader1)
//		//fmt.Println(cfg.logs[1])
//		//fmt.Println(cfg.logs[2])
//		//fmt.Println(cfg.logs[3])
//		//fmt.Println(cfg.logs[4])
//		//fmt.Println()
//		time.Sleep(150*time.Millisecond)
//	}
//
//	cfg.one(1000, servers, true)
//
//	cfg.end()
//}

func TestFigure8Unreliable2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Figure 8 (unreliable)")

	cfg.one(rand.Int()%10000, 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			cfg.setlongreordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			_, _, ok := cfg.rafts[i].Start(rand.Int() % 10000)
			if ok && cfg.connected[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			cfg.disconnect(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.connected[s] == false {
				cfg.connect(s)
				nup += 1
			}
		}
		//if iters%5==0{
		//	fmt.Println(cfg.logs[0])
		//	fmt.Println(cfg.logs[1])
		//	fmt.Println(cfg.logs[2])
		//	fmt.Println(cfg.logs[3])
		//	fmt.Println(cfg.logs[4])
		//	fmt.Println()
		//	fmt.Println()
		//}
	}

	for i := 0; i < servers; i++ {
		if cfg.connected[i] == false {
			cfg.connect(i)
		}
	}
	fmt.Println("ddddddddddddddd.........................................")


	cfg.one(111%10000, servers, true)

	cfg.end()
}