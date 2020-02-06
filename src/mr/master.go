package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.

	sync.Mutex
	Files               []string
	WorkerNum           int
	TaskTimeout         []time.Time
	MapCompletedFlag    []bool
	ReduceCompletedFlag []bool
	MapTask             int
	ReduceTask          int
	MapCompleted        int
	ReduceCompleted     int
	NMap                int
	NReduce             int
}

func Max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler
//
// the RPC argument and reply types are defined in rpc.go.
//
/*
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
*/

func (m *Master) Schedule(args *MRArgs, reply *MRReply) error {

	//fmt.Println("ask for task")

	if args.Phase == mapPhase || args.Phase == registerPhase {
		m.Lock()

		if args.Phase == registerPhase {
			fmt.Printf("new Worker\n", m.WorkerNum)
			m.WorkerNum++
		} else if args.TaskNum != -1 {
			m.MapCompleted++
			m.MapCompletedFlag[args.TaskNum] = true
			fmt.Printf("map task %v completed\n", args.TaskNum)
		}

		if m.MapTask < m.NMap {
			//m.TaskTimeout[args.]
			reply.FileName = m.Files[m.MapTask]
			timeout, _ := time.ParseDuration("10s")
			m.TaskTimeout[m.MapTask] = time.Now().Add(timeout)
			reply.TaskNum = m.MapTask
			reply.NTask = m.NReduce
			//fmt.Printf("send map task %v %v\n", reply.TaskNum, reply.FileName)
			m.MapTask++
		} else if m.MapCompleted < m.NMap {

			//fmt.Println(m.MapCompletedFlag)
			for i := 0; i < m.NMap; i++ {
				now := time.Now()
				if m.TaskTimeout[i].Before(now) && !m.MapCompletedFlag[i] {
					timeout, _ := time.ParseDuration("10s")
					reply.FileName = m.Files[i]
					m.TaskTimeout[i] = time.Now().Add(timeout)
					reply.TaskNum = i
					reply.NTask = m.NReduce
					m.Unlock()
					return nil
				}
			}

			fmt.Println(m.MapCompletedFlag)
			//fmt.Println(m.TaskTimeout)
			//fmt.Println(time.Now())
			//fmt.Printf("map task %v %v\n", m.MapCompleted, m.NMap)
			reply.TaskNum = -1

		} else {
			reply.TaskNum = -2
		}
		m.Unlock()
	} else if args.Phase == reducePhase || args.Phase == waitReducePhase {
		m.Lock()

		if args.Phase == reducePhase && args.TaskNum != -1 {
			m.ReduceCompleted++
			m.ReduceCompletedFlag[args.TaskNum] = true
			//fmt.Println("reduce task complete, total completed %v, total task %v\n", m.ReduceCompleted, m.NReduce)
		}

		if m.MapCompleted < m.NMap {
			//fmt.Println("reduce task wait %v %v\n", m.MapCompleted, m.NMap)
			reply.TaskNum = -1
		} else if m.ReduceTask < m.NReduce {
			//fmt.Println("reduce task get")
			reply.TaskNum = m.ReduceTask
			reply.NTask = m.NMap
			timeout, _ := time.ParseDuration("10s")
			m.TaskTimeout[m.ReduceTask] = time.Now().Add(timeout)
			//fmt.Printf("send reduce task %v\n", reply.TaskNum)
			m.ReduceTask++
		} else if m.ReduceCompleted < m.NReduce {
			now := time.Now()
			for i := 0; i < m.NMap; i++ {
				if m.TaskTimeout[i].Before(now) && !m.ReduceCompletedFlag[i] {

					timeout, _ := time.ParseDuration("10s")
					m.TaskTimeout[i] = time.Now().Add(timeout)
					reply.TaskNum = i
					reply.NTask = m.NMap
					m.Unlock()
					return nil

				}
			}

			reply.TaskNum = -3
		} else {
			reply.TaskNum = -2
			//fmt.Println("reduce task complete")
		}

		m.Unlock()
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	m.Lock()
	if m.ReduceCompleted == m.NReduce {
		ret = true
	}
	//fmt.Println("check master done")
	m.Unlock()

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	fmt.Println(files)
	m := Master{}
	m.Files = files
	m.MapTask = 0
	m.ReduceTask = 0
	m.NReduce = nReduce
	m.NMap = len(files)
	m.WorkerNum = 0
	m.MapCompleted = 0
	m.ReduceCompleted = 0
	m.MapCompletedFlag = make([]bool, m.NMap)
	m.ReduceCompletedFlag = make([]bool, m.NReduce)
	m.TaskTimeout = make([]time.Time, Max(m.NMap, m.NReduce))

	for i := 0; i < m.NMap; i++ {
		m.MapCompletedFlag[i] = false
	}

	for i := 0; i < m.NReduce; i++ {
		m.ReduceCompletedFlag[i] = false
	}

	for i := 0; i < Max(m.NMap, m.NReduce); i++ {
		m.TaskTimeout[i] = time.Now()
	}

	//fmt.Println(m.files[0])

	// Your code here.

	m.server()
	return &m
}
