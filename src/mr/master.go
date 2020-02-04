package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.

	sync.Mutex
	files           []string
	workerNum       int
	mapTask         int
	reduceTask      int
	mapCompleted    int
	reduceCompleted int
	nMap            int
	nReduce         int
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
func (m *Master) schedule(args *Args, reply *Reply) error {

	fmt.Println("ask for task")

	if args.phase == mapPhase || args.phase == registerPhase {
		m.Lock()
		if args.phase == registerPhase {
			m.workerNum++
		}
		if m.mapTask < m.nMap {
			reply.fileName = m.files[m.mapTask]
			reply.taskNum = m.mapTask
			reply.nTask = m.nReduce
			m.mapTask++
		} else {
			reply.fileName = ""
			m.mapCompleted++
		}
		m.Unlock()
	} else if args.phase == reducePhase {
		m.Lock()
		if m.mapCompleted < m.nMap {
			reply.taskNum = -1
		} else if m.reduceTask < m.nReduce {
			reply.taskNum = m.reduceTask
			reply.nTask = m.nMap
			m.reduceTask++
		} else {
			reply.taskNum = -2
			m.reduceCompleted++
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
	if m.reduceCompleted == m.nReduce {
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
	fmt.Println("make master")
	m := Master{}
	m.files = files
	m.mapTask = 0
	m.reduceTask = 0
	m.nReduce = nReduce
	m.nMap = len(files)
	m.workerNum = 0
	m.mapCompleted = 0
	m.reduceCompleted = 0
	fmt.Println(m.files[0])

	// Your code here.

	m.server()
	return &m
}
