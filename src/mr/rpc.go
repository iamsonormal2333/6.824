package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
type jobPhase string

const (
	mapPhase        jobPhase = "mapPhase"
	reducePhase              = "reducePhase"
	registerPhase            = "registerPhase"
	waitReducePhase          = "waitReducePhase"
)

type MRArgs struct {
	//	phase标记当前阶段为map或是reduce
	Phase   jobPhase
	TaskNum int
}

type MRReply struct {
	FileName string
	TaskNum  int
	NTask    int
}

//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
