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
	mapPhase      jobPhase = "mapPhase"
	reducePhase            = "reducePhase"
	registerPhase          = "registerPhase"
)

type Args struct {
	//	phase标记当前阶段为map或是reduce
	phase jobPhase
}

type Reply struct {
	fileName string
	taskNum  int
	nTask    int
}

//
/*
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}
*/

// Add your RPC definitions here.
