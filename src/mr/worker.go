package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	fmt.Println("make worker")

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	args := Args{}
	args.phase = registerPhase

	reply := Reply{}

	call("Master.schedule", &args, &reply)

	fmt.Printf("get map task %v\n", reply.nTask)

	for reply.fileName != "" {
		fmt.Println("get map task")
		intermediate := []KeyValue{}
		filename := reply.fileName
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
		sort.Sort(ByKey(intermediate))

		filesenc := make([]*json.Encoder, reply.nTask)

		for i := 0; i < reply.nTask; i++ {
			fileName := "mr-" + strconv.Itoa(reply.taskNum) + "-" + strconv.Itoa(i)
			fout, err := os.Create(fileName)
			if err != nil {
				fmt.Println(fileName, err)
				return
			}
			filesenc[i] = json.NewEncoder(fout)
		}

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}

			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			err := filesenc[ihash(intermediate[i].Key)%reply.nTask].Encode(&output)
			if err != nil {
				fmt.Printf("%s Encode Failed %v\n", intermediate[i].Key, err)
			}

			i = j
		}

		args = Args{}
		args.phase = reducePhase

		reply = Reply{}
		call("Master.schedule", &args, &reply)
	}

	args = Args{}
	args.phase = reducePhase

	reply = Reply{}

	call("Master.schedule", &args, &reply)
	for reply.taskNum == -1 {
		args = Args{}
		args.phase = reducePhase

		reply = Reply{}
		call("Master.schedule", &args, &reply)
	}

	for reply.taskNum != -2 {
		kva := []KeyValue{}
		for j := 0; j < reply.taskNum; j++ {
			filename := "mr-" + strconv.Itoa(j) + "-" + strconv.Itoa(reply.taskNum)
			file, err := os.Open(filename)
			if err != nil {
				break
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			file.Close()
		}
		sort.Sort(ByKey(kva))

		oname := "mr-out-" + strconv.Itoa(reply.taskNum)
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}

		args = Args{}
		args.phase = reducePhase

		reply = Reply{}
		call("Master.schedule", &args, &reply)
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}
*/

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
