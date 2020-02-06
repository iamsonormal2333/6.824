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
	"time"
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

	args := MRArgs{}
	args.Phase = registerPhase

	reply := MRReply{}
	call("Master.Schedule", &args, &reply)
	//fmt.Printf("get map task %v\n", reply.NTask)

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	//fmt.Printf("get map task %v\n", reply.NTask)

	for reply.TaskNum != -2 {
		//fmt.Println("get map task")
		fmt.Printf("get map task %v %v\n", reply.TaskNum, reply.FileName)

		if reply.TaskNum == -1 {
			time.Sleep(time.Duration(3) * time.Second)
			fmt.Printf("worker wake up\n")
			args = MRArgs{}
			args.Phase = mapPhase
			args.TaskNum = -1

			reply = MRReply{}
			call("Master.Schedule", &args, &reply)
			continue
		}

		intermediate := []KeyValue{}
		filename := reply.FileName
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

		filesenc := make([]*json.Encoder, reply.NTask)
		files := make([]*os.File, reply.NTask)

		for i := 0; i < reply.NTask; i++ {
			fileName := "mr-" + strconv.Itoa(reply.TaskNum) + "-" + strconv.Itoa(i)
			fout, err := os.Create(fileName)
			if err != nil {
				fmt.Println(fileName, err)
				return
			}
			filesenc[i] = json.NewEncoder(fout)
			files[i] = fout
		}

		i := 0
		for i < len(intermediate) {
			j := i
			output := KeyValue{intermediate[i].Key, intermediate[i].Value}

			for ; j < len(intermediate) && intermediate[j].Key == intermediate[i].Key; j++ {

				err := filesenc[ihash(intermediate[i].Key)%reply.NTask].Encode(&output)
				if err != nil {
					fmt.Printf("%s Encode Failed %v\n", intermediate[i].Key, err)
				}
			}

			// this is the correct format for each line of Reduce output.

			i = j
		}

		for _, f := range files {
			f.Close()
		}

		args = MRArgs{}
		args.Phase = mapPhase
		args.TaskNum = reply.TaskNum

		reply = MRReply{}
		call("Master.Schedule", &args, &reply)
	}

	args = MRArgs{}
	args.Phase = waitReducePhase

	reply = MRReply{}

	call("Master.Schedule", &args, &reply)
	for reply.TaskNum == -1 {
		args = MRArgs{}
		args.Phase = waitReducePhase

		reply = MRReply{}
		call("Master.Schedule", &args, &reply)
	}

	for reply.TaskNum != -2 {

		if reply.TaskNum == -3 {
			time.Sleep(time.Duration(1) * time.Second)
			args = MRArgs{}
			args.Phase = reducePhase
			args.TaskNum = -1

			reply = MRReply{}
			call("Master.Schedule", &args, &reply)
			continue
		}

		fmt.Printf("get reduce task %v\n", reply.TaskNum)

		kva := []KeyValue{}
		for j := 0; j < reply.NTask; j++ {
			filename := "mr-" + strconv.Itoa(j) + "-" + strconv.Itoa(reply.TaskNum)
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

		oname := "mr-out-" + strconv.Itoa(reply.TaskNum)
		ofile, _ := os.Create(oname)

		i := 0

		fmt.Printf("reduce taks %v length %v\n", reply.TaskNum, len(kva))
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

		args = MRArgs{}
		args.Phase = reducePhase
		args.TaskNum = reply.TaskNum

		reply = MRReply{}
		call("Master.Schedule", &args, &reply)
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

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
