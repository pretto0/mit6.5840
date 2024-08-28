package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	loop := true
	for loop {
		r := RequestTask()
		// fmt.Printf("cur task state:%d\n",r.Response)
		switch r.Response{

		case WorkState:
			task := r.Task
			switch r.Task.TaskType{
			case MapState:
				DoMapTask(&task,mapf)
			case ReduceState:
				DoReduceTask(&task, reducef)
			}

		case WaitState:
			time.Sleep(time.Second)

		case FinishState:
			loop = false

		default:
			fmt.Println("request fail")
		}
	// fmt.Println("worker loop done.")	
	}
}


//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func RequestTask() TaskReply{
	args := TaskRequest{}
	reply := TaskReply{}
	ok := call("Coordinator.PullTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("task id %d\n", reply.Task.TaskID)
	} else {
		fmt.Printf("GetTask failed!\n")
	}
	return reply
}

func ReportTask(id int){
	args := FinArgs{Id: id}
	reply := FinReply{}
	// fmt.Printf("report id %d done.\n",id)
	ok := call("Coordinator.UpdateTask", &args, &reply)
	if !ok{
		fmt.Printf("id %d update failed!",id)
	}
}


func DoMapTask(t *Task,mapf func(string,string) []KeyValue){
	filename := t.Filenamelist[0]
	// fmt.Println(filename)
	file,err := os.Open(filename)
	if err != nil{
		log.Fatalf("cannot open file %v",filename)
	}
	content, err := io.ReadAll(file)
	if err != nil{
		log.Fatalf("cannot read file %v",filename)
	}
	file.Close()
	reduceNum := t.ReduceNum
	intermediate := mapf(filename, string(content))
	HashKv := make([][]KeyValue, reduceNum)
	for _,v := range(intermediate){
		index := ihash(v.Key) % reduceNum
		HashKv[index] = append(HashKv[index],v)
	}

	for i:=0 ; i<reduceNum;i++{
		filename := "mr-tmp-" + strconv.Itoa(t.TaskID) + "-" + strconv.Itoa(i)
		new_file,err := os.Create(filename)
		if err != nil{
			log.Fatal("can't create file:",err)
		}
		enc := json.NewEncoder(new_file)
		for _ ,kv := range(HashKv[i]){
			err := enc.Encode(&kv)
			if err != nil{
				log.Fatal("encode failed:",err)
			}
		}
		new_file.Close()
	}
	ReportTask(t.TaskID)
}

func DoReduceTask(t *Task,reducef func(string,[]string) string){
	// intermediate := []KeyValue{}

	intermediate := shuffle(t.Filenamelist)

	dir,_ := os.Getwd()

	tmpfile,err := os.CreateTemp(dir,"mr-out-tmp-")

	if err != nil{
		log.Fatal("failed to create temp file",err)
	}

	i := 0
	for i<len(intermediate){
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key{
			j++
		}
		values := []string{}
		for k:=i;k<j;k++{
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tmpfile,"%v %v\n",intermediate[i].Key,output)
		i = j
	}
	tmpfile.Close()
	oname := "mr-out-" + strconv.Itoa(t.TaskID)
	os.Rename(dir+tmpfile.Name(),dir+oname)
	ReportTask(t.TaskID)
}



//shuffle拿到文件名后访问对应的文件，将内容通过json库读出key和value，多个文件的结果按Key sort之后输出。
func shuffle(files []string) []KeyValue{
	kva := []KeyValue{}
	for _,filename := range files{
		file,err := os.Open(filename)
		if err != nil {
			log.Fatalf("can not open file %v",filename)
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil{
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
