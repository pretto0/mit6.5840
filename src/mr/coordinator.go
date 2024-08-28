package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Task struct{
	TaskType Tasktype //任务阶段：Map,Reduce
	TaskState int	//任务状态,WorkState = 1,WaitState = 2,FinishState = 3
	TaskID int		//任务id，记录intermediate files
	ReduceNum int  //lab给定，用于生成Reducetask的个数
	Filenamelist []string //task对应的文件名称
	StartTime time.Time
}

type Tasktype int

const (
	MapState = 1
	ReduceState = 2
)

const (
	WorkState = 1
	WaitState = 2
	FinishState = 3
)

type Coordinator struct {
	// Your definitions here.
	CoordinatorState int // 1:map , 2:reduce , 3:finished
	Files []string

	MapperNum int //验证map第一阶段任务是否完成

	NumMapReducers int

	TaskIdForGen int

	MapTaskchan chan *Task
 	ReduceTaskchan chan *Task
	TaskMap map[int]*Task
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) PullTask(args *TaskRequest, reply *TaskReply) error {
	mu.Lock()
	defer mu.Unlock()
	// fmt.Printf("c.CoordinatorState: %d\n",c.CoordinatorState)
	switch c.CoordinatorState{
	case MapState:
		if len(c.MapTaskchan) > 0{
			taskp := <- c.MapTaskchan
			if taskp.TaskState == WaitState{
				reply.Response = WorkState
				reply.Task = *taskp
				taskp.TaskState = WorkState
				taskp.StartTime = time.Now()
				c.TaskMap[taskp.TaskID] = taskp
				// fmt.Printf("Map Task[%d] has been allocated.\n",taskp.TaskID)
			}
		} else {
			// fmt.Println("map task all done,begin checking...")
			reply.Response = WaitState
			if c.CheckTaskDone(MapState) {
				c.CoordinatorState = ReduceState
				c.CreateReduceTask()
			}
			return nil
		}
			//
	case ReduceState:
		if len(c.ReduceTaskchan) > 0{
			taskp := <- c.ReduceTaskchan
			if taskp.TaskState == WaitState{
				reply.Response = WorkState
				reply.Task = *taskp
				taskp.TaskState = WorkState
				taskp.StartTime = time.Now()
				c.TaskMap[taskp.TaskID] = taskp
				// fmt.Printf("Reduce Task[%d] has been allocated.\n",taskp.TaskID)
			}
		} else {
			reply.Response = WaitState
			if c.CheckTaskDone(ReduceState) {
				c.CoordinatorState = FinishState
				c.Done()
			}
			return nil
		}
		
	}
	return nil
}
 
func (c *Coordinator) CrashHandle(){
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.CoordinatorState == FinishState{
			mu.Unlock()
			break
		}

		for _,task := range c.TaskMap{
			if task.TaskState == WorkState && time.Since(task.StartTime) > 10*time.Second{
				// fmt.Printf("task %d is crashed\n",task.TaskID)
				task.TaskState = WaitState
				switch task.TaskType{
				case MapState:
					c.MapTaskchan <- task
				case ReduceState:
					c.ReduceTaskchan <- task
				}
				delete(c.TaskMap,task.TaskID)
			}
		}

		mu.Unlock()
	}
}


func (c *Coordinator) UpdateTask(args *FinArgs, reply *FinReply) error {
	mu.Lock()
	defer mu.Unlock()
	c.TaskMap[args.Id].TaskState = FinishState
	return nil
}

//确保channel里不再有任务，并且处理的任务数为预先设置好的任务数
func (c *Coordinator)CheckTaskDone(t Tasktype) bool {
	var (
		taskDoneNum = 0
		taskUnDoneNum = 0
	)
	// fmt.Printf("check state:%d\n",t)
	switch t{
	case MapState:
		for _,v := range c.TaskMap{
			if v.TaskType == MapState{
				if v.TaskState == FinishState{
					taskDoneNum++
				} else {
					taskUnDoneNum++
				}
			}
		}
		// fmt.Printf("Map taskDoneNum:%d\n",taskDoneNum)
		// fmt.Printf("Map c.MapperNum:%d\n",c.MapperNum)
		// fmt.Printf("Map taskUnDoneNum:%d\n",taskUnDoneNum)
		if taskDoneNum == c.MapperNum && taskUnDoneNum == 0 {
			return true
		}

	case ReduceState:
		for _,v := range c.TaskMap{
			if v.TaskType == ReduceState{
				if v.TaskState == FinishState{
					taskDoneNum++
				} else {
					taskUnDoneNum++
				}
			}
		}
		// fmt.Printf("Reduce taskDoneNum:%d\n",taskDoneNum)
		// fmt.Printf("Reduce c.MapperNum:%d\n",c.NumMapReducers)
		// fmt.Printf("Reduce taskUnDoneNum:%d\n",taskUnDoneNum)
		if taskDoneNum == c.NumMapReducers && taskUnDoneNum == 0 {
			return true
		}
	}

	for _,v := range c.TaskMap{
		if v.TaskType == MapState{
			if v.TaskState == FinishState{
				taskDoneNum++ 
			} else {
				taskUnDoneNum++
			}
		}
	}
	if taskDoneNum == c.MapperNum && taskUnDoneNum == 0 {
		return true
	}
	return false
}




//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.CoordinatorState == FinishState{
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(Files []string, nReduce int) *Coordinator {
	c := Coordinator{CoordinatorState: MapState,
		Files: Files,
		TaskIdForGen: 1,
		NumMapReducers: nReduce,
		MapTaskchan: make(chan *Task, len(Files)),
		ReduceTaskchan: make(chan *Task, nReduce),
		TaskMap: make(map[int]*Task, len(Files)+nReduce),
		MapperNum: len(Files),
	}

		// State int //0for start ,1 for map,2 for reduce
		// Files []string
	
		// NumMapWorkers int
		// NumMapReducers int
	
		// MapTask chan Task
		//  ReduceTask chan Task

	// Your code here.
	c.CreateMapTasks(Files)

	go c.CrashHandle()

	c.server()
	return &c
}

func (c *Coordinator)CreateMapTasks(files []string){
	for _,v := range(files){
		id := c.GenerateTaskId()
		task := Task{
			TaskType: 1,
			TaskID: id,
			TaskState: WaitState,
			ReduceNum: c.NumMapReducers,
			Filenamelist: []string{v},
		}
		c.TaskMap[id] = &task
		c.MapTaskchan <- &task
		// fmt.Println(v,"成功写入")
	}
}

func (c *Coordinator) GenerateTaskId()int{
	res := c.TaskIdForGen
	c.TaskIdForGen++
	return res
}

func (c *Coordinator) CreateReduceTask(){
	// fmt.Println("begin make reduce tasks...")
	rn := c.NumMapReducers
	dir, _ := os.Getwd()//Getwd返回当前路径
	files, err := os.ReadDir(dir)//ReadDir返回目录下的所有文件
	if err != nil{
		fmt.Println(err)
	}
	for i:=0;i<rn;i++{
		id := c.GenerateTaskId()
		filelist := []string{}
		for _,file := range files{
			if strings.HasPrefix(file.Name(),"mr-tmp") && strings.HasSuffix(file.Name(),strconv.Itoa(i)){
				filelist = append(filelist, file.Name())

			}
		}
		
		reduceTask := Task{
			TaskType: ReduceState, 
			TaskState: WaitState,
			TaskID: id,		
			ReduceNum: c.NumMapReducers,  
			Filenamelist: filelist, 
		}
		// fmt.Printf("create a reduce task id:%d\n",id)
		c.ReduceTaskchan <- &reduceTask
	}
}