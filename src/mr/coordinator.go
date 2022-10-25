package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Coordinator struct {
	ReducerNum        int
	Phase             Phase
	ReduceTaskChannel chan *Task
	MapTaskChannel    chan *Task
	TaskMetaHolder    TaskMetaHolder
	files             []string
	TaskId            int // auto increase id for task
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReducerNum:        nReduce,
		Phase:             MapPhase,
		ReduceTaskChannel: make(chan *Task, nReduce),
		MapTaskChannel:    make(chan *Task, len(files)),
		files:             files,
		TaskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}

	c.makeMapTask(files)
	c.server()

	go c.CrashDetector()
	return &c
}

type TaskMetaInfo struct {
	State     State
	StartTime time.Time
	TaskAdr   *Task
}

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

func (t *TaskMetaHolder) acceptMeta(info *TaskMetaInfo) bool {
	taskId := info.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		return false
	}

	t.MetaMap[taskId] = info
	return true
}

func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.State != Waitting {
		return false
	}
	taskInfo.State = Working
	taskInfo.StartTime = time.Now()
	return true
}

func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	for _, v := range t.MetaMap {
		if v.TaskAdr.TaskType == MapTask {
			if v.State == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.State == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}

	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
		return true
	}

	return false
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2) //avoid offen get mutex
		mu.Lock()
		if c.Phase == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.TaskMetaHolder.MetaMap {
			if v.State == Working && time.Since(v.StartTime) > 9*time.Second {
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAdr.TaskId, time.Since(v.StartTime))
				switch v.TaskAdr.TaskType {
				case MapTask:
					c.MapTaskChannel <- v.TaskAdr
					v.State = Waitting
				case ReduceTask:
					c.ReduceTaskChannel <- v.TaskAdr
					v.State = Waitting
				}
			}
		}
		mu.Unlock()
	}
}

func (c *Coordinator) PollTask(args *TaskArgs, resp *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.Phase {
	case MapPhase:
		if len(c.MapTaskChannel) > 0 {
			*resp = *<-c.MapTaskChannel
			if !c.TaskMetaHolder.judgeState(resp.TaskId) {
				fmt.Printf("Map-taskid[ %d ] is running\n", resp.TaskId)
			}
		} else {
			resp.TaskType = WaittingTask
			if c.TaskMetaHolder.checkTaskDone() {
				c.toNextPhase()
			}
			return nil
		}

	case RedecePhase:
		if len(c.ReduceTaskChannel) > 0 {
			*resp = *<-c.ReduceTaskChannel
			if !c.TaskMetaHolder.judgeState(resp.TaskId) {
				fmt.Printf("Reduce-taskid[ %d ] is running\n", resp.TaskId)
			}
		} else {
			resp.TaskType = WaittingTask
			if c.TaskMetaHolder.checkTaskDone() {
				c.toNextPhase()
			}
			return nil
		}

	case AllDone:
		resp.TaskType = ExitTask
	default:
		panic("Undefined Phase")
	}

	return nil
}

func (c *Coordinator) makeMapTask(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{v},
		}

		taskMetaInfo := TaskMetaInfo{
			State:   Waitting,
			TaskAdr: &task,
		}

		c.TaskMetaHolder.acceptMeta(&taskMetaInfo)
		c.MapTaskChannel <- &task
	}
}

func (c *Coordinator) makeReduceTask() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    id,
			FileSlice: selectReduceName(i),
		}

		taskMetaInfo := TaskMetaInfo{
			State:   Waitting,
			TaskAdr: &task,
		}

		c.TaskMetaHolder.acceptMeta(&taskMetaInfo)
		c.ReduceTaskChannel <- &task
	}
}

func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}

	return s
}

func (c *Coordinator) MarkFinished(args *Task, resp *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch args.TaskType {
	case MapTask:
		meta, ok := c.TaskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.State == Working {
			meta.State = Done
		}
		break
	case ReduceTask:
		meta, ok := c.TaskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.State == Working {
			meta.State = Done
		}
		break
	default:
		panic("Undefined task type")
	}

	return nil
}

func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (c *Coordinator) toNextPhase() {
	if c.Phase == MapPhase {
		c.makeReduceTask()
		c.Phase = RedecePhase
	} else if c.Phase == RedecePhase {
		c.Phase = AllDone
	}
}

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
	mu.Lock()
	defer mu.Unlock()
	if c.Phase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	}
	return false
}