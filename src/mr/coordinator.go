package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	"sync"
)

type State int

const (
	Idle State = iota
	InProcess
	Done
)

const (
	MapPhase    = 0 // 代表 Map 阶段
	ReducePhase = 1 // 代表 Reduce 阶段
	ExitPhase   = 2 // 代表所有任务完成，可以退出的阶段
)

type TaskStatus struct {
	State     State //0:Idle  1:InProcess 2:Done
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	//锁 保护好下面的切片
	mu sync.Mutex

	phase int // 0:IMap 1:Reduce 2:Alldone 3 wait

	mapTasks []TaskStatus //Map任务队列 //0 是Idle  1是InProcess 2是Complete

	reduceTasks []TaskStatus //Reduce任务队列 //0 是Idle  1是InProcess 2是Complete

	//任务伪队列 其实是切片
	//MapTaskStatus []int //0 是Idle  1是InProcess 2是Complete

	//文件名
	Files []string

	nmap int //一共有n个map任务

	nReduce int //一共有n个reduce

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) TaskDistribute(args *GetTaskArgs, reply *GetTaskReply) error {
	//fmt.Println("DEBUG: 收到一个任务请求 RPC")
	//fmt.Println("信号灯 A: Worker 请求到达，准备竞争锁...")
	c.mu.Lock()
	//fmt.Println("信号灯 B: Worker 成功拿到锁！")
	defer func() {
		c.mu.Unlock()
		//fmt.Println("信号灯 C: 锁已释放")
	}()
	switch c.phase {
	case MapPhase: //0为Map阶段
		for i := range c.mapTasks {
			if c.mapTasks[i].State == Idle {
				reply.TaskID = i
				reply.TaskType = "Map"
				reply.FileName = c.Files[i]
				reply.NMap = c.nmap
				reply.NReduce = c.nReduce
				c.mapTasks[i].State = InProcess
				c.mapTasks[i].StartTime = time.Now()
				//fmt.Printf("Coordinator: 派发 Map 任务 %d\n", i)
				return nil
			}
		}
		reply.TaskType = "Wait"

	case ReducePhase: // 1为Reduce阶段
		for i := range c.reduceTasks {
			if c.reduceTasks[i].State == Idle {
				reply.TaskID = i
				reply.TaskType = "Reduce"
				reply.NMap = c.nmap
				reply.NReduce = c.nReduce
				c.reduceTasks[i].State = InProcess
				c.reduceTasks[i].StartTime = time.Now()
				//fmt.Printf("Coordinator: 派发 Reduce 任务 %d\n", i)
				return nil
			}
		}

		// 2. Reduce 活发完了但还没收全，继续 Wait
		reply.TaskType = "Wait"

	case ExitPhase:
		reply.TaskType = "NoJob"

	default:
		// 容错处理
		reply.TaskType = "Wait"

	}
	//fmt.Printf("DEBUG: 派发响应类型为 %v, 任务ID为 %d\n", reply.TaskType, reply.TaskID)
	return nil
}

func (c *Coordinator) TaskAfterDone(args *TellTaskComplete, reply *KonwTaskComplete) error {

	c.mu.Lock()
	//fmt.Printf("收到任务汇报: %v ID: %d\n", args.TaskType, args.TaskID)
	defer c.mu.Unlock()

	//安全检验 如果回报的任务类型和当前的任务阶段不符合 直接忽略
	if args.TaskType == "Map" && c.phase != MapPhase {
		return nil
	}
	if args.TaskType == "Reduce" && c.phase != ReducePhase {
		return nil
	}

	//更新对应的任务状态
	if args.TaskType == "Map" {
		c.mapTasks[args.TaskID].State = Done
		//fmt.Printf("Coordinator: Map 任务 %d 已完成\n", args.TaskID)
	}
	if args.TaskType == "Reduce" {
		c.reduceTasks[args.TaskID].State = Done
		//fmt.Printf("Coordinator: Reduce 任务 %d 已完成\n", args.TaskID)
	}

	//检查是否需要切换阶段
	if c.allDone() {
		if c.phase == MapPhase {
			c.phase = ReducePhase
			fmt.Println("Coordinator: 所有 Map 任务完成，进入 Reduce 阶段")
		} else if c.phase == ReducePhase {
			c.phase = ExitPhase
			fmt.Println("Coordinator: 所有 Reduce 任务完成，准备退出")
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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
	//fmt.Println("Coordinator: RPC 服务已在 Unix Socket 启动监听")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == ExitPhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//fmt.Println("Coordinator: 正在初始化...") // 加这行

	c := Coordinator{
		Files:       files,
		nmap:        len(files),
		nReduce:     nReduce,
		phase:       0,
		mapTasks:    make([]TaskStatus, len(files)),
		reduceTasks: make([]TaskStatus, nReduce),
	}

	for i := 0; i < c.nmap; i++ {
		c.mapTasks[i].State = Idle
	} //初始化任务状态

	//2.核心 启动后台 检查超时
	go c.serverCheck()
	// Your code here.

	c.server() //启动rpc监听
	//fmt.Println("Coordinator: 初始化完成，等待 Worker 连接...")
	return &c
}

func (c *Coordinator) serverCheck() {

	for {
		time.Sleep(500 * time.Millisecond) //不要一直扫描

		c.mu.Lock()

		//如果整个大任务都结束了 就可以直接结束扫描任务了
		if c.phase == ExitPhase {
			c.mu.Unlock()
			return
		}

		//根据当前任务阶段 决定看那张表
		var tasks []TaskStatus
		if c.phase == MapPhase {
			tasks = c.mapTasks
		} else if c.phase == ReducePhase {
			tasks = c.reduceTasks
		}

		//开始逐个检查任务
		for i := range tasks {
			if tasks[i].State == InProcess {
				if time.Since(tasks[i].StartTime) > 10*time.Second {
					fmt.Printf("巡逻兵发现：任务 %d 超时了！正在回收...\n", i)
					tasks[i].State = Idle
				}
			}
		}
		c.mu.Unlock()
	}

}

func (c *Coordinator) allDone() bool {

	//c.mu.Lock()

	if c.phase == MapPhase {
		for _, t := range c.mapTasks {
			if t.State != Done {
				return false
			}
		}
	} else if c.phase == ReducePhase {
		for _, t := range c.reduceTasks {
			if t.State != Done {
				return false
			}
		}
	}
	//c.mu.Unlock()
	return true
}
