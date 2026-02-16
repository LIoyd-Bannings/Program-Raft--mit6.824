package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type GetTaskArgs struct{}//master收到的信号，收到它意味着worker要任务了，要发送下面的GetTaskReply 给他们安排任务

//matser给的回复信号
type GetTaskReply struct{
	TaskType string //任务类型
	NReduce int //任务数量
	TaskID int //任务的编号
	FileName string//文件名
	NMap int//干Map的任务总数
}

//worker把任务结束后，给master发送报告
type TellTaskComplete struct{
	TaskType string//完成的什么任务
	TaskID   int//完成的任务ID
}

//matser发送的回复
type KonwTaskComplete struct{}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
