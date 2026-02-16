package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//fmt.Println("Worker: 启动成功，准备开始请求任务...")
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for { //死循环 Worker会一直找coordinator要任务，然后做任务，做完任务回复，然后等待两秒继续呼叫
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := call("Coordinator.TaskDistribute", &args, &reply)
		if ok { //收到了任务派发，开始做任务

			switch reply.TaskType {
			case "Map":
				//fmt.Printf("Worker: 得到 Map 任务，ID %d, 文件 %s\n", reply.TaskID, reply.FileName)
				err := DoMap(mapf, reply.FileName, reply.NReduce, reply.TaskID) //开始Map任务
				if err != nil {
					fmt.Printf("Map task failed: %v\n", err)
				} else {
					//汇报任务结束
					CallTaskDone("Map", reply.TaskID)
				}

			case "Reduce":
				//fmt.Printf("Worker: 得到 Reduce 任务，ID %d\n", reply.TaskID)
				err := DoRuduce(reducef, reply.TaskID, reply.NMap) //开始Reduce任务
				if err != nil {
					fmt.Printf("Reduce task failed: %v\n", err)
				} else {
					//汇报任务结束
					CallTaskDone("Reduce", reply.TaskID)
				}

			case "Wait":
				//fmt.Println("Worker: 没活干，睡觉中...")
				time.Sleep(time.Second) //暂时没活 master让我等
				// 如果所有任务都在进行中（InProgress），Master 会让你等。
				// 如果不 sleep，Worker 会疯狂发请求（忙轮询），把 CPU 跑满。
			case "NoJob": //都做完了
				return
			}

		} else {
			fmt.Printf("Catch Task failed!\n")
		}

	}

}
func DoMap(mapf func(string, string) []KeyValue, FileName string, nReduce int, TaskNum int) error {
	//读文件

	content := readFile(FileName)
	//用mapf把文本割成一堆键值对
	kva := mapf(FileName, content)

	//准备nRecude个桶，负责装哈希值一样的键值对
	buckets := make([][]KeyValue, nReduce)
	for i := range buckets {
		buckets[i] = []KeyValue{}
	}
	//按照 ihash(key) % nReduce 分成 nReduce 组
	//每个桶装好键值对
	for _, kv := range kva {
		bucketID := ihash(kv.Key) % nReduce
		buckets[bucketID] = append(buckets[bucketID], kv)
	}

	//落盘 将桶写进文件里面
	for i := 0; i < nReduce; i++ {
		temFile, err := ioutil.TempFile(".", "mr-tmp-*")
		if err != nil {
			return fmt.Errorf("create temp file err: %v", err)
		}

		enc := json.NewEncoder(temFile)
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				return fmt.Errorf("Encode temp file err: %v", err)
			}
		}

		temFile.Close()

		//原子重命名
		finalName := fmt.Sprintf("mr-%d-%d", TaskNum, i)
		os.Rename(temFile.Name(), finalName)

	}

	return nil
}

func DoRuduce(reducef func(string, []string) string, TaskId int, MapNum int) error {

	var intermediate []KeyValue //准备一个大容器 装所有的Kv-Value

	//去所有的Map那里处理
	for i := 0; i < MapNum; i++ {
		//1.拼凑出文件名
		filename := fmt.Sprintf("mr-%d-%d", i, TaskId)
		//2.打开文件
		file, err := os.Open(filename)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			} else {
				return fmt.Errorf("open file err: %v", err)
			}
		}
		//创建endecode解码的对象
		dec := json.NewDecoder(file)

		//循环解码
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break // 这个文件读完了，跳出内层循环
				}
				return fmt.Errorf("decode file err: %v", err)
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	//得到了一个所有key-value的大容器  要开始进行排序
	sort.Slice(intermediate, func(i, j int) bool { return intermediate[i].Key < intermediate[j].Key })

	//排序成功 开始写入文件  也同样是先用tempflie
	tempFile, err := ioutil.TempFile(".", "mr-out-tmp-*")
	if err != nil {
		return fmt.Errorf("create temp output file err: %v", err)
	}

	//有了临时文件  往里面写
	i := 0
	for i < len(intermediate) {
		j := i + 1
		// 寻找所有 Key 相同的连续区间 [i, j)
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}

		//已经找到一组 [i,j]的key值了 可以进行数字统计和压缩了
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//目前values已经是一个键值对组合，利用reducef进行合并
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	finalName := fmt.Sprintf("mr-out-%d", TaskId)
	os.Rename(tempFile.Name(), finalName)

	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func CallTaskDone(taskType string, taskNum int) {
	args := TellTaskComplete{
		TaskType: taskType,
		TaskID:   taskNum,
	}
	reply := KonwTaskComplete{}

	call("Coordinator.TaskAfterDone", &args, &reply)

}

func readFile(Filename string) string {
	file, err := os.Open(Filename)
	if err != nil {
		fmt.Printf("无法打开文件:%s", err)
		return ""
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("无法读取文件:%s", err)
		return ""
	}

	return string(content)
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
