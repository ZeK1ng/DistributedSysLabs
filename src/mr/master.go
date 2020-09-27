package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	// "io/ioutil"
)

type Master struct {
	// Your definitions here.
	Files        []string
	NReduce      int
	MutX         sync.Mutex
	WorkDone     bool
	Mtasks       []MrTask
	DoneMapTask  []MrTask
	RTasks       []MrTask
	MDone        int
	RDone        int
	MapsDone bool
	ReducesDone bool
	FilesAddress []string
	ReducerId int
}

type MrTask struct {
	TypeT string
	DoneT bool
	File  string
	Ind   int
	Assigned bool
}

// Your code here -- RPC handlers for the worker to call.
//
func(m *Master) WatchMan(index int , ttype string){
		timeLapse := time.NewTimer(time.Second * 10)
		<-timeLapse.C
		if(ttype == "MAP"){
			if(!m.Mtasks[index].DoneT){
				m.Mtasks[index].Assigned = false
			}
		}else{
			if(!m.RTasks[index].DoneT){
				m.RTasks[index].Assigned = false
			}
		}
}
func(m *Master) NotifyMapDone(req *ComplitedMapReq,res *ComplitedMapRes) error{
	m.MutX.Lock()
	taskId := req.ID
	m.Mtasks[taskId].DoneT=true
	m.MDone++
	if m.MDone == len(m.Mtasks){
		m.MapsDone = true
	}
	m.MutX.Unlock()
	return nil
}
func(m *Master) NotifyReducerDone(req *CompliteRedReq, res*CompliteRedRes) error{
	m.MutX.Lock()
	defer m.MutX.Unlock()
	taskID := req.ID
	m.RTasks[taskID].DoneT=true
	m.RDone++
	if m.RDone == m.NReduce{
		m.WorkDone = true
	}
	return nil
}
func (m *Master) GetTask(req *RequestTask, res *ResponseTask) error {
	m.MutX.Lock()
	defer m.MutX.Unlock()
	if(m.WorkDone){
		res.IsDone = true
		return nil
	}
	if !m.MapsDone {
		for i:= range m.Mtasks{
			if(!m.Mtasks[i].Assigned){
				m.Mtasks[i].Assigned = true
				res.RespTask = m.Mtasks[i]
				res.Nreduce=m.NReduce
				go m.WatchMan(i,"MAP")
				return nil
			}
		}
	}else if !m.ReducesDone{
		for i:= range m.RTasks{
			if(!m.RTasks[i].Assigned){
				m.RTasks[i].Assigned = true
				res.RespTask = m.RTasks[i]
				go m.WatchMan(i , "REDUCE")
				return nil
			}
		}
	}
	return nil
}




// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	m.MutX.Lock()
	defer m.MutX.Unlock()
	ret = m.WorkDone
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// fmt.Println(os.Getwd())
	// Dir,err := os.Getwd()
	// if(err != nil){
	// 	fmt.Println("sa")
	// }
	// fs,err:= ioutil.ReadDir(Dir)
	// for _,file := range fs{
	// 	fmt.Println(file.Name())
	// }
	m := Master{}
	m.Files = files
	m.NReduce = nReduce
	m.MutX = sync.Mutex{}
	m.WorkDone = false
	m.ReducerId = 0
	m.MDone = 0
	m.RDone  = 0
	createMapReduceTasks(&m, files, nReduce)
	// printTasks(&m)
	m.server()
	return &m
}

func createMapReduceTasks(m *Master, files []string, nReduce int) {
	for ind, file := range files {
		var newMtask MrTask
		newMtask = MrTask{}
		newMtask.Assigned = false
		newMtask.DoneT = false
		newMtask.File = file
		newMtask.Ind = ind
		newMtask.TypeT = "MAP"
		m.Mtasks = append(m.Mtasks, newMtask)
		// fmt.Println(newMtask)
	}
	// fmt.Println("asd")
	for i := 0; i < nReduce; i++ {
		var newRtask MrTask
		newRtask = MrTask{}
		newRtask.Assigned = false
		newRtask.DoneT = false
		newRtask.File = ""
		newRtask.TypeT = "REDUCE"
		newRtask.Ind = i
		m.RTasks = append(m.RTasks, newRtask)
		// fmt.Println(newRtask)
	}
}

func printTasks(m *Master) {
	for _, task := range m.Mtasks {
		fmt.Println(task)
	}
	fmt.Println("Printing r ")
	for _, task := range m.RTasks {
		fmt.Println(task)
	}
}
