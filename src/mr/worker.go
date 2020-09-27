package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
	"sort"
	"encoding/json"
 	"io/ioutil"

)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for{
		req := RequestTask{}
		res := ResponseTask{}
		cs:=call("Master.GetTask", &req, &res)
		if cs == false {
			os.Exit(-2)
		}
		if(res.IsDone){
			return
		}
		if res.RespTask.TypeT == "MAP"{
			
			doMap(&res.RespTask,mapf,res.Nreduce)
			mapperReq := ComplitedMapReq{res.RespTask.Ind}
			mapperRes := ComplitedMapRes{}
			call("Master.NotifyMapDone",&mapperReq,&mapperRes)

		}else if res.RespTask.TypeT == "REDUCE"{
			
			doReduce(&res.RespTask,reducef)
			reducerReq := CompliteRedReq{res.RespTask.Ind}
			reducerRes:= CompliteRedRes{}
			call("Master.NotifyReducerDone",&reducerReq,&reducerRes)
		}else{
			fmt.Println("sleeping")
			time.Sleep(time.Second * 100)
		}
	}
}
func doMap(task *MrTask,mapf func(string, string) []KeyValue,nRed int){
	filename := task.File
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
		sort.Sort(ByKey(kva))
		// deleteOldIntermediateFiles(nRed,task.Ind)
		hashTable :=make(map[int][] KeyValue)
		for _,cont :=range kva {
			hashedIndex :=ihash(cont.Key) % nRed 
			hashTable[hashedIndex] = append(hashTable[hashedIndex], cont)
		}
		createInterMediateFiles(hashTable,nRed, task.Ind)
}

func createInterMediateFiles(hashTabel map[int][]KeyValue,nRed int,taskID int){
	for i:= 0 ; i<nRed; i++{
		tempInterFile := fmt.Sprintf("temp-mr-%v",taskID)
		tempOutFile,err := ioutil.TempFile(".",tempInterFile)
		if(err != nil){
			log.Fatalf("File couldn`t be created")
		}
		enc :=json.NewEncoder(tempOutFile)
		for _, cont :=range hashTabel[i]{
			err:=enc.Encode(cont)
			if(err != nil){
				log.Fatalf("Can`t write to file")
			}
		}
		interFileName := fmt.Sprintf("mr-%v-%v",taskID,i)
		os.Rename(tempOutFile.Name(),interFileName)
		tempOutFile.Close()
	}
}

func deleteOldIntermediateFiles(nRed int ,taskID int){
	for i:=0; i<nRed; i++ {
		fname := fmt.Sprintf("mr-%v-%v",taskID,i)
		os.Remove(fname)
	}
}

func doReduce(task *MrTask,reducef func(string, []string) string){
	Dir,err:=os.Getwd() 
	if(err != nil){
		log.Fatalf("Cant get path to directory")
	}
	directory ,err0 :=ioutil.ReadDir(Dir)
	if(err0 != nil){
		log.Fatalf("Cant open current Directory")
	}
	outPutFile := fmt.Sprintf("mr-out-%v",task.Ind)
	var kva	[]KeyValue
	for _,file :=range directory{
		mapInd := -1
		redInd := -1
		match,err01 :=fmt.Sscanf(file.Name(),"mr-%v-%v",&mapInd,&redInd)
		if(err01 == nil && redInd == task.Ind && match >0 ){
			currFile,err1 :=os.Open(file.Name())
			if(err1 != nil){
				log.Fatalf("Cant open File")
			}
			decoder := json.NewDecoder(currFile)
			for{
				var kv KeyValue
				if err2:=decoder.Decode(&kv); err2!=nil{
					break
				}
				kva = append(kva,kv)
			}
			currFile.Close()
		}
	}
	sort.Sort(ByKey(kva))
	// removeError:=os.Remove(outPutFile)
	// if removeError != nil{
	// 	log.Fatalf("Can`t delete last outpuT File")
	// }
	leng := len(kva)
	hashTable := make(map[string]string)
	for i:= 0; i< leng;{
		var j int
		vals := []string{}
		j = i
		for j < leng && kva[i].Key==kva[j].Key {
			vals = append(vals,kva[j].Value)
			j++
		}
		out:= reducef(kva[i].Key,vals)
		hashTable[kva[i].Key]=out	
		i =j
	}
	tempOutFile :=fmt.Sprintf("reduce-temp-%v",task.Ind)
	tempFile, tempfileError := ioutil.TempFile(".",tempOutFile)
	
	if(tempfileError == nil){
		log.Printf("temp file created")
	}
	for key, val := range hashTable {
		fmt.Fprintf(tempFile, "%v %v\n", key, val)
	}
	renameError := os.Rename(tempFile.Name(),outPutFile)
	if renameError != nil{
		log.Fatalf("Cant rename")
	}
	tempFile.Close()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
