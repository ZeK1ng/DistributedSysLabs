package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	// "encoding/gob"
)

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

type RequestTask struct {
}
type ResponseTask struct {
	RespTask MrTask
	Nreduce int
	IsDone bool
}
type ComplitedMapReq struct {
	ID int
}
type ComplitedMapRes struct{

}
type CompliteRedReq struct{
 ID int
}
type CompliteRedRes struct{
	
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	// gob.Register(RequestTask{})
	// gob.Register(ResponseTask{})
	return s
}
