package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Progress struct {
	// one and only one of File or ReducePieceNum should be assigned
	File           string
	ReducePieceNum int
	AssignedWorker int
}

type Stage string

const (
	SMap    Stage = "Map"
	SReduce Stage = "Reduce"
	SFinish Stage = "Finish"
)

type Coordinator struct {
	// Your definitions here.
	l       sync.Mutex
	nReduce int
	stage   Stage

	// For map jobs
	// map task num counter
	mapCounter int
	// map task completion counter
	mapCompleteCounter int
	totalFileNum       int
	MapFiles           []string
	// MapInProgress is map jobs, the key is map job number
	MapInProgress map[int]Progress
	MapTimers     map[int]*time.Timer

	// For reduce jobs
	reduceCompeteCounter int
	// there are nReduce pieces
	ReducePieces []int
	// ReduceInProgress is reduce jobs, the key is reduce piece number
	ReduceInProgress map[int]Progress
	ReduceTimers     map[int]*time.Timer
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil

}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RetrieveJob(args *GetJobArgs, reply *GetJobReply) error {
	reply.NReduce = c.nReduce
	c.l.Lock()
	defer c.l.Unlock()
	switch c.stage {
	case SMap:
		if len(c.MapFiles) == 0 {
			// all files are issued, but some of them are not finished
			reply.JobType = WaitType
			return nil
		}
		// Issue one map job
		reply.JobType = MapType
		mapNum := c.GetMapJobNum()
		mapFile := c.MapFiles[0]
		c.MapFiles = append(c.MapFiles[:0], c.MapFiles[1:]...)
		c.MapInProgress[mapNum] = Progress{
			File:           mapFile,
			AssignedWorker: args.WorkerID,
		}
		reply.Map = Map{
			File:    mapFile,
			TaskNum: mapNum,
		}

		// Start one timer in case this job fail
		t := time.NewTimer(10 * time.Second)
		c.MapTimers[mapNum] = t
		go func(file string) {
			<-t.C
			// Requeue the map job for this file
			c.l.Lock()
			c.MapFiles = append(c.MapFiles, file)
			c.l.Unlock()
		}(mapFile)
	case SReduce:
		if len(c.ReducePieces) == 0 {
			// all reduce job are issued but not finished
			reply.JobType = WaitType
			return nil
		}
		reply.JobType = ReduceType

		reducePiece := c.ReducePieces[0]
		c.ReducePieces = append(c.ReducePieces[:0], c.ReducePieces[1:]...)
		c.ReduceInProgress[reducePiece] = Progress{
			ReducePieceNum: reducePiece,
			AssignedWorker: args.WorkerID,
		}

		reply.Reduce = Reduce{
			ReducePiece: reducePiece,
		}

		// set a reduce timer
		t := time.NewTimer(10 * time.Second)
		c.ReduceTimers[reducePiece] = t
		go func(rPiece int) {
			<-t.C
			// Requeue the reduce job for this file
			c.l.Lock()
			c.ReducePieces = append(c.ReducePieces, rPiece)
			c.l.Unlock()
		}(reducePiece)
	case SFinish:
		reply.Done = true
	}
	return nil
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	c.l.Lock()
	defer c.l.Unlock()
	switch args.JobType {
	case MapType:
		taskNum := args.Map.TaskNum
		t, ok := c.MapTimers[taskNum]
		if ok {
			t.Stop()
			delete(c.MapTimers, taskNum)
			c.mapCompleteCounter++
		}
		fmt.Printf("Done map job: %d/%d\n", c.mapCompleteCounter, c.totalFileNum)

		// check all finished
		//fmt.Println("Check stage")
		if c.mapCompleteCounter == c.totalFileNum {

			fmt.Println("stage changed to SReduce")
			c.stage = SReduce
		}
		//fmt.Println(c.stage)
	case ReduceType:
		reducePiece := args.Reduce.ReducePiece
		t, ok := c.ReduceTimers[reducePiece]
		if ok {
			t.Stop()
			delete(c.ReduceTimers, reducePiece)
			c.reduceCompeteCounter++
			fmt.Printf("Done reduce job: %d/%d\n", c.reduceCompeteCounter, c.nReduce)
		}

		if c.reduceCompeteCounter == c.nReduce {
			fmt.Println("stage change to SFinish")
			c.stage = SFinish
		}
	}
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

func (c *Coordinator) GetMapJobNum() int {
	count := c.mapCounter
	c.mapCounter++
	return count
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	// Your code here.
	c.l.Lock()
	defer c.l.Unlock()
	return c.stage == SFinish

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:            SMap,
		nReduce:          nReduce,
		totalFileNum:     len(files),
		MapFiles:         files,
		MapInProgress:    map[int]Progress{},
		MapTimers:        map[int]*time.Timer{},
		ReducePieces:     []int{},
		ReduceInProgress: map[int]Progress{},
		ReduceTimers:     map[int]*time.Timer{},
	}
	for i := 0; i < nReduce; i++ {
		c.ReducePieces = append(c.ReducePieces, i)
	}

	// Your code here.

	c.server()
	return &c
}
