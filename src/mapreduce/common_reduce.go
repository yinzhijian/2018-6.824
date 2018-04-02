package mapreduce

<<<<<<< HEAD
import (
    "os"
    "encoding/json"
    //"sort"
    //"fmt"
)
// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
    //kvmap := make(map[int][]KeyValue)
    kvmap := make(map[string][]string)
    var kvs []KeyValue
    for m := 0; m < nMap; m++ {
        inFileName := reduceName(jobName, m, reduceTaskNumber)
        inFile, err := os.Open(inFileName)
        check_err(err)
        enc := json.NewDecoder(inFile)
        enc.Decode(&kvs)
        //fmt.Printf("name:%s,kvslen:%d",inFileName, len(kvs))
        for _, kv := range kvs {
            kvmap[kv.Key] = append(kvmap[kv.Key],kv.Value)
            //fmt.Printf("name:%s,key:%s,value:%s",inFileName, kv.Key, kv.Value)
            //sort.Strings(kvmap[kv.Key])
        }
        //fmt.Printf("name:%s,len:%d",inFileName, len(kvmap))
    }
    file, err := os.Create(outFile)
    check_err(err)
	enc := json.NewEncoder(file)
	for key,kvs := range kvmap {
        enc.Encode(KeyValue{key, reduceF(key, kvs)})
	}
	file.Close()
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
