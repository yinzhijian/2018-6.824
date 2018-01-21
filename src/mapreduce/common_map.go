package mapreduce

import (
	"hash/fnv"
    "io/ioutil"
    "os"
    "encoding/json"
    //"fmt"
)
func check_err(e error) {
    if e != nil {
        panic(e)
    }
}

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce tasks that will run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	//
	// You will need to write this function.
	//
	// The intermediate output of a map task is stored as multiple
	// files, one per destination reduce task. The file name includes
	// both the map task number and the reduce task number. Use the
	// filename generated by reduceName(jobName, mapTaskNumber, r) as
	// the intermediate file for reduce task r. Call ihash() (see below)
	// on each key, mod nReduce, to pick r for a key/value pair.
	//
	// mapF() is the map function provided by the application. The first
	// argument should be the input file name, though the map function
	// typically ignores it. The second argument should be the entire
	// input file contents. mapF() returns a slice containing the
	// key/value pairs for reduce; see common.go for the definition of
	// KeyValue.
	//
	// Look at Go's ioutil and os packages for functions to read
	// and write files.
	//
	// Coming up with a scheme for how to format the key/value pairs on
	// disk can be tricky, especially when taking into account that both
	// keys and values could contain newlines, quotes, and any other
	// character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//
    dat, err := ioutil.ReadFile(inFile)
    check_err(err)
    kvs := mapF(inFile, string(dat))
    m := make(map[int][]KeyValue)
    for _, kv := range kvs {
        r := ihash(kv.Key) % nReduce
        m[r] = append(m[r], kv)
        //fmt.Printf("key:%s,value:%s,r:%d\n", kv.Key, kv.Value, r)
    }
    for r, mKvs := range m {
        name := reduceName(jobName, mapTaskNumber, r)
        f, err := os.Create(name)
        check_err(err)
        defer f.Close()
        enc := json.NewEncoder(f)
        //fmt.Printf("name:%s,r:%d,len:%d\n", name, r,len(mKvs))
        err = enc.Encode(mKvs)
        check_err(err)
    }
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
