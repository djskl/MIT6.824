package mapreduce

import (
	"os"
	"encoding/json"
	"log"
	"io"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	map_kvs := make(map[string][]string)

	var tmp_kv KeyValue

	for mapIdx :=0; mapIdx <nMap; mapIdx++ {
		map_filename := reduceName(jobName, mapIdx, reduceTaskNumber)
		map_file, err := os.OpenFile(map_filename, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatalf("File Open Error: %s\n", err)
		}

		kv_dec := json.NewDecoder(map_file)

		for {
			err := kv_dec.Decode(&tmp_kv)

			if err == io.EOF {
				break
			}

			if err != nil {
				map_file.Close()
				log.Fatalf("Decode Error: %s\n", err)
			}

			k, v := tmp_kv.Key, tmp_kv.Value
			map_kvs[k] = append(map_kvs[k], v)
		}

		map_file.Close()
	}

	result_file, err := os.OpenFile(outFile, os.O_RDWR | os.O_CREATE, 0777)
	if err != nil {
		log.Fatalf("File Open Error: %s\n", err)
	}

	rst_enc := json.NewEncoder(result_file)

	for k, vs := range map_kvs {
		tmp := KeyValue{k, reduceF(k, vs)}
		rst_enc.Encode(tmp)
	}

	result_file.Close()

	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
}
