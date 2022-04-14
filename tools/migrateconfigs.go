package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

func main() {
	response, err := http.Get("http://localhost:80/getAllStreams")

	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}

	responseData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	//fmt.Println(string(responseData))

	var streamConfigs []map[string]interface{}
	json.Unmarshal(responseData, &streamConfigs)

	for _, streamConfig := range streamConfigs {

		streamId := streamConfig["stream_id"].(map[string]interface{})["String"].(string)

		var newStreamConfig = make(map[string]interface{})

		for key, value := range streamConfig { //iterate through keys in the config

			_, mapType := value.(map[string]interface{})

			if mapType {
				if value.(map[string]interface{})["Valid"].(bool) { //check if a valid value exists
					
					for innerKey, innerValue := range value.(map[string]interface{}) { //interate the inner structure
						
						switch innerKey {
						case "String":
							newStreamConfig[key] = innerValue.(string)
							//log.Println(reflect.TypeOf(innerValue))
						case "Bool":
							newStreamConfig[key] = innerValue.(bool)
							//log.Println(reflect.TypeOf(innerValue))
						case "Int64":
							newStreamConfig[key] = innerValue.(float64)
							//log.Println(reflect.TypeOf(innerValue))
						}
					}
	
				}
	
			} else { //primitive type
				newStreamConfig[key] = value
			}

		}

		configFile, _ := json.MarshalIndent(newStreamConfig, "", " ")

		_ = ioutil.WriteFile("../storage/configs/"+streamId+".json", configFile, 0644)

	}

}
