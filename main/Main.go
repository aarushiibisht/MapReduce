package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"learning.project/mapReduce/mapper"
	"learning.project/mapReduce/reducer"
	"learning.project/mapReduce/userFunctions"
	"strconv"
	"strings"
)

func main(){
	var userSelection = ""
	var fileLocation = ""
	var numberOfMappers = ""
	var numberOfReducers = ""
	fmt.Println("Choose one of the task")
	fmt.Println("1. Word Count")
	fmt.Scanln(&userSelection)
	fmt.Println("Enter number of mappers")
	fmt.Scanln(&numberOfMappers)
	fmt.Println("Enter number of reducers")
	fmt.Scanln(&numberOfReducers)
	fmt.Println("Enter the name of the file to be used as input")
	fmt.Scanln(&fileLocation)

	mappers, _:= strconv.Atoi(numberOfMappers)
	reducers, _ := strconv.Atoi(numberOfReducers)
	result, err := startExecution(fileLocation, userSelection, mappers, reducers)
	if err != nil{
		fmt.Println("Failed to perform the task due to: " + err.Error())
	}else{
		fmt.Println(result)
	}
}

func startExecution(fileLocation string, task string, numberOfMappers int, numberOfReducers int)(map[string]int64, error){

	mappingInputChan := make(chan string)
	mappingOutputChan := make(chan map[string]int64)
	reducersInputChan := make([] chan map[string]int64, 0)
	reducersOutputChan := make(chan map[string]int64)
	finishedMappers := 0
	finishedReducers := 0
	quitM := make(chan bool)
	quitR := make(chan bool)
	finalResult := make(map[string]int64)
	var mapFuncObj userFunctions.MapperReducer

	switch task {
	case "1":
		mapFuncObj = userFunctions.WordCount{}
	default:
		return nil, errors.New("unimplemented or invalid task")
	}
	for i:=0; i< numberOfMappers;i++{
		go mapper.HandleMapping(mappingInputChan, mapFuncObj, mappingOutputChan, quitM)
	}
	for i:=0; i<numberOfReducers;i++{
		reducersInputChan = append(reducersInputChan, make(chan map[string]int64))
		go reducer.HandleReducing(reducersInputChan[i], mapFuncObj, reducersOutputChan, quitR)
	}
	fileContent, err := readFile(fileLocation)
	if err != nil {
		return nil, err
	}
	go divideFileContent(fileContent, mappingInputChan)
	OuterLoop1:
		for {
			select {
			case mapperResult := <-mappingOutputChan:
				fmt.Println("Got result", mapperResult)
				sendToReducers(mapperResult, reducersInputChan)
			case <- quitM:
				finishedMappers++
				if finishedMappers == numberOfMappers{
					closeReducersInputChannel(reducersInputChan)
					break OuterLoop1
				}

			}
		}

	OuterLoop2:
		for {
			select {
				case reducerResult := <- reducersOutputChan:{
					appendToFinalResult(reducerResult, finalResult)
				}
				case <- quitR:
					finishedReducers++
					if finishedReducers == numberOfReducers{
						close(reducersOutputChan)
						break OuterLoop2
					}
			}
		}
	return finalResult, nil
}

func readFile(fileLocation string) (string, error){
	dat, err := ioutil.ReadFile(fileLocation)
	if err != nil{
		return "", errors.New("cannot read file")
	}
	return string(dat), nil
}

func divideFileContent(fileContent string, ch chan string){
	tmp := strings.Split(fileContent, ".")
	for _, t:= range tmp {
		if len(t) > 0 {
			ch <- t
		}
	}
	close(ch)
}
func sendToReducers(input map[string]int64, reducersCh [] chan map[string]int64){
	for k, v := range input{
		s := asciiSum(k)
		index := s%len(reducersCh)
		reducersCh[index] <- map[string]int64{k:v}
	}
}

func asciiSum(word string) int{
	sum := 0
	for i:=0; i< len(word); i++{
		sum += int(word[i])
	}
	return sum
}

func closeReducersInputChannel(reducersInputChan [] chan map[string]int64){
	for _, inputChan := range reducersInputChan {
		close(inputChan)
	}
}

func appendToFinalResult(output map[string]int64, result  map[string]int64){
	for k, v := range output{
		result[k] = v
	}
}
