package mapper

import (
	"fmt"
	"learning.project/mapReduce/userFunctions"
)

func HandleMapping(inputCh chan string, mapFunc userFunctions.MapperReducer, ch chan map[string]int64, quit chan bool) {
	for input := range inputCh{
		fmt.Println("Got input:" + input)
		ch <- mapFunc.MapFunc(input)
	}
	quit <- true
}
