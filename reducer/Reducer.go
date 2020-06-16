package reducer

import (
	"learning.project/mapReduce/userFunctions"
)

func HandleReducing(inputCh chan map[string]int64, reduceFun userFunctions.MapperReducer, ch chan map[string]int64, quit chan bool) {
	data := make(map[string][]int64)
	for input := range inputCh{
		for k, v := range input {
			_, ok := data[k]
			if ok {
				data[k] = append(data[k], v)
			}else{
				data[k] = []int64{v}
			}
		}
	}
	ch <- reduceFun.ReduceFunc(data)
	quit <- true
}
