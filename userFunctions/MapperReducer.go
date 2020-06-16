package userFunctions

import "strings"

type MapperReducer interface{
	MapFunc(input string) map[string]int64
	ReduceFunc(input map[string][]int64) map[string]int64
}

type WordCount struct{}

func (wordCount WordCount) MapFunc(input string) map[string] int64{
	input = strings.ToLower(input)
	input = strings.TrimSpace(input)
	replacer := strings.NewReplacer(",", "", ":", "")
	input = replacer.Replace(input)
	sentences := strings.Split(input, " ")
	result := map[string] int64{}
	for _, w := range sentences{
		if len(w) == 0{
			continue
		}
		_, ok:= result[w]
		if ok{
			result[w] += 1
		}else{
			result[w] = 1
		}
	}
	return result
}

func (wordCount WordCount) ReduceFunc(input map[string] []int64) map[string]int64{
	result := make(map[string]int64)
	for k, v := range input{
		result[k] = 0
		for _, i := range v{
			result[k] += i
		}
	}
	return result
}
