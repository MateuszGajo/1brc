package main

import (
	"fmt"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"time"
)

// The task is to write a Java program which reads the file, calculates the min, mean, and max temperature value per weather station, and emits the results on stdout like this (i.e. sorted alphabetically by station name, and the result values per station in the format <min>/<mean>/<max>, rounded to one fractional digit):
var i = 0

type Data struct {
	Sum   float64
	Count int
	Avg   float64
	Min   float64
	Max   float64
}

var stations = map[string]*Data{}

func init() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
}

func NewData() *Data {
	return &Data{
		Min: math.MaxFloat64,
		Max: -math.MaxFloat64,
	}
}

func main() {

	start := time.Now()
	parseData()
	timeElapsed := time.Since(start)
	fmt.Printf("The `for` loop took %s", timeElapsed)
}

func parseData() {
	fmt.Println("hello world!")
	file, err := os.OpenFile("./data/measurements_small.txt", os.O_RDWR, 0644)

	defer file.Close()

	if err != nil {
		panic(err)
	}

	buffer := make([]byte, 100_000)
	rest := []byte{}
	station := ""
	for {
		n, err := file.Read(buffer)
		if err != nil && n == 0 {
			break
		}
		startPointer := 0

		data := rest
		data = append(data, buffer[:n]...)
		for i = 0; i < len(data); i++ {
			if data[i] == ';' {
				station = string(data[startPointer:i])
				startPointer = i + 1
			}

			if data[i] == '\n' {
				val := 0.0
				val, err = strconv.ParseFloat(string(data[startPointer:i]), 64)
				if err != nil {
					panic(err)
				}
				startPointer = i + 1

				data, isOk := stations[station]

				if !isOk {
					data = NewData()
				}

				data.Sum += val
				data.Count++
				if data.Min > val {
					data.Min = val
				}

				if data.Max < val {
					data.Max = val
				}

				data.Avg = data.Sum / float64(data.Count)

				stations[station] = data
			}
		}
		rest = data[startPointer:]

	}
	for station, stats := range stations {
		fmt.Printf("Station: %s, Avg: %.2f, Sum: %.2f, Min: %.2f, Max: %.2f\n",
			station, stats.Avg, stats.Sum, stats.Min, stats.Max)
	}

}
