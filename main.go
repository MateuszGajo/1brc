package main

import (
	"fmt"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"time"
)

// The task is to write a Java program which reads the file, calculates the min, mean, and max temperature value per weather station, and emits the results on stdout like this (i.e. sorted alphabetically by station name, and the result values per station in the format <min>/<mean>/<max>, rounded to one fractional digit):

type Data struct {
	Sum   float64
	Count int
	Avg   float64
	Min   float64
	Max   float64
}

type SafeStations struct {
	mu       sync.Mutex
	stations map[string]*Data
}

var safeStations = SafeStations{
	stations: map[string]*Data{},
}

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

func calculateData(data []byte) map[string]*Data {
	startPointer := 0
	semicolomPointer := 0
	safeStations2 := map[string]*Data{}
	for i := 0; i < len(data); i++ {
		if data[i] == ';' {

			semicolomPointer = i
		}

		if data[i] == '\n' {
			station := string(data[startPointer:semicolomPointer])
			val := 0.0
			val, err := strconv.ParseFloat(string(data[semicolomPointer+1:i]), 64)
			if err != nil {
				fmt.Println(semicolomPointer, i)
				panic(err)
			}
			startPointer = i + 1

			data, isOk := safeStations2[station]

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

			safeStations2[station] = data
		}
	}
	return safeStations2
}

func parseData() {
	fmt.Println("hello world!")
	file, err := os.OpenFile("./data/measurements.txt", os.O_RDWR, 0644)

	defer file.Close()

	if err != nil {
		panic(err)
	}

	rest := []byte{}
	var wg sync.WaitGroup
	channels := make(chan struct{}, 40)
	for {
		buffer := make([]byte, 400_000_00)
		n, err := file.Read(buffer)
		if err != nil && n == 0 {
			break
		}

		data := rest
		data = append(data, buffer[:n]...)
		newLine := -1
		for i := len(data) - 1; i > 0; i-- {
			if data[i] == '\n' {
				newLine = i
				break
			}
		}
		newCopy := make([]byte, newLine+1)
		copy(newCopy, data[:newLine+1])
		rest = data[newLine+1:]

		channels <- struct{}{}
		wg.Add(1)
		go func(d []byte) {
			defer wg.Done()
			res := calculateData(d)
			for station, stats := range res {
				safeStations.mu.Lock()
				safeStation, isOk := safeStations.stations[station]

				if !isOk {
					safeStation = stats
				} else {
					safeStation.Count += stats.Count
					safeStation.Sum += stats.Sum
					safeStation.Avg = safeStations.stations[station].Sum / float64(safeStations.stations[station].Count)
					if safeStation.Min > stats.Min {
						safeStation.Min = stats.Min
					}

					if safeStation.Max < stats.Max {
						safeStation.Max = stats.Max
					}

				}
				safeStations.stations[station] = safeStation
				safeStations.mu.Unlock()
			}

			<-channels
		}(newCopy)

	}
	wg.Wait()
	for station, stats := range safeStations.stations {
		fmt.Printf("Station: %s, Avg: %.2f, Sum: %.2f, Min: %.2f, Max: %.2f\n",
			station, stats.Avg, stats.Sum, stats.Min, stats.Max)
	}

	// file, err = os.Create("data1.json")
	// if err != nil {
	// 	log.Fatalf("Failed to create file: %v", err)
	// }
	// defer file.Close()
	// encoder := json.NewEncoder(file)
	// encoder.Encode(safeStations.stations)

}
