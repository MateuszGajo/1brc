package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"sync"
	"time"
)

// The task is to write a Java program which reads the file, calculates the min, mean, and max temperature value per weather station, and emits the results on stdout like this (i.e. sorted alphabetically by station name, and the result values per station in the format <min>/<mean>/<max>, rounded to one fractional digit):

type Data struct {
	Sum   int
	Count int
	Avg   int
	Min   int
	Max   int
}

type SafeStations struct {
	mu       sync.Mutex
	stations map[string]*Data
}

var safeStations = SafeStations{
	stations: make(map[string]*Data, 9000),
}

func init() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
}

func main() {
	f, err := os.Create("cpu.pprof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	start := time.Now()
	parseData()
	timeElapsed := time.Since(start)
	fmt.Printf("The `for` loop took %s", timeElapsed)
}

var startNumberChar = byte(48)

func parseToInt(data []byte) int {
	i := 0
	multiply := 1

	if data[i] == '-' {
		i++
		multiply = -1
	}

	var temp int
	// We now there is only one decimal place, and can be 1 or 2 digit before .
	if data[i+1] != '.' {
		temp = int(data[i]-startNumberChar)*100 + int(data[i+1]-startNumberChar)*10 + int(data[i+3]-startNumberChar)
	} else {
		temp = int(data[i]-startNumberChar)*10 + int(data[i+2]-startNumberChar)
	}

	return temp * multiply

}

func calculateData(data []byte) map[string]*Data {
	startPointer := 0
	semicolomPointer := 0
	stations := map[string]*Data{}
	for i := 0; i < len(data); i++ {
		if data[i] == ';' {

			semicolomPointer = i
		}

		if data[i] == '\n' {
			station := string(data[startPointer:semicolomPointer])
			temp := 0
			temp = parseToInt(data[semicolomPointer+1 : i])

			startPointer = i + 1

			data := stations[station]

			if data == nil {
				stations[station] = &Data{
					Min:   temp,
					Max:   temp,
					Sum:   temp,
					Count: 1,
				}
			} else {
				data.Sum += temp
				data.Count++
				if data.Min > temp {
					data.Min = temp
				}

				if data.Max < temp {
					data.Max = temp
				}

			}

		}
	}
	return stations

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
				safeStation := safeStations.stations[station]

				if safeStation == nil {
					safeStations.stations[station] = stats
				} else {
					safeStation.Count += stats.Count
					safeStation.Sum += stats.Sum
					safeStation.Avg = safeStations.stations[station].Sum / safeStations.stations[station].Count
					if safeStation.Min > stats.Min {
						safeStation.Min = stats.Min
					}

					if safeStation.Max < stats.Max {
						safeStation.Max = stats.Max
					}

				}
				safeStations.mu.Unlock()
			}

			<-channels
		}(newCopy)

	}
	wg.Wait()
	for station, stats := range safeStations.stations {
		fmt.Printf("Station: %s, Avg: %.2f, Sum: %.2f, Min: %.2f, Max: %.2f\n",
			station, stats.Avg/10, stats.Sum/10, stats.Min/10, stats.Max/10)
	}

	// file, err = os.Create("data1.json")
	// if err != nil {
	// 	log.Fatalf("Failed to create file: %v", err)
	// }
	// defer file.Close()
	// encoder := json.NewEncoder(file)
	// encoder.Encode(safeStations.stations)

}
