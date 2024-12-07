package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CityData struct {
	total float64
	count int
	min   float64
	max   float64
}

type City struct {
	name string
	temp float64
}

const BATCH_SIZE = 1000

// create a 2d array of strings
var batchPool [1000][]string
var batchPool2 [1000][]City

func producer(fillIndex chan int, freeIndex <-chan int, inputFile string) {

	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	batch := <-freeIndex
	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		batchPool[batch] = append(batchPool[batch], line)
		if len(batchPool[batch]) >= BATCH_SIZE {
			fillIndex <- batch
			batch = <-freeIndex
		}
		lineCount++
		if lineCount >= 100000000 && lineCount%100000000 == 0 {
			fmt.Println("Line count:", lineCount)
		}
	}
	close(fillIndex)
}

func consumer(fillIndex <-chan int, freeIndex chan int, id int, fillIndex2 chan int, freeIndex2 <-chan int) {
	batch2 := <-freeIndex2
	for batch := range fillIndex {
		for _, line := range batchPool[batch] {
			city, tempStr, split := strings.Cut(line, ";")
			if !split {
				fmt.Println("Invalid line format:", line)
				continue
			}
			temp, err := strconv.ParseFloat(tempStr, 64)
			if err != nil {
				panic(err)
			}
			citys := City{name: city, temp: temp}
			batchPool2[batch2] = append(batchPool2[batch2], citys)
			if len(batchPool2[batch2]) >= BATCH_SIZE {
				fillIndex2 <- batch2
				batch2 = <-freeIndex2
			}
		}
		batchPool[batch] = batchPool[batch][:0]
		freeIndex <- batch
		// fmt.Printf("from consumer %v: %s;%.2f\n", id, city, temp)
	}
	if len(batchPool2[batch2]) > 0 {
		fillIndex2 <- batch2
	}
}

func mergeCities(fillIndex2 <-chan int, freeIndex2 chan int, outputs *[]string) {
	processed := 0
	var cities = make(map[string]CityData)
	for batch2 := range fillIndex2 {
		for _, result := range batchPool2[batch2] {
			processed++
			c, ok := cities[result.name]
			if !ok {
				c = CityData{total: 0, count: 0, min: result.temp, max: result.temp}
			}
			c.total += result.temp
			c.count++
			c.min = min(c.min, result.temp)
			c.max = max(c.max, result.temp)
			cities[result.name] = c
		}

		batchPool2[batch2] = batchPool2[batch2][:0]
		freeIndex2 <- batch2
	}

	for city, data := range cities {
		avg := data.total / float64(data.count)
		s := fmt.Sprintf("%s=%.1f/%.1f/%.1f;", city, avg, data.min, data.max)
		*outputs = append(*outputs, s)
	}

	fmt.Println("Processed:", processed)
}

func printResult(outputs []string, outputFile string) {
	sort.Slice(outputs, func(i, j int) bool {
		return outputs[i] < outputs[j]
	})

	file, err := os.Create(outputFile)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer file.Close()

	for _, s := range outputs {
		file.WriteString(s)
	}
}

func main() {

	f, _ := os.Create("cpu.prof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	// start calculating the time
	start := time.Now()
	elapsed := time.Since(start)

	inputFile := "1brc/measurements.txt"
	outputFile := "output.txt"

	var wg sync.WaitGroup
	freeIndex := make(chan int, 1000)
	fillIndex := make(chan int, 1000)

	freeIndex2 := make(chan int, 1000)
	fillIndex2 := make(chan int, 1000)
	for i := 0; i < 1000; i++ {
		freeIndex <- i
		freeIndex2 <- i

		batchPool[i] = make([]string, 0, BATCH_SIZE)
		batchPool2[i] = make([]City, 0, BATCH_SIZE)
	}

	go producer(fillIndex, freeIndex, inputFile)
	// elapsed = time.Since(start)
	// fmt.Printf("Time taken: %vm,%vs\n", int(elapsed.Minutes()), int(elapsed.Seconds())%60)

	const consumerCount = 10

	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			consumer(fillIndex, freeIndex, id, fillIndex2, freeIndex2)
		}(i)
	}

	go func() {
		wg.Wait()
		close(fillIndex2)
	}()

	// make an array of City structs
	// sort the array by city name
	// print the array
	outputs := []string{}
	mergeCities(fillIndex2, freeIndex2, &outputs)

	printResult(outputs, outputFile)

	// end calculating the time
	elapsed = time.Since(start)
	fmt.Printf("Time taken: %vm,%vs\n", int(elapsed.Minutes()), int(elapsed.Seconds())%60)
}
