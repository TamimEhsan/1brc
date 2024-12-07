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

func producer(ch chan []string, inputFile string) {

	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	lineCount := 0
	// Read the file using buffered reader
	// and print the content
	scanner := bufio.NewScanner(file)
	batch := []string{}
	for scanner.Scan() {
		line := scanner.Text()
		batch = append(batch, line)
		if len(batch) >= BATCH_SIZE {
			ch <- batch
			batch = []string{}
		}
		lineCount++
		if lineCount >= 100000000 && lineCount%100000000 == 0 {
			fmt.Println("Line count:", lineCount)
		}
	}
	if len(batch) > 0 {
		ch <- batch
	}
	fmt.Println("Line count:", lineCount)
	close(ch)
}

func consumer(ch <-chan []string, id int, results chan<- []City) {
	cityBatch := []City{}
	for batch := range ch {
		for _, line := range batch {
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
			cityBatch = append(cityBatch, citys)
			if len(cityBatch) >= BATCH_SIZE {
				results <- cityBatch
				cityBatch = []City{}
			}
		}
		// fmt.Printf("from consumer %v: %s;%.2f\n", id, city, temp)
	}
	if len(cityBatch) > 0 {
		results <- cityBatch
	}
}

func mergeCities(results <-chan []City, outputs *[]string) {
	processed := 0
	var cities = make(map[string]CityData)
	for batch := range results {
		for _, result := range batch {
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

	inputFile := "../1brc/measurements.txt"
	outputFile := "output.txt"

	var wg sync.WaitGroup
	ch := make(chan []string, 1000)
	results := make(chan []City, 1000)

	go producer(ch, inputFile)

	const consumerCount = 10

	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			consumer(ch, id, results)
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// make an array of City structs
	// sort the array by city name
	// print the array
	outputs := []string{}
	mergeCities(results, &outputs)

	printResult(outputs, outputFile)

	// end calculating the time
	elapsed := time.Since(start)
	fmt.Printf("Time taken: %vm,%vs\n", int(elapsed.Minutes()), int(elapsed.Seconds())%60)
}
