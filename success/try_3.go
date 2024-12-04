package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
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
	data CityData
}

func producer(ch chan string, inputFile string) {

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
	for scanner.Scan() {
		line := scanner.Text()
		_ = line
		lineCount++
		ch <- line
		if lineCount >= 100000000 && lineCount%100000010 == 0 {
			fmt.Println("Line count:", lineCount)
		}
	}
	fmt.Println("Line count:", lineCount)
	close(ch)
}

func consumer(ch <-chan string, id int, cities map[string]CityData) {
	for line := range ch {
		lineSplit := strings.Split(line, ";")
		if len(lineSplit) != 2 {
			fmt.Println("Invalid line format:", line)
			continue
		}
		city := lineSplit[0]
		tempStr := lineSplit[1]
		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			fmt.Println("Error converting temperature:", err)
			continue
		}
		c, ok := cities[city]
		if !ok {
			c = CityData{total: 0, count: 0, min: temp, max: temp}
		}
		c.total += temp
		c.count++
		if temp < c.min {
			c.min = temp
		}
		if temp > c.max {
			c.max = temp
		}
		cities[city] = c
		// fmt.Printf("from consumer %v: %s;%.2f\n", id, city, temp)
	}
}

func main() {
	runtime.GOMAXPROCS(6)
	// start calculating the time
	start := time.Now()

	inputFile := "../1brc/measurements_moderate.txt"
	outputFile := "output.txt"

	var wg sync.WaitGroup
	ch := make(chan string, 1000000)

	wg.Add(1)
	go func() {
		defer wg.Done()
		producer(ch, inputFile)
	}()

	const consumerCount = 5
	var cities [consumerCount]map[string]CityData

	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		cities[i] = make(map[string]CityData)
		go func(id int) {
			defer wg.Done()
			consumer(ch, id, cities[id])
		}(i)
	}
	wg.Wait()

	// make an array of City structs
	// sort the array by city name
	// print the array

	var citiesTotal = make(map[string]CityData)

	for i := 0; i < consumerCount; i++ {
		for k, v := range cities[i] {
			c, ok := citiesTotal[k]
			if !ok {
				c = CityData{total: 0, count: 0, min: v.min, max: v.max}
			}
			c.total += v.total
			c.count += v.count
			if v.min < c.min {
				c.min = v.min
			}
			if v.max > c.max {
				c.max = v.max
			}
			citiesTotal[k] = c
		}
	}

	outputs := []string{}
	for city, data := range citiesTotal {
		avg := data.total / float64(data.count)
		s := fmt.Sprintf("%s=%.2f/%.2f/%.2f;", city, avg, data.min, data.max)
		outputs = append(outputs, s)
	}

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

	// end calculating the time
	elapsed := time.Since(start)
	fmt.Printf("Time taken: %vm,%vs\n", int(elapsed.Minutes()), int(elapsed.Seconds())%60)
}
