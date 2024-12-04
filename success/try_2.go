package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
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

func producer(inputFile string) {

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
		parse(line)
		if lineCount >= 100000000 && lineCount%100000010 == 0 {
			fmt.Println("Line count:", lineCount)
		}
	}
	fmt.Println("Line count:", lineCount)

}

var cities = make(map[string]CityData)

func parse(line string) {
	lineSplit := strings.Split(line, ";")
	if len(lineSplit) != 2 {
		fmt.Println("Invalid line format:", line)
		return
	}
	city := lineSplit[0]
	tempStr := lineSplit[1]
	temp, err := strconv.ParseFloat(tempStr, 64)
	if err != nil {
		fmt.Println("Error converting temperature:", err)
		return
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
}

func main() {
	// start calculating the time
	start := time.Now()

	inputFile := "../1brc/measurements.txt"
	outputFile := "output.txt"

	producer(inputFile)

	// write to output file
	file, err := os.Create(outputFile)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer file.Close()

	outputs := []string{}
	for city, data := range cities {
		avg := data.total / float64(data.count)
		s := fmt.Sprintf("%s=%.2f/%.2f/%.2f;", city, avg, data.min, data.max)
		outputs = append(outputs, s)
	}

	sort.Slice(outputs, func(i, j int) bool {
		return outputs[i] < outputs[j]
	})

	for _, s := range outputs {
		file.WriteString(s)
	}
	// end calculating the time
	elapsed := time.Since(start)
	fmt.Printf("Time taken: %vm,%vs\n", int(elapsed.Minutes()), int(elapsed.Seconds())%60)
}
