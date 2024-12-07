package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CityData struct {
	name  string
	total float64
	count int
	min   float64
	max   float64
}

type City struct {
	name string
	temp float64
}

const BATCH_SIZE = 1024 * 10
const POOL_SIZE = 1024

// create a 2d array of strings
var batchPool2 [POOL_SIZE][]City

type Chunk struct {
	Data []byte
}

var CHUNK_SIZE int64 = 1024 * 1024 * 10

func splitter(chunkChan chan Chunk, inputFile string) {

	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Errorf("error getting file info: %v", err)
		os.Exit(0)
	}
	fileSize := fileInfo.Size()

	// Buffer to store incomplete lines
	var buffer []byte

	for offset := int64(0); offset < fileSize; {
		// Calculate chunk size
		remainingSize := fileSize - offset
		if remainingSize < CHUNK_SIZE {
			CHUNK_SIZE = remainingSize
		}

		// Read chunk
		chunk := make([]byte, CHUNK_SIZE)
		bytesRead, err := file.ReadAt(chunk, offset)
		if err != nil && err != io.EOF {
			fmt.Errorf("error getting file info: %v", err)
			os.Exit(0)
		}

		// If this is not the last chunk, find the last newline
		if offset+int64(bytesRead) < fileSize {
			lastNewline := bytes.LastIndexByte(chunk[:bytesRead], '\n')
			if lastNewline == -1 {
				// No newline found, append to buffer
				buffer = append(buffer, chunk[:bytesRead]...)
				offset += int64(bytesRead)
				continue
			}

			// Trim chunk to last complete line
			chunk = chunk[:lastNewline+1]
			bytesRead = lastNewline + 1
		}

		// Combine previous buffer with current chunk
		if len(buffer) > 0 {
			chunk = append(buffer, chunk...)
			buffer = nil
		}

		// If this is the last chunk, save any incomplete line to buffer
		if offset+int64(bytesRead) >= fileSize {
			// Send the final chunk
			chunkChan <- Chunk{Data: chunk}
			break
		}

		// Send chunk
		chunkChan <- Chunk{Data: chunk}

		// Move offset
		offset += int64(bytesRead)
	}
	close(chunkChan)
}

func mapper(chunChan <-chan Chunk, id int, cityChan chan []CityData) {
	var cities = make(map[string]*CityData)
	for chunk := range chunChan {
		reader := bufio.NewReader(bytes.NewReader(chunk.Data))
		// XXX
		for {
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Println("Error reading line:", err)
				os.Exit(1)
			}
			line = line[:len(line)-1]
			city, tempStr, split := strings.Cut(line, ";")
			if !split {
				fmt.Println("Invalid line format:", line)
				continue
			}
			temp, err := strconv.ParseFloat(tempStr, 64)
			if err != nil {
				panic(err)
			}

			c, ok := cities[city]
			if !ok {
				c = &CityData{name: city, total: 0, count: 0, min: temp, max: temp}
				cities[city] = c
			}
			c.total += temp
			c.count++
			c.min = min(c.min, temp)
			c.max = max(c.max, temp)

		}

		// fmt.Printf("from consumer %v: %s;%.2f\n", id, city, temp)
	}

	var outputs []CityData
	for _, data := range cities {
		outputs = append(outputs, *data)
	}
	cityChan <- outputs
}

func reducer(cityChan <-chan []CityData, outputs *[]string) {
	processed := 0
	var cityMap = make(map[string]*CityData)
	for cities := range cityChan {
		for _, result := range cities {
			processed += result.count
			_ = result
			c, ok := cityMap[result.name]
			if !ok {
				c = &CityData{
					total: result.total,
					count: result.count,
					min:   result.min,
					max:   result.max}
				cityMap[result.name] = c
				continue
			}
			c.total += result.total
			c.count += result.count
			c.min = min(c.min, result.min)
			c.max = max(c.max, result.max)
		}

	}

	for city, data := range cityMap {
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
	chunkChan := make(chan Chunk)

	cityChan := make(chan []CityData)

	go splitter(chunkChan, inputFile)
	// elapsed = time.Since(start)
	// fmt.Printf("Time taken: %vm,%vs\n", int(elapsed.Minutes()), int(elapsed.Seconds())%60)

	const workerCount = 10

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mapper(chunkChan, id, cityChan)
		}(i)
	}

	go func() {
		wg.Wait()
		close(cityChan)
	}()

	// make an array of City structs
	// sort the array by city name
	// print the array
	outputs := []string{}
	reducer(cityChan, &outputs)

	printResult(outputs, outputFile)

	// end calculating the time
	elapsed = time.Since(start)
	fmt.Printf("Time taken: %vm,%vs\n", int(elapsed.Minutes()), int(elapsed.Seconds())%60)
}
