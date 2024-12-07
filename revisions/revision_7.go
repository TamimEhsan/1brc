package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
)

type CityData struct {
	name  []byte
	total int
	count int32
	min   int16
	max   int16
}

type City struct {
	name string
	temp int16
}

// create a 2d array of strings

type Chunk struct {
	Data []byte
}

var CHUNK_SIZE int64 = 1024 * 1024 * 10

// this takes 10s standalone
func splitter(chunkChan chan Chunk, inputFile string) {

	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Errorf("error opening file: %v", err)
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

/*
// this will be inlined by the compiler

	func boolToInt(b bool) int {
		return int((*(*uint8)(unsafe.Pointer(&b))) & 1)
	}

// this will be inlined by the compiler

	func parse(tempStr string) int {
		temp := 0
		for i := 0; i < len(tempStr); i++ {
			c := tempStr[i]
			cond := boolToInt(c >= '0' && c <= '9')
			temp = (temp*10+int(c-'0'))*cond + temp*(1-cond)
		}
		temp = temp * (1 - 2*boolToInt(tempStr[0] == '-'))
		return temp
	}
*/
func parse(tempStr string) int {
	temp := 0
	for i := 0; i < len(tempStr); i++ {
		c := tempStr[i]
		if c >= '0' && c <= '9' {
			temp = temp*10 + int(c-'0')
		}
	}
	if tempStr[0] == '-' {
		temp = -temp
	}
	return temp
}
func mapper(chunChan <-chan Chunk, id int, cityChan chan []CityData) {
	var cities = make(map[uint64]*CityData)
	for chunk := range chunChan {

		// reader := bufio.NewReader(bytes.NewReader(chunk.Data))
		LEN := len(chunk.Data)
		// XXX
		for xx := 0; xx < LEN; xx++ {
			// record reader

			i := 0
			ends := 0
			var hash uint64 = 0
			var base uint64 = 257
			for ; i+xx < LEN; i++ {
				if chunk.Data[i+xx] == ';' {
					break
				}
				hash = (hash*base + uint64(chunk.Data[i+xx]))
			}
			i++
			ends = xx + i - 1
			// city := chunk.Data[xx : xx+i-1]
			var temp int16 = 0
			neg := false
			if chunk.Data[i+xx] == '-' {
				neg = true
				i++
			}
			for ; i < LEN; i++ {
				c := chunk.Data[i+xx]
				if c >= '0' && c <= '9' {
					temp = temp*10 + int16(c-'0')
				}
				if c == '\n' {
					break
				}
			}

			if neg {
				temp = -temp
			}

			// mapper and combiner
			c, ok := cities[hash]
			if !ok {
				city := chunk.Data[xx:ends]
				c = &CityData{name: city, total: 0, count: 0, min: temp, max: temp}
				cities[hash] = c
			}
			c.total += int(temp)
			c.count += 1
			c.min = min(c.min, temp)
			c.max = max(c.max, temp)

			xx += i
		}

	}

	var outputs []CityData
	for _, data := range cities {
		outputs = append(outputs, *data)
	}
	cityChan <- outputs
}

// this takes typically 1s
func reducer(cityChan <-chan []CityData, outputs *[]string) {
	// processed := 0
	var cityMap = make(map[string]*CityData)
	for cities := range cityChan {
		for _, result := range cities {
			// processed += result.count
			_ = result

			c, ok := cityMap[string(result.name)]
			if !ok {
				c = &CityData{
					total: result.total,
					count: result.count,
					min:   result.min,
					max:   result.max}
				cityMap[string(result.name)] = c
				continue
			}
			c.total += result.total
			c.count += result.count
			c.min = min(c.min, result.min)
			c.max = max(c.max, result.max)
		}

	}

	for city, data := range cityMap {
		avg := float32(data.total) / float32(data.count)
		s := fmt.Sprintf("%s=%.1f/%.1f/%.1f;", city, avg/10.0, float32(data.min)/10.0, float32(data.max)/10.0)
		*outputs = append(*outputs, s)
	}

	// fmt.Println("Processed:", processed)
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
	// set worker count to the number of cpu cores
	workerCount := runtime.NumCPU() - 1
	fmt.Println("Number of workers:", workerCount)

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
