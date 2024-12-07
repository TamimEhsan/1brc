// r1: simple, idiomatic Go using bufio.Scanner and strconv.ParseFloat
//
// ~1.004s for 10M rows

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func r1(inputPath string, output io.Writer) error {
	type stats struct {
		min, max, sum float64
		count         int64
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	stationStats := make(map[string]stats)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		station, tempStr, hasSemi := strings.Cut(line, ";")
		if !hasSemi {
			continue
		}

		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			return err
		}

		s, ok := stationStats[station]
		if !ok {
			s.min = temp
			s.max = temp
			s.sum = temp
			s.count = 1
		} else {
			s.min = min(s.min, temp)
			s.max = max(s.max, temp)
			s.sum += temp
			s.count++
		}
		stationStats[station] = s
	}

	stations := make([]string, 0, len(stationStats))
	for station := range stationStats {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	fmt.Fprint(output, "{")
	for i, station := range stations {
		if i > 0 {
			fmt.Fprint(output, ", ")
		}
		s := stationStats[station]
		mean := s.sum / float64(s.count)
		fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, s.min, mean, s.max)
	}
	fmt.Fprint(output, "}\n")
	return nil
}

func main() {

	start := time.Now()
	inputPath := "../1brc/measurements.txt"

	outputPath := "output.txt"
	file, err := os.Create(outputPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	r1(inputPath, file)

	elapsed := time.Since(start)
	fmt.Printf("Time taken: %vm,%vs\n", int(elapsed.Minutes()), int(elapsed.Seconds())%60)

}
