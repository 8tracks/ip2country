package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/8tracks/ip2country/vendor/geoip"
	"runtime"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

const BATCHSIZE = 1000
var numGoRoutines int
var geoipDbPath string
var outputDir string
var wg = sync.WaitGroup{}

func init() {
	flag.StringVar(&geoipDbPath, "d", "", "GeoIP data file")
	flag.StringVar(&outputDir, "o", "", "Output directory")
	flag.IntVar(&numGoRoutines, "r", 3, "Number of goroutines to execute queries.")
	flag.IntVar(&column, "c", 3, "IP Address column.")
}

func convIp2Country(outFilePath string, pipe chan []string) {

	log.Printf("Starting up converter ... %s", geoipDbPath)

	gdb, err := geoip.Open(geoipDbPath)
	if err != nil {
		fmt.Println("Could not open GeoIP database", err)
		os.Exit(1)
	}

	out, err := os.Create(outFilePath)
	if err != nil {
		log.Panicf("Could not open %s: %s", outFilePath, err)
	}
	defer out.Close()

	for lines := range pipe {
		for _, line := range lines {
			parts := strings.Split(line, "|")
			country, _ := gdb.GetCountry(parts[4])
			if country != "" {
				parts[4] = country
			} else {
				parts[4] = "--"
			}
			out.WriteString(strings.Join(parts, "|"))
		}
	}

	wg.Done()
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	input := bufio.NewReader(os.Stdin)

	pipe := make(chan []string)

	for i := 0; i < numGoRoutines; i++ {
		go convIp2Country(fmt.Sprintf("/Users/paydro/code/go/src/github.com/8tracks/ip2country/output/out%d", i), pipe)
	}

	for {
		bucket, err := readLines(input)
		if err == io.EOF {
			pipe <- bucket
			break // We're done with stdin
		} else if err != nil {
			log.Panicf("readLines error: %s", err)
		}

		pipe <- bucket
	}


	close(pipe)
	wg.Wait()


}

func readLines(input *bufio.Reader) ([]string, error) {
	bucket := make([]string, 0, BATCHSIZE)

	for len(bucket) <= BATCHSIZE {
		line, err := input.ReadString('\n')

		// This will send back io.EOF as well as any other error that occurs
		// Caller should handle the io.EOF case (a valid case where the bucket
		// has data).
		if err != nil {
			return bucket, err
		}

		bucket = append(bucket, line)
	}

	return bucket, nil
}