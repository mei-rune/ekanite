package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ekanite/ekanite"
)

func main() {
	var delta time.Duration
	var format string
	flag.DurationVar(&delta, "delta", 0, "")
	flag.StringVar(&format, "format", "", "")
	flag.Parse()
	args := flag.Args()

	create := ekanite.NewShardWriter
	if format == "csv" {
		create = func(pa string) (ekanite.Writer, error) {
			return ekanite.NewCsvWriter(os.Stdout)
		}
	}
	for _, name := range args {
		fmt.Println("*", name)
		err := ekanite.Convert(name, delta, create)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
			return
		}
		fmt.Println("*", name, "is ok")
	}

	fmt.Println("all is ok")
}
