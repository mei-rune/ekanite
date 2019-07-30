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
	flag.DurationVar(&delta, "delta", 0, "")
	flag.Parse()
	args := flag.Args()
	for _, name := range args {
		fmt.Println("*", name)
		err := ekanite.Convert(name, delta)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
			return
		}
		fmt.Println("*", name, "is ok")
	}

	fmt.Println("all is ok")
}
