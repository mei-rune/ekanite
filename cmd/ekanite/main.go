package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ekanite/ekanite"
)

func main() {
	flag.Parse()
	args := flag.Args()
	for _, name := range args {
		fmt.Println("*", name)
		err := ekanite.Convert(name)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
			return
		}
		fmt.Println("*", name, "is ok")
	}

	fmt.Println("all is ok")
}
