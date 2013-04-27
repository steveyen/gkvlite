package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/steveyen/gkvlite"
)

var keyFormat = flag.String("key-format", "string",
	"format item key as string, bytes, raw, or none")
var valFormat = flag.String("val-format", "string",
	"format item val as string, bytes, raw, or none")
var indent = flag.Bool("indent", false,
	"show tree depth using indentation")

func usage() {
	fmt.Fprintf(os.Stderr, "gkvlite file view/inspect tool\n")
	fmt.Fprintf(os.Stderr, "  suports gkvlite file versions: %d\n", gkvlite.VERSION)
	fmt.Fprintf(os.Stderr, "\nusage: %s [flags] gkvlite-file [cmd [cmd-specific-args ...]]\n",
		os.Args[0])
	fmt.Fprintf(os.Stderr, "\nexamples:\n")
	fmt.Fprintf(os.Stderr, "  ./view /tmp/my-data.gkvlite\n")
	fmt.Fprintf(os.Stderr, "  ./view /tmp/my-data.gkvlite names\n")
	fmt.Fprintf(os.Stderr, "  ./view /tmp/my-data.gkvlite items warehouse-inventory\n")
	fmt.Fprintf(os.Stderr, "\ncmd:\n")
	fmt.Fprintf(os.Stderr, "  names - lists collection names (default cmd)\n")
	fmt.Fprintf(os.Stderr, "  items <collection-name> - emits items in a collection\n")
	fmt.Fprintf(os.Stderr, "\nflags:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "  -h: print this usage/help message\n")
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if err := mainDo(flag.Args()); err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
}

func mainDo(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("missing gkvlite file arg")
	}
	fname, args := args[0], args[1:]

	f, err := os.Open(fname)
	if err != nil || f == nil {
		return fmt.Errorf("could not open file: %v", fname)
	}
	defer f.Close()

	s, err := gkvlite.NewStore(f)
	if err != nil || s == nil {
		return fmt.Errorf("could not create store from file: %v", fname)
	}

	cmd := "names"
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}
	switch cmd {
	case "names":
		collNames := s.GetCollectionNames()
		for _, collName := range collNames {
			fmt.Printf("%s\n", collName)
		}
	case "items":
		if len(args) < 1 {
			return fmt.Errorf("missing 'items <collection-name>' param")
		}
		collName := args[0]
		coll := s.GetCollection(collName)
		if coll == nil {
			return fmt.Errorf("could not find collection: %v", collName)
		}
		return coll.VisitItemsAscendEx(nil, true, emitItem)
	default:
		return fmt.Errorf("unknown command: %v", cmd)
	}

	return nil
}

func emitItem(i *gkvlite.Item, depth uint64) bool {
	if *indent {
		for i := 0; i < int(depth); i++ {
			fmt.Printf(" ")
		}
	}
	emit(i.Key, *keyFormat)
	if *keyFormat != "none" && *valFormat != "none" {
		fmt.Printf("=")
	}
	emit(i.Val, *valFormat)
	fmt.Printf("\n")
	return true
}

func emit(v []byte, format string) {
	switch format {
	default:
	case "string":
		fmt.Printf("%s", string(v))
	case "bytes":
		fmt.Printf("%#v", v)
	case "raw":
		fmt.Printf("%v", v)
	case "none":
	}
}
