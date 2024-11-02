package main

import (
	bitcask "bitcask-go"
	"fmt"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = "/home/cyj/test"
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("key"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("key"))
	if err != nil {
		panic(err)
	}

	fmt.Printf("value: %s\n", val)

	err = db.Delete([]byte("key"))
	if err != nil {
		panic(err)
	}
}
