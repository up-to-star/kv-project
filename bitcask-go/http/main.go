package main

import (
	bitcask "bitcask-go"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
)

var db *bitcask.DB

func init() {
	var err error
	options := bitcask.DefaultOptions
	options.DirPath, _ = os.MkdirTemp("", "bitcask-go-http")
	db, err = bitcask.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open bitcask db, %v", err))
	}
}

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put key in db %s: %v", key, err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := request.URL.Query().Get("key")
	value, err := db.Get([]byte(key))
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get key in db %s: %v", key, err)
		return
	}
	writer.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := request.URL.Query().Get("key")
	if err := db.Delete([]byte(key)); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to delete key in db %s: %v", key, err)
		return
	}
	writer.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	keys := db.ListKeys()
	writer.Header().Set("content-type", "application/json")
	var result []string
	for _, key := range keys {
		result = append(result, string(key))
	}
	_ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	stat := db.Stat()
	writer.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)
}

func main() {
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listkeys", handleListKeys)
	http.HandleFunc("/bitcask/stat", handleStat)
	_ = http.ListenAndServe(":8080", nil)
}
