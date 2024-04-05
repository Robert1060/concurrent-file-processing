package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
)

type pair struct {
	hash string
	path string
}

type fileList []string

type results map[string]fileList

func hashFile(path string) pair {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	hash := md5.New()

	if _, err := io.Copy(hash, file); err != nil {
		log.Fatal(err)
	}
	return pair{fmt.Sprintf("%x", hash.Sum(nil)), path}
}

func walkTree(dir string, paths chan<- string) error {
	visit := func(p string, fi os.FileInfo, err error) error {
		if err != nil && err != os.ErrNotExist {
			log.Fatal(err)
		}
		if fi.Mode().IsRegular() && fi.Size() > 0 {
			paths <- p
		}
		return nil
	}

	return filepath.Walk(dir, visit)
}

func processFiles(paths <-chan string, pairs chan<- pair, done chan bool) {
	for p := range paths {
		pairs <- hashFile(p)
	}
	done <- true
}

func collectHashes(pairs <-chan pair, res chan<- results) {
	result := make(results)
	for p := range pairs {
		result[p.hash] = append(result[p.hash], p.path)
	}
	res <- result
}

func run(dir string) results {
	workers := 2 * runtime.GOMAXPROCS(0)
	result := make(chan results)
	paths := make(chan string)
	pairs := make(chan pair)
	done := make(chan bool)

	for i := 0; i < workers; i++ {
		go processFiles(paths, pairs, done)
	}

	// run collecting hashes on different goroutine to dont block main thread
	go collectHashes(pairs, result)

	err := walkTree(dir, paths)
	if err != nil {
		return nil
	}

	// close paths to signal workers to stop working
	close(paths)

	for i := 0; i < workers; i++ {
		<-done
	}
	// close pairs after all workers are done
	close(pairs)

	return <-result
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Provide a dir name!!")
	}

	if results := run(os.Args[1]); results != nil {
		for hash, files := range results {
			if len(files) > 1 {
				fmt.Println(hash[len(hash)-7:], len(files))

				for _, f := range files {
					fmt.Println(" ", f)
				}
			}
		}
	}

}
