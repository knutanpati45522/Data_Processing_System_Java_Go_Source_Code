package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type Task struct {
	ID      int
	Payload string
}

type Result struct {
	TaskID    int
	WorkerID  int
	Output    string
	Timestamp time.Time
	Err       error
}

func processTask(workerID int, task Task) (string, error) {
	// Simulate computational delay
	time.Sleep(time.Duration(120+(task.ID%3)*90) * time.Millisecond)

	if strings.EqualFold(task.Payload, "bad-data") {
		return "", errors.New("invalid payload encountered: " + task.Payload)
	}

	return strings.ToUpper(task.Payload) + "_PROCESSED", nil
}

func worker(workerID int, tasks <-chan Task, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Go Worker-%d started", workerID)

	for task := range tasks {
		output, err := processTask(workerID, task)
		if err != nil {
			log.Printf("Go Worker-%d error on task %d: %v", workerID, task.ID, err)
			results <- Result{TaskID: task.ID, WorkerID: workerID, Err: err, Timestamp: time.Now()}
			continue
		}

		results <- Result{
			TaskID:    task.ID,
			WorkerID:  workerID,
			Output:    output,
			Timestamp: time.Now(),
		}
		log.Printf("Go Worker-%d completed task %d", workerID, task.ID)
	}

	log.Printf("Go Worker-%d exiting", workerID)
}

func resultWriter(results <-chan Result, outputFile string, done chan<- struct{}, collected *[]string, mu *sync.Mutex) {
	defer close(done)

	file, err := os.Create(outputFile)
	if err != nil {
		log.Printf("Failed to create output file %s: %v", outputFile, err)
		return
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			log.Printf("Error closing file: %v", cerr)
		}
	}()

	writer := bufio.NewWriter(file)
	defer func() {
		if ferr := writer.Flush(); ferr != nil {
			log.Printf("Error flushing writer: %v", ferr)
		}
	}()

	for res := range results {
		if res.Err != nil {
			log.Printf("Skipping failed task %d from Worker-%d", res.TaskID, res.WorkerID)
			continue
		}

		line := fmt.Sprintf("%s | Worker-%d | task=%d | result=%s",
			res.Timestamp.Format("15:04:05"), res.WorkerID, res.TaskID, res.Output)

		if _, err := writer.WriteString(line + "\n"); err != nil {
			log.Printf("Write error for task %d: %v", res.TaskID, err)
			continue
		}

		mu.Lock()
		*collected = append(*collected, line)
		mu.Unlock()
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	tasks := make(chan Task, 8)   // channel acts as concurrency-safe queue
	results := make(chan Result, 8)
	doneWriting := make(chan struct{})

	var wg sync.WaitGroup
	var mu sync.Mutex
	collectedResults := make([]string, 0, 8)

	outputFile := "go_results.txt"

	// Start result writer goroutine (shared output resource manager)
	go resultWriter(results, outputFile, doneWriting, &collectedResults, &mu)

	workerCount := 3
	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go worker(i, tasks, results, &wg)
	}

	// Producer: add tasks
	payloads := []string{"alpha", "beta", "gamma", "delta", "bad-data", "epsilon", "zeta", "eta"}
	for i, p := range payloads {
		tasks <- Task{ID: i + 1, Payload: p}
	}
	close(tasks) // safe termination signal for workers

	wg.Wait()
	close(results)
	<-doneWriting // wait for writer to finish

	fmt.Println("=== Go Data Processing Summary ===")
	fmt.Println("Output file:", outputFile)

	mu.Lock()
	fmt.Println("Successful results:", len(collectedResults))
	for _, line := range collectedResults {
		fmt.Println(line)
	}
	mu.Unlock()

	fmt.Println("==================================")
}
