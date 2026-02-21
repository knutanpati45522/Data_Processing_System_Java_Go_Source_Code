# Data Processing System (Java and Go)

This repository contains two implementations of a concurrent Data Processing System:
- Java version using `ReentrantLock`, `Condition`, and `ExecutorService`
- Go version using goroutines, channels, `sync.WaitGroup`, and `defer`

## Project Structure

```
data_processing_system_assignment/
├── java/
│   └── DataProcessingSystem.java
└── go/
    └── main.go
```

## Java Run Instructions

```bash
cd java
javac DataProcessingSystem.java
java DataProcessingSystem
```

## Go Run Instructions

> In some environments, `go run` may hang due to sandbox/toolchain restrictions. If that happens, build and run the binary:

```bash
cd go
go build -o data_processing_go main.go
./data_processing_go
```

## Expected Behavior
- Multiple workers process tasks from a shared queue in parallel
- One invalid task (`bad-data`) triggers error handling/logging
- Successful results are written to output files:
  - `java/java_results.txt`
  - `go/go_results.txt`
