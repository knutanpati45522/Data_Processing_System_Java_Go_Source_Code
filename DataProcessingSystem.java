import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataProcessingSystem {
    private static final Logger LOGGER = Logger.getLogger(DataProcessingSystem.class.getName());

    static class Task {
        final int id;
        final String payload;

        Task(int id, String payload) {
            this.id = id;
            this.payload = payload;
        }
    }

    static class SharedTaskQueue {
        private final Queue<Task> queue = new ArrayDeque<>();
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition notEmpty = lock.newCondition();
        private boolean closed = false;

        public void addTask(Task task) {
            lock.lock();
            try {
                if (closed) {
                    throw new IllegalStateException("Cannot add task. Queue is closed.");
                }
                queue.offer(task);
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }

        public Task getTask() throws InterruptedException {
            lock.lock();
            try {
                while (queue.isEmpty() && !closed) {
                    notEmpty.await();
                }
                if (queue.isEmpty() && closed) {
                    return null; // termination signal
                }
                return queue.poll();
            } finally {
                lock.unlock();
            }
        }

        public void close() {
            lock.lock();
            try {
                closed = true;
                notEmpty.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    static class ResultsWriter implements AutoCloseable {
        private final List<String> results = new ArrayList<>();
        private final BufferedWriter writer;

        ResultsWriter(String outputPath) throws IOException {
            this.writer = new BufferedWriter(new FileWriter(outputPath));
        }

        public synchronized void writeResult(String line) throws IOException {
            results.add(line);
            writer.write(line);
            writer.newLine();
            writer.flush();
        }

        public synchronized List<String> snapshot() {
            return new ArrayList<>(results);
        }

        @Override
        public synchronized void close() throws IOException {
            writer.close();
        }
    }

    static class Worker implements Runnable {
        private final int workerId;
        private final SharedTaskQueue queue;
        private final ResultsWriter resultsWriter;

        Worker(int workerId, SharedTaskQueue queue, ResultsWriter resultsWriter) {
            this.workerId = workerId;
            this.queue = queue;
            this.resultsWriter = resultsWriter;
        }

        @Override
        public void run() {
            String name = "Worker-" + workerId;
            LOGGER.info(name + " started.");
            try {
                while (true) {
                    Task task = queue.getTask();
                    if (task == null) {
                        LOGGER.info(name + " detected queue completion and is terminating.");
                        break;
                    }

                    try {
                        String result = processTask(name, task);
                        resultsWriter.writeResult(result);
                        LOGGER.info(name + " completed task " + task.id);
                    } catch (IOException ioEx) {
                        LOGGER.log(Level.SEVERE, name + " failed writing result for task " + task.id, ioEx);
                    } catch (RuntimeException ex) {
                        LOGGER.log(Level.WARNING, name + " encountered processing error on task " + task.id, ex);
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING, name + " interrupted while waiting for tasks.", ie);
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, name + " unexpected error.", ex);
            } finally {
                LOGGER.info(name + " exited.");
            }
        }

        private String processTask(String workerName, Task task) {
            try {
                Thread.sleep(150 + (task.id % 3) * 80L); // simulate computation
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted during processing", e);
            }

            // Simulate a bad record to demonstrate exception handling.
            if ("bad-data".equalsIgnoreCase(task.payload)) {
                throw new IllegalArgumentException("Invalid payload encountered: " + task.payload);
            }

            String transformed = task.payload.toUpperCase() + "_PROCESSED";
            return String.format("%s | %s | task=%d | result=%s",
                    LocalTime.now().withNano(0), workerName, task.id, transformed);
        }
    }

    public static void main(String[] args) {
        String outputFile = "java_results.txt";
        SharedTaskQueue queue = new SharedTaskQueue();

        try (ResultsWriter resultsWriter = new ResultsWriter(outputFile)) {
            int workerCount = 3;
            ExecutorService executor = Executors.newFixedThreadPool(workerCount);

            for (int i = 1; i <= workerCount; i++) {
                executor.submit(new Worker(i, queue, resultsWriter));
            }

            // Producer section: add tasks
            String[] payloads = {
                "alpha", "beta", "gamma", "delta", "bad-data", "epsilon", "zeta", "eta"
            };
            for (int i = 0; i < payloads.length; i++) {
                queue.addTask(new Task(i + 1, payloads[i]));
            }

            queue.close(); // signal no more tasks

            executor.shutdown();
            boolean finished = executor.awaitTermination(30, TimeUnit.SECONDS);
            if (!finished) {
                LOGGER.warning("Executor did not terminate in time. Forcing shutdown.");
                executor.shutdownNow();
            }

            List<String> finalResults = resultsWriter.snapshot();
            System.out.println("=== Java Data Processing Summary ===");
            System.out.println("Output file: " + outputFile);
            System.out.println("Successful results: " + finalResults.size());
            for (String line : finalResults) {
                System.out.println(line);
            }
            System.out.println("====================================");
        } catch (IOException ioEx) {
            LOGGER.log(Level.SEVERE, "I/O failure in main.", ioEx);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.SEVERE, "Main thread interrupted.", ie);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Unexpected application error.", ex);
        }
    }
}
