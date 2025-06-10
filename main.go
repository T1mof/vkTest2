package main

import (
	"fmt"
	"sync"
	"time"
)

// Worker структура
type Worker struct {
	id       int
	jobQueue chan string
	quit     chan struct{}
}

// Создание нового воркера
func NewWorker(id int) *Worker {
	return &Worker{
		id:       id,
		jobQueue: make(chan string, 5),
		quit:     make(chan struct{}),
	}
}

// Запуск воркера
func (w *Worker) Start(wg *sync.WaitGroup, results chan<- string) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case job := <-w.jobQueue:
				time.Sleep(200 * time.Millisecond)
				fmt.Printf("Worker %d processing job: %s\n", w.id, job)
				results <- fmt.Sprintf("Worker %d processed: %s", w.id, job)
			case <-w.quit:
				fmt.Printf("Worker %d stopping\n", w.id)
				return
			}
		}
	}()
}

// Остановка воркера
func (w *Worker) Stop() {
	close(w.quit)
}

// WorkerPool структура с round-robin распределением
type WorkerPool struct {
	workers           map[int]*Worker
	workerIDs         []int
	lastWorkerIndex   int
	jobChannel        chan string
	results           chan string
	wg                sync.WaitGroup
	addWorker         chan bool
	removeWorker      chan bool
	workerID          int
	mu                sync.RWMutex
	running           bool
	stopChan          chan struct{}
	distributionMutex sync.Mutex
}

// Создание нового пула
func NewWorkerPool() *WorkerPool {
	return &WorkerPool{
		workers:         make(map[int]*Worker),
		workerIDs:       make([]int, 0),
		lastWorkerIndex: -1,
		jobChannel:      make(chan string, 100),
		results:         make(chan string, 100),
		addWorker:       make(chan bool, 10),
		removeWorker:    make(chan bool, 10),
		stopChan:        make(chan struct{}),
		running:         false,
	}
}

// Запуск пула
func (wp *WorkerPool) Start() {
	wp.mu.Lock()
	if wp.running {
		wp.mu.Unlock()
		return
	}
	wp.running = true
	wp.mu.Unlock()

	go func() {
		for {
			select {
			case job := <-wp.jobChannel:
				wp.distributeJobRoundRobin(job)
			case <-wp.addWorker:
				wp.addNewWorker()
			case <-wp.removeWorker:
				wp.removeExistingWorker()
			case <-wp.stopChan:
				return
			}
		}
	}()
}

// Round-robin распределение заданий
func (wp *WorkerPool) distributeJobRoundRobin(job string) {
	wp.distributionMutex.Lock()
	defer wp.distributionMutex.Unlock()

	wp.mu.RLock()
	workerCount := len(wp.workerIDs)
	wp.mu.RUnlock()

	if workerCount == 0 {
		go func() {
			time.Sleep(100 * time.Millisecond)
			select {
			case wp.jobChannel <- job:
			default:
				fmt.Printf("Failed to requeue job: %s (no workers available)\n", job)
			}
		}()
		return
	}

	for attempts := 0; attempts < workerCount; attempts++ {
		wp.mu.RLock()
		if len(wp.workerIDs) == 0 {
			wp.mu.RUnlock()
			return
		}

		wp.lastWorkerIndex = (wp.lastWorkerIndex + 1) % len(wp.workerIDs)
		workerID := wp.workerIDs[wp.lastWorkerIndex]
		worker, exists := wp.workers[workerID]
		wp.mu.RUnlock()

		if !exists {
			continue
		}

		select {
		case worker.jobQueue <- job:
			fmt.Printf("Job '%s' assigned to Worker %d (round-robin)\n", job, workerID)
			return
		default:
			continue
		}
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		select {
		case wp.jobChannel <- job:
			fmt.Printf("Job '%s' requeued (all workers busy)\n", job)
		default:
			fmt.Printf("Failed to requeue job: %s (queue full)\n", job)
		}
	}()
}

// Добавление нового воркера
func (wp *WorkerPool) addNewWorker() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	wp.workerID++
	worker := NewWorker(wp.workerID)
	worker.Start(&wp.wg, wp.results)
	wp.workers[wp.workerID] = worker
	wp.workerIDs = append(wp.workerIDs, wp.workerID)

	fmt.Printf("Added worker %d (total: %d)\n", wp.workerID, len(wp.workers))
}

// Удаление существующего воркера
func (wp *WorkerPool) removeExistingWorker() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if len(wp.workers) == 0 {
		fmt.Println("No workers to remove")
		return
	}

	lastID := wp.workerIDs[len(wp.workerIDs)-1]

	if worker, exists := wp.workers[lastID]; exists {
		worker.Stop()
		delete(wp.workers, lastID)

		wp.workerIDs = wp.workerIDs[:len(wp.workerIDs)-1]

		if wp.lastWorkerIndex >= len(wp.workerIDs) && len(wp.workerIDs) > 0 {
			wp.lastWorkerIndex = len(wp.workerIDs) - 1
		}

		fmt.Printf("Removed worker %d (total: %d)\n", lastID, len(wp.workers))
	}
}

// Добавить воркера с ожиданием
func (wp *WorkerPool) AddWorker() {
	select {
	case wp.addWorker <- true:
		time.Sleep(50 * time.Millisecond)
	default:
		fmt.Println("Failed to add worker - channel full")
	}
}

// Удалить воркера с ожиданием
func (wp *WorkerPool) RemoveWorker() {
	select {
	case wp.removeWorker <- true:
		time.Sleep(50 * time.Millisecond)
	default:
		fmt.Println("Failed to remove worker - channel full")
	}
}

// Отправить задание
func (wp *WorkerPool) SubmitJob(job string) {
	select {
	case wp.jobChannel <- job:
		fmt.Printf("Submitted job: %s\n", job)
	default:
		fmt.Printf("Job queue full, dropping job: %s\n", job)
	}
}

// Получить результаты
func (wp *WorkerPool) Results() <-chan string {
	return wp.results
}

// Получить количество активных воркеров
func (wp *WorkerPool) WorkerCount() int {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	return len(wp.workers)
}

// Получить статистику распределения
func (wp *WorkerPool) GetDistributionStats() map[int]int {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	stats := make(map[int]int)
	for _, id := range wp.workerIDs {
		stats[id] = 0
	}
	return stats
}

// Остановить пул и дождаться завершения всех воркеров
func (wp *WorkerPool) Stop() {
	wp.mu.Lock()
	if !wp.running {
		wp.mu.Unlock()
		return
	}
	wp.running = false

	for _, worker := range wp.workers {
		worker.Stop()
	}
	wp.mu.Unlock()

	close(wp.stopChan)

	wp.wg.Wait()

	close(wp.results)
	fmt.Println("WorkerPool stopped")
}

func main() {
	pool := NewWorkerPool()
	pool.Start()

	time.Sleep(100 * time.Millisecond)

	fmt.Println("=== Создание воркеров ===")
	pool.AddWorker()
	pool.AddWorker()
	pool.AddWorker()

	fmt.Printf("Active workers: %d\n", pool.WorkerCount())

	resultsDone := make(chan bool)
	go func() {
		defer close(resultsDone)
		for result := range pool.Results() {
			fmt.Println("✓ Result:", result)
		}
	}()

	fmt.Println("\n=== Отправка заданий (round-robin) ===")
	jobs := []string{"job1", "job2", "job3", "job4", "job5", "job6"}
	for _, job := range jobs {
		pool.SubmitJob(job)
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(3 * time.Second)

	fmt.Println("\n=== Динамическое управление воркерами ===")
	pool.AddWorker()
	fmt.Printf("Active workers after adding: %d\n", pool.WorkerCount())

	pool.RemoveWorker()
	fmt.Printf("Active workers after removing: %d\n", pool.WorkerCount())

	fmt.Println("\n=== Дополнительные задания ===")
	moreJobs := []string{"job7", "job8", "job9"}
	for _, job := range moreJobs {
		pool.SubmitJob(job)
		time.Sleep(30 * time.Millisecond)
	}

	time.Sleep(4 * time.Second)

	fmt.Println("\n=== Завершение работы ===")
	pool.Stop()

	<-resultsDone
	fmt.Println("Program completed")
}
