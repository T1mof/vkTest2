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
		jobQueue: make(chan string, 5), // Буферизованный канал для заданий
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
				// Симуляция обработки задания
				time.Sleep(100 * time.Millisecond)
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

// WorkerPool структура
type WorkerPool struct {
	workers      map[int]*Worker
	jobChannel   chan string
	results      chan string
	wg           sync.WaitGroup
	addWorker    chan bool
	removeWorker chan bool
	workerID     int
	mu           sync.RWMutex
	running      bool
	stopChan     chan struct{}
}

// Создание нового пула
func NewWorkerPool() *WorkerPool {
	return &WorkerPool{
		workers:      make(map[int]*Worker),
		jobChannel:   make(chan string, 100),
		results:      make(chan string, 100),
		addWorker:    make(chan bool, 10), // Буферизованный канал
		removeWorker: make(chan bool, 10), // Буферизованный канал
		stopChan:     make(chan struct{}),
		running:      false,
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
				wp.distributeJob(job)
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

// Распределение задания между воркерами
func (wp *WorkerPool) distributeJob(job string) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if len(wp.workers) == 0 {
		// Если нет воркеров, возвращаем задание в очередь
		go func() {
			time.Sleep(50 * time.Millisecond)
			select {
			case wp.jobChannel <- job:
			default:
				fmt.Printf("Failed to requeue job: %s\n", job)
			}
		}()
		return
	}

	// Пробуем отправить задание любому доступному воркеру
	for _, worker := range wp.workers {
		select {
		case worker.jobQueue <- job:
			return // Задание успешно отправлено
		default:
			continue // Воркер занят, пробуем следующего
		}
	}

	// Если все воркеры заняты, ждем и повторяем попытку
	go func() {
		time.Sleep(50 * time.Millisecond)
		select {
		case wp.jobChannel <- job:
		default:
			fmt.Printf("Failed to requeue job: %s\n", job)
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

	// Находим воркера с наибольшим ID для удаления
	maxID := 0
	for id := range wp.workers {
		if id > maxID {
			maxID = id
		}
	}

	if worker, exists := wp.workers[maxID]; exists {
		worker.Stop()
		delete(wp.workers, maxID)
		fmt.Printf("Removed worker %d (total: %d)\n", maxID, len(wp.workers))
	}
}

// Добавить воркера
func (wp *WorkerPool) AddWorker() {
	select {
	case wp.addWorker <- true:
	default:
		fmt.Println("Failed to add worker - channel full")
	}
}

// Удалить воркера
func (wp *WorkerPool) RemoveWorker() {
	select {
	case wp.removeWorker <- true:
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

// Остановить пул и дождаться завершения всех воркеров
func (wp *WorkerPool) Stop() {
	wp.mu.Lock()
	if !wp.running {
		wp.mu.Unlock()
		return
	}
	wp.running = false

	// Останавливаем все воркеры
	for _, worker := range wp.workers {
		worker.Stop()
	}
	wp.mu.Unlock()

	// Останавливаем главную горутину пула
	close(wp.stopChan)

	// Ждем завершения всех воркеров
	wp.wg.Wait()

	// Закрываем канал результатов
	close(wp.results)
	fmt.Println("WorkerPool stopped")
}

func main() {
	pool := NewWorkerPool()
	pool.Start()

	// Даем время пулу запуститься
	time.Sleep(100 * time.Millisecond)

	fmt.Println("=== Создание воркеров ===")
	pool.AddWorker()
	pool.AddWorker()
	pool.AddWorker()

	// Даем время воркерам запуститься
	time.Sleep(200 * time.Millisecond)
	fmt.Printf("Active workers: %d\n", pool.WorkerCount())

	// Запускаем обработчик результатов
	resultsDone := make(chan bool)
	go func() {
		defer close(resultsDone)
		for result := range pool.Results() {
			fmt.Println("✓ Result:", result)
		}
	}()

	fmt.Println("\n=== Отправка заданий ===")
	jobs := []string{"job1", "job2", "job3", "job4", "job5"}
	for _, job := range jobs {
		pool.SubmitJob(job)
		time.Sleep(10 * time.Millisecond) // Небольшая задержка между заданиями
	}

	// Ждем обработки первой партии заданий
	time.Sleep(2 * time.Second)

	fmt.Println("\n=== Динамическое управление воркерами ===")
	pool.AddWorker()
	fmt.Printf("Active workers after adding: %d\n", pool.WorkerCount())

	time.Sleep(1 * time.Second)

	pool.RemoveWorker()
	fmt.Printf("Active workers after removing: %d\n", pool.WorkerCount())

	// Отправляем дополнительные задания
	fmt.Println("\n=== Дополнительные задания ===")
	moreJobs := []string{"job6", "job7", "job8"}
	for _, job := range moreJobs {
		pool.SubmitJob(job)
	}

	// Ждем завершения всех заданий
	time.Sleep(3 * time.Second)

	fmt.Println("\n=== Завершение работы ===")
	pool.Stop()

	// Ждем завершения обработчика результатов
	<-resultsDone
	fmt.Println("Program completed")
}
