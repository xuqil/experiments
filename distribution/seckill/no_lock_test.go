package seckill

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestNoLockSecKill(t *testing.T) {
	inventory := 1000

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(inventory *int) {
			defer wg.Done()
			time.Sleep(2 * time.Millisecond)
			*inventory -= 1
		}(&inventory)
	}

	wg.Wait()
	fmt.Println("inventory:", inventory)
}

func TestMutexLock(t *testing.T) {
	inventory := 1000

	var wg sync.WaitGroup
	var lock sync.Mutex
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(inventory *int) {
			defer wg.Done()
			time.Sleep(2 * time.Millisecond)
			lock.Lock()
			*inventory -= 1
			lock.Unlock()
		}(&inventory)
	}

	wg.Wait()
	fmt.Println("inventory:", inventory)
}

func TestProcessLock(t *testing.T) {
	lockFile := "./lock.pid"
	lock, err := os.Create(lockFile)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(lockFile)
	defer lock.Close()

	err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		t.Fatal("running, ./lock.pid exists", err)
	}
}

func TestSlice(t *testing.T) {
	s1 := []int{1, 2}
	s2 := s1
	s2 = append(s2, 3)
	SliceRise(s1)
	SliceRise(s2)
	fmt.Println(s1, s2)
}

func SliceRise(s []int) {
	s = append(s, 0)
	for i := range s {
		s[i]++
	}
}
