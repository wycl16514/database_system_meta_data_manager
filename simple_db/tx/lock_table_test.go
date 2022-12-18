package tx

import (
	"errors"
	fm "file_manager"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestGetManySlockOneTime(t *testing.T) {
	lock_table := NewLockTable()
	blk := fm.NewBlockId("testfile", 1)

	lock_table.SLock(blk)
	err := lock_table.SLock(blk)
	require.Nil(t, err)
}

func TestGetXlockFailForOnlyUnLockOneSLocForTwoSLock(t *testing.T) {
	lock_table := NewLockTable()
	blk := fm.NewBlockId("testfile", 1)

	lock_table.SLock(blk)
	lock_table.SLock(blk)

	start := time.Now()
	err := lock_table.XLock(blk)
	elapsed := time.Since(start).Seconds()
	require.Equal(t, elapsed >= MAX_WAITING_TIME, true)
	require.Equal(t, err, errors.New("XLock error: SLock on given blk"))
}

func TestGetSLockFailWithoutRealseXLock(t *testing.T) {
	lock_table := NewLockTable()
	blk := fm.NewBlockId("testfile", 1)
	lock_table.XLock(blk)
	start := time.Now()
	err := lock_table.SLock(blk)
	elapsed := time.Since(start).Seconds()
	require.Equal(t, elapsed >= MAX_WAITING_TIME, true)
	require.Equal(t, err, errors.New("SLock Exception: XLock on given blk"))
}

func TestRoutinesWithSLockTimeout(t *testing.T) {
	var err_array []error
	var err_array_lock sync.Mutex
	blk := fm.NewBlockId("testfile", 1)
	lock_table := NewLockTable()
	lock_table.XLock(blk)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			err_array_lock.Lock()
			defer err_array_lock.Unlock()
			err := lock_table.SLock(blk)
			if err == nil {
				fmt.Println("access slock ok")
			}
			err_array = append(err_array, err)
		}()
	}
	time.Sleep(1 * time.Second) //让线程都运行起来
	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	require.Equal(t, elapsed >= MAX_WAITING_TIME, true)
	require.Equal(t, len(err_array), 3)
	for i := 0; i < 3; i++ {
		require.Equal(t, err_array[i], errors.New("SLock Exception: XLock on given blk"))
	}
}

func TestRoutinesWithSLockAfterXLockRelease(t *testing.T) {
	var err_array []error
	var err_array_lock sync.Mutex
	blk := fm.NewBlockId("testfile", 1)
	lock_table := NewLockTable()
	lock_table.XLock(blk)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			err_array_lock.Lock()
			defer err_array_lock.Unlock()
			err := lock_table.SLock(blk)
			if err == nil {
				fmt.Println("access slock ok")
			}
			err_array = append(err_array, err)
		}()
	}
	time.Sleep(1 * time.Second) //让线程都运行起来
	lock_table.UnLock(blk)      //释放加在区块上的互斥锁
	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	require.Equal(t, elapsed < MAX_WAITING_TIME, true)
	require.Equal(t, len(err_array), 3)
	for i := 0; i < 3; i++ {
		require.Nil(t, err_array[i]) //所有线程能获得共享锁然后读取数据
	}

	require.Equal(t, lock_table.lock_map[*blk], int64(3))
}
