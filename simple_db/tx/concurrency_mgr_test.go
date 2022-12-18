package tx

import (
	bm "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"testing"
	"time"
)

func TestCurrencyManager(_ *testing.T) {
	file_manager, _ := fm.NewFileManager("txtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile")
	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)
	//tx.NewTransation(file_manager, log_manager, buffer_manager)
	go func() {
		txA := NewTransation(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockId("testfile", 1)
		blk2 := fm.NewBlockId("testfile", 2)
		txA.Pin(blk1)
		txA.Pin(blk2)
		fmt.Println("Tx A: rquest slock 1")
		txA.GetInt(blk1, 0) //如果返回错误，我们应该放弃执行下面操作并执行回滚，这里为了测试而省略
		fmt.Println("Tx A: receive slock 1")
		time.Sleep(2 * time.Second)
		fmt.Println("Tx A: request slock 2")
		txA.GetInt(blk2, 0)
		fmt.Println("Tx A: receive slock 2")
		fmt.Println("Tx A: Commit")
		txA.Commit()
	}()

	go func() {
		time.Sleep(1 * time.Second)
		txB := NewTransation(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockId("testfile", 1)
		blk2 := fm.NewBlockId("testfile", 2)
		txB.Pin(blk1)
		txB.Pin(blk2)
		fmt.Println("Tx B: rquest xlock 2")
		txB.SetInt(blk2, 0, 0, false)
		fmt.Println("Tx B: receive xlock 2")
		time.Sleep(2 * time.Second)
		fmt.Println("Tx B: request slock 1")
		txB.GetInt(blk1, 0)
		fmt.Println("Tx B: receive slock 1")
		fmt.Println("Tx B: Commit")
		txB.Commit()
	}()

	go func() {
		time.Sleep(2 * time.Second)
		txC := NewTransation(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockId("testfile", 1)
		blk2 := fm.NewBlockId("testfile", 2)
		txC.Pin(blk1)
		txC.Pin(blk2)
		fmt.Println("Tx C: rquest xlock 1")
		txC.SetInt(blk1, 0, 0, false)
		fmt.Println("Tx C: receive xlock 1")
		time.Sleep(1 * time.Second)
		fmt.Println("Tx C: request slock 2")
		txC.GetInt(blk2, 0)
		fmt.Println("Tx C: receive slock 2")
		fmt.Println("Tx C: Commit")
		txC.Commit()
	}()

	time.Sleep(20 * time.Second)
}
