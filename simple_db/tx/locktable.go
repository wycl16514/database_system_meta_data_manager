package tx

import (
	"errors"
	fm "file_manager"
	"math/rand"
	"sync"
	"time"
)

const (
	MAX_WAITING_TIME = 10 //3用于测试，在正式使用时设置为10
)

type LockTable struct {
	lock_map    map[fm.BlockId]int64           //将锁和区块对应起来
	notify_chan map[fm.BlockId]chan struct{}   //用于实现超时回退的管道
	notify_wg   map[fm.BlockId]*sync.WaitGroup //用于实现唤醒通知
	method_lock sync.Mutex                     //实现方法调用的线程安全，相当于java的synchronize关键字
}

var lock_table_instance *LockTable
var lock = &sync.Mutex{}

func GetLockTableInstance() *LockTable {
	lock.Lock()
	defer lock.Unlock()
	if lock_table_instance == nil {
		lock_table_instance = NewLockTable()
	}

	return lock_table_instance
}

func (l *LockTable) waitGivenTimeOut(blk *fm.BlockId) {
	wg, ok := l.notify_wg[*blk]
	if !ok {
		var new_wg sync.WaitGroup
		l.notify_wg[*blk] = &new_wg
		wg = &new_wg
	}
	wg.Add(1)
	defer wg.Done()
	l.method_lock.Unlock() //挂起前释放方法锁
	select {
	case <-time.After(MAX_WAITING_TIME * time.Second):
		//fmt.Println("routine wake up for timeout")
	case <-l.notify_chan[*blk]:
		//fmt.Println("routine wake up by notify channel")
	}
	l.method_lock.Lock() //唤起后加上方法锁
}

func (l *LockTable) notifyAll(blk *fm.BlockId) {
	//s := fmt.Sprintf("close channle for blk :%v\n", *blk)
	//fmt.Println(s)

	channel, ok := l.notify_chan[*blk]
	if ok {
		close(channel)
		delete(l.notify_chan, *blk)
		mark := rand.Intn(10000)

		//s := fmt.Sprintf("delete blk: %v and launch rotinue to create it, mark: %d\n", *blk, mark)
		//fmt.Print(s)

		go func(blk_unlock fm.BlockId, ran_num int) {
			//等待所有线程返回后再重新设置channel
			//注意这个线程不一定得到及时调度，因此可能不能及时创建channel对象从而导致close closed channel panic
			//s := fmt.Sprintf("wait group for blk: %v, with mark:%d\n", blk_unlock, ran_num)
			//fmt.Print(s)
			l.notify_wg[blk_unlock].Wait()
			//访问内部数据时需要加锁
			l.method_lock.Lock()
			l.notify_chan[blk_unlock] = make(chan struct{})
			l.method_lock.Unlock()
			//s = fmt.Sprintf("create notify channel for %v\n", blk_unlock)
			//fmt.Print(s)

		}(*blk, mark)
	} else {
		//s = fmt.Sprintf("channel for %v is already closed\n", *blk)
		//fmt.Print(s)
	}
}

func NewLockTable() *LockTable {
	/*
		如果给定blk对应的值为-1，表明有互斥锁,如果大于0表明有相应数量的共享锁加在对应区块上，
		如果是0则表示没有锁
	*/
	lock_table := &LockTable{
		lock_map:    make(map[fm.BlockId]int64),
		notify_chan: make(map[fm.BlockId]chan struct{}),
		notify_wg:   make(map[fm.BlockId]*sync.WaitGroup),
	}

	return lock_table
}

func (l *LockTable) initWaitingOnBlk(blk *fm.BlockId) {
	_, ok := l.notify_chan[*blk]
	if !ok {
		l.notify_chan[*blk] = make(chan struct{})
	}

	_, ok = l.notify_wg[*blk]
	if !ok {
		l.notify_wg[*blk] = &sync.WaitGroup{}
	}
}

func (l *LockTable) SLock(blk *fm.BlockId) error {
	l.method_lock.Lock()
	defer l.method_lock.Unlock()
	l.initWaitingOnBlk(blk)

	start := time.Now()
	for l.hasXlock(blk) && !l.waitingTooLong(start) {
		l.waitGivenTimeOut(blk)
	}
	//如果等待过长时间，有可能是产生了死锁
	if l.hasXlock(blk) {
		//fmt.Println("slock fail for xlock")
		return errors.New("SLock Exception: XLock on given blk")
	}

	val := l.getLockVal(blk)
	l.lock_map[*blk] = val + 1
	return nil
}

func (l *LockTable) XLock(blk *fm.BlockId) error {
	l.method_lock.Lock()
	defer l.method_lock.Unlock()
	l.initWaitingOnBlk(blk)

	start := time.Now()
	for l.hasOtherSLocks(blk) && !l.waitingTooLong(start) {
		//	fmt.Println("get xlock fail and sleep")
		l.waitGivenTimeOut(blk)
	}

	if l.hasOtherSLocks(blk) {
		return errors.New("XLock error: SLock on given blk")
	}

	//-1表示区块被加上互斥锁
	l.lock_map[*blk] = -1

	return nil
}

func (l *LockTable) UnLock(blk *fm.BlockId) {
	l.method_lock.Lock()
	defer l.method_lock.Unlock()

	val := l.getLockVal(blk)
	if val > 1 {
		l.lock_map[*blk] = val - 1
	} else {
		delete(l.lock_map, *blk)
		//通知所有等待给定区块的线程从Wait中恢复
		//	s := fmt.Sprintf("unlock by blk: +%v\n", *blk)
		//fmt.Println(s)
		l.notifyAll(blk)
	}
}

func (l *LockTable) hasXlock(blk *fm.BlockId) bool {
	return l.getLockVal(blk) < 0
}

func (l *LockTable) hasOtherSLocks(blk *fm.BlockId) bool {
	/*
		这里必须要大于1，因为同一个线程可以先获取读锁再获取写锁,同一个线程获取读锁时会让计数加1，
		如果获取写锁时对应读锁的计数只有1，那意味着读锁就是本线程获得的，于是可以直接获得写锁
	*/
	return l.getLockVal(blk) > 1
}

func (l *LockTable) waitingTooLong(start time.Time) bool {
	elapsed := time.Since(start).Seconds()
	if elapsed >= MAX_WAITING_TIME {
		return true
	}

	return false
}

func (l *LockTable) getLockVal(blk *fm.BlockId) int64 {
	val, ok := l.lock_map[*blk]
	if !ok {
		l.lock_map[*blk] = 0
		return 0
	}

	return val
}
