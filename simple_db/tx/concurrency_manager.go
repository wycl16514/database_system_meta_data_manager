package tx

import (
	fm "file_manager"
)

type ConCurrencyManager struct {
	lock_table *LockTable
	lock_map   map[fm.BlockId]string
}

func NewConcurrencyManager() *ConCurrencyManager {
	concurrency_mgr := &ConCurrencyManager{
		lock_table: GetLockTableInstance(),
		lock_map:   make(map[fm.BlockId]string),
	}

	return concurrency_mgr
}

func (c *ConCurrencyManager) SLock(blk *fm.BlockId) error {
	_, ok := c.lock_map[*blk]
	if !ok {
		err := c.lock_table.SLock(blk)
		if err != nil {
			return err
		}
		c.lock_map[*blk] = "S"
	}
	return nil
}

func (c *ConCurrencyManager) XLock(blk *fm.BlockId) error {
	if !c.hasXLock(blk) {
		c.SLock(blk)
		/*
			之所以在获取写锁之前获取读锁，是因为同一个线程可以在获得读锁的情况下再获取写锁。
			获取读锁时，读锁的计数会加1，如果读锁的计数大于1，说明其他线程对同一个区块加了读锁，
			此时获取写锁就要失败，如果读锁计数只有1，那意味着读锁是上面获取的，也就是同一个线程获取到了读锁
			于是，同一个线程就可以在读锁基础上添加写锁
		*/
		err := c.lock_table.XLock(blk)
		if err != nil {
			return err
		}
		c.lock_map[*blk] = "X"
	}

	return nil
}

func (c *ConCurrencyManager) Release() {
	for key, _ := range c.lock_map {
		c.lock_table.UnLock(&key)
	}
}

func (c *ConCurrencyManager) hasXLock(blk *fm.BlockId) bool {
	lock_type, ok := c.lock_map[*blk]
	return ok && lock_type == "X"
}
