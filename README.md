在数据库中，除了数据表外，还有一个重要对象叫视图。视图是由SQL语句将不同字段从不同表中抽取或者构造后形成的新表，跟数据库表不同在于，它不存储在磁盘上，而是在使用时临时构建出来。

跟数据库表一样，视图同样需要进行元数据管理。跟上节相同我们定义一个ViewManager来创建视图，同时创建一个viewcat数据库表来存储视图的元数据，这个表有两个字段分别是ViewName,他是字符串类型，还有一个叫ViewDef，他是一个二进制数据类型，具体细节在后面的实现中会清楚说明。

我们看看代码实现，首先在metadata_management目录下创建文件view_manager.go文件，然后输入代码如下：
```go
package metadata_manager

import (
	rm "record_manager"
	"tx"
)

const (
	/*
		视图元数据对应数据结构大小，通常视图元数据的定义需要几千个字节，我们这里出于学习和实验目的，只把使用
		100字节来表示
	*/
	MAX_VIEWDEF = 100
)

type ViewManager struct {
	tblMgr *TableManager
}

func NewViewManager(isNew bool, tblMgr *TableManager, tx *rm.Transation) *ViewManager {
	viewMgr := &ViewManager{
		tblMgr: tblMgr,
	}

	if isNew {
		//使用表管理器创建元数据表viewcat
		sch := rm.NewSchema()
		sch.AddStringField("viewname", MAX_NAME)
		sch.AddStringField("viewdef", MAX_VIEWDEF)
		tblMgr.CreateTable("viewcat", sch, tx)
	}

	return viewMgr
}

func (v *ViewManager) CreateView(vname string, vdef string, tx *rm.Transation) {
	//每创建一个视图对象，就在viewcat表中插入一条对该视图对象元数据的记录
	layout := v.tblMgr.getLayout("viewcat", tx)
	ts := rm.NewTableScan(tx, "viewcat", layout)
	ts.Insert()
	ts.SetString("viewname", vname)
	ts.SetString("viewdef", vdef)
	ts.Close()
}

func (v *ViewManager) GetViewDef(vname string, tx *tx.Transation) string {
	result := ""
	layout := v.tblMgr.GetLayout("viewcat", tx)
	//获取视图的表结构
	ts := rm.NewTableScan(tx, "viewcat", layout)
	for ts.Next() {
		if ts.GetString("viewcat") == vname {
			result = ts.GetString("viewdef")
			break
		}
	}
	
	ts.Close()
	return result
}

```
另外还需要考虑的元数据是统计信息。统计信息一般包含当前有多少条记录，字段在磁盘中的分布信息等，这些数据在引擎执行查询时用于估计成本，统计信息处理的好能大大加快查询速度。如果是商用数据库，这些信息将会多如牛毛，我们这里简单起见就保持三种数据即可，他们分别是每个表使用了多少区块，每个表包含了多少条记录，对于某个表中的某个字段，它有多少个不重复的值。

不难看到维护这些统计信息需要付出一定的性能代价，因为当数据库表有插入，删除，更新等操作时，我们都得对统计信息进行更新，为了处理这个问题我们不再像前面那样使用元数据表来存放统计数据，而是把统计信息全部保留在内存里，当数据库系统启动时，它扫描一次所有数据库表，构造出统计信息寄存在内存中。同时每过一段时间系统就扫描数据库表然后更新统计数据。这种做法的问题在于在某个时刻统计信息跟实际情况有所不符，但问题不大，因为这些信息主要用来估算查询成本，它不是很准确问题也不大。我们看看统计元数据的实现，在当前目录增加一个文件名为stat_manager.go，实现代码如下：
```go
package metadata_manager

import (
	rm "record_manager"
	"sync"
	"tx"
)

const (
	//数据库表发生变化100次后更新统计数据
	REFRESH_STAT_INFO_COUNT = 100
)

type StatInfo struct {
	numBlocks int //数据库表的区块数
	numRecs   int //数据库表包含的记录数
}

func newStatInfo(numBlocks int, numRecs int) *StateInfo {
	return &StatInfo{
		numBlocks: numBlocks,
		numRecs:   numRecs,
	}
}

func (s *StatInfo) BlocksAccessed() int {
	return s.numBlocks
}

func (s *StatInfo) RecordsOutput() int {
	return s.numRecs
}

func (s *StatInfo) DistincValues(fldName string) int {
	//字段包含多少不同的值
	return 1 + (s.numRecs / 3) //初步认为三分之一，后面再修改
}

type StatManager struct {
	tblMgr     *TableManager
	tableStats map[string]*StateInfo
	numCalls   int
	lock       sync.Mutex
}

func NewStatManager(tblMgr *TableManager, tx *tx.Transation) *StatManager {
	statMgr := &StatManager{
		tblMgr:   tblMgr,
		numCalls: 0,
	}
	//更新统计数据
	statMgr.refreshStatistics(tx)
	return statMgr
}

func (s *StatManager) GetStatInfo(tblName string, layout *rm.Layout, tx *tx.Transation) *StatInfo {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.numCalls += 1
	if s.numCalls > REFRESH_STAT_INFO_COUNT {
		s.refreshStatistic(tx)
	}

	si := s.tableStats[talName]
	if si == nil {
		//为新数据库表创建统计对象
		si = s.calcTableStats(tblName, layout, tx)
		s.tableStats[tblName] = si
	}

	return si
}

func (s *StatManager) refreshStatistics(tx *tx.Transation) {
	s.tableStats = make(map[string]*StatInfo)
	s.numCalls = 0
	tcatLayout := s.tblMgr.GetLayout("tblcat", tx)
	tcat := rm.NewTableScan(tx, "tblcat", tcatLayout)
	for tcat.Next() {
		tblName := tcat.GetString("tblname")
		layout := s.tblMgr.GetLayout(tblName, tx)
		si := s.calcTableStats(tblName, layout, tx)
		s.tableStats[tblName] = si
	}

	tcat.Close()
}

func (s *StatManager) calcTableStats(tblName string, layout *rm.Layout, tx *tx.Transation) *StatInfo {
	numRecs := 0
	numBlocks := 0
	ts := rm.NewTableScan(tx, tblName, layout)
	for ts.Next() {
		numRecs += 1
		numBlocks = ts.GetRid().BlockNumber() + 1
	}
	ts.Close()
	return newStatInfo(numRecs, numBlocks)
}

```
在上面代码中，我们使用对象StatInfo来包含表的统计信息，其中包括表的记录数，区块数还有给定字段拥有的不同值的数量。StatManager用于获取统计元数据，它只在系统启动时创建，在创建时它调用自己的refreshStatistics接口创建统计数据并存储在内存中，这个接口会继续调用calcTableStats来获取每个表的相关数据，后者会从通过TableManage获取所有数据库表，然后获得每个表的相关数据，然后创建StatInfo对象，并把表的统计数据存储在其中。

最后我们使用一个名为MetaDataManager的对象将前面实现的所有Manager统一管理起来，在目录中创建meta_manager.go实现代码如下：
```go
package metadata_manager

import (
	rm "record_manager"
	"tx"
)

type MetaDataManager struct {
	tblMgr  *TableManager
	viewMgr *ViewManager
	statMgr *StatManager
	//索引管理器以后再处理
	//idxMgr *IndexManager
}

func NewMetaDataManager(isNew bool, tx *tx.Transation) *MetaDataManager {
	metaDataMgr := &MetaDataManager{
		tblMgr: NewTableManager(isNew, tx),
	}

	metaDataMgr.viewMgr = NewViewManager(isNew, metaDataMgr.tblMgr, tx)
	metaDataMgr.statMgr = NewStatManager(metaDataMgr.tblMgr, tx)

	return metaDataMgr
}

func (m *MetaDataManager) CreateTable(tblName string, sch *rm.Schema, tx *tx.Transation) {
	m.tblMgr.CreateTable(tblName, sch, tx)
}

func (m *MetaDataManager) CreateView(viewName string, viewDef string, tx *tx.Transation) {
	m.viewMgr.CreateView(viewName, viewDef, tx)
}

func (m *MetaDataManager) GetLayout(tblName string, tx *tx.Transation) *rm.Layout {
	return m.tblMgr.GetLayout(tblName, tx)
}

func (m *MetaDataManager) GetViewDef(viewName string, tx *tx.Transation) string {
	return m.viewMgr.GetViewDef(viewName, tx)
}

func (m *MetaDataManager) GetStatInfo(tblName string, layout *rm.Layout, tx *tx.Transation) *StatInfo {
	return m.statMgr.GetStatInfo(tblName, layout, tx)
}

```

最后我们在main函数中调用MetaDataManager看看效果：
```go
package main

import (
	bmg "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"math/rand"
	mm "metadata_management"
	record_mgr "record_manager"
	"tx"
)

func main() {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
	buffer_manager := bmg.NewBufferManager(file_manager, log_manager, 3)

	tx := tx.NewTransation(file_manager, log_manager, buffer_manager)
	sch := record_mgr.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)

	mdm := mm.NewMetaDataManager(true, tx)
	mdm.CreateTable("MyTable", sch, tx)
	layout := mdm.GetLayout("MyTable", tx)
	size := layout.SlotSize()
	fmt.Printf("MyTable has slot size: %d\n", size)
	sch2 := layout.Schema()
	fmt.Println("Its fields are: ")
	for _, fldName := range sch2.Fields() {
		fldType := ""
		if sch2.Type(fldName) == record_mgr.INTEGER {
			fldType = "int"
		} else {
			strlen := sch2.Length(fldName)
			fldType = fmt.Sprintf("varchar ( %d )", strlen)
		}

		fmt.Printf("%s :  %s\n", fldName, fldType)
	}

	ts := record_mgr.NewTableScan(tx, "MyTable", layout)
	//测试统计元数据
	for i := 0; i < 50; i++ {
		ts.Insert()
		n := rand.Intn(50)
		ts.SetInt("A", n)
		strField := fmt.Sprintf("rec%d", n)
		ts.SetString("B", strField)
	}
	si := mdm.GetStatInfo("MyTable", layout, tx)
	fmt.Printf("blocks for MyTable is %d\n", si.BlocksAccessed())
	fmt.Printf("records for MyTable is :%d\n", si.RecordsOutput())
	fmt.Printf("Distinc values for field A is %d\n", si.DistinctValues("A"))
	fmt.Printf("Distinc values for field B is %d\n", si.DistinctValues("B"))

	//统计视图信息
	viewDef := "select B from MyTable where A = 1"
	mdm.CreateView("viewA", viewDef, tx)
	v := mdm.GetViewDef("viewA", tx)
	fmt.Printf("View def = %s\n", v)
	tx.Commit()
}

```

上面代码运行后输出结果如下：
```go
MyTable has slot size: 33
Its fields are: 
A :  int
B :  varchar ( 9 )
blocks for MyTable is 5
records for MyTable is :50
Distinc values for field A is 17
Distinc values for field B is 17
View def = select B from MyTable where A = 1
transation 1  committed
```

具体的逻辑请在B站搜索Coding迪斯尼查看调试演示和逻辑讲解。
