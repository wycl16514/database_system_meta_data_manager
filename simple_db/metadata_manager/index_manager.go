package metadata_manager

/*
//索引管理器需要使用到后面才讲到的SQL查询和索引算法知识，所以先放一放
import (
	rm "record_manager"
	"tx"
)

type IndexManager struct {
	layout  *rm.Layout
	tblMgr  *TableManager
	statMgr *StatManager
}

func NewIndexManager(isNew bool, tblMgr *TableManager, statMgr *StatManager, tx *tx.Transation) *IndexManager {
	if isNew {
		//索引元数据表包含三个字段，索引名，对应的表名，被索引的字段名
		sch := rm.NewSchema()
		sch.AddStringField("indexname", MAX_NAME)
		sch.AddStringField("tablename", MAX_NAME)
		sch.AddStringField("fieldname", MAX_NAME)
		tblMgr.CreateTable("idxcat", sch, tx)
	}

	idxMgr := &IndexManager{
		tblMgr:  tblMgr,
		statMgr: statMgr,
		layout:  tblMgr.GetLayout("idxcat", tx),
	}

	return idxMgr
}

func (i *IndexManager) CreateIndex(idxName string, tblName string, fldName string, tx *tx.Transation) {
	//创建索引时就为其在idxcat索引元数据表中加入一条记录
	ts := rm.NewTableScan(tx, "idxcat", layout)
	ts.Insert()
	ts.SetString("indexname", idxName)
	ts.SetString("tablename", tableName)
	ts.SetString("fieldname", fldName)
	ts.Close()
}

func (i *IndexManager) GetIndexInfo(tblName string, tx *tx.Transation) map[strng]*IndexInfo {
	result := make(map[string]*IndexInfo)
	ts := rm.NewTableScan(tx, "idxcat", i.layout)
	for ts.Next() {
		if ts.GetString("tablename") == tblName {
			idxName := ts.GetString("indexname")
			fldName := ts.GetString("fieldname")
			tblLayout := i.tblMgr.GetLayout(tblName, tx)
			tblSi := i.statMgr.GetStatInfo(tblName, tblLayout, tx)
			ii := newIndexInfo(idxName, fldName, tblLayout.Schema(), tx, tblSi)
			result[fldName] = ii
		}

		ts.Close()
		return result
	}
}
*/
