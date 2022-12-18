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

func NewViewManager(isNew bool, tblMgr *TableManager, tx *tx.Transation) *ViewManager {
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

func (v *ViewManager) CreateView(vname string, vdef string, tx *tx.Transation) {
	//每创建一个视图对象，就在viewcat表中插入一条对该视图对象元数据的记录
	layout := v.tblMgr.GetLayout("viewcat", tx)
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
		if ts.GetString("viewname") == vname {
			result = ts.GetString("viewdef")
			break
		}
	}

	ts.Close()
	return result
}
