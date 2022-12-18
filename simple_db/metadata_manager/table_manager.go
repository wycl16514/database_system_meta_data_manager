package metadata_manager

import (
	rm "record_manager"
	"tx"
)

const (
	MAX_NAME = 16
)

type TableManager struct {
	tcatLayout *rm.Layout
	fcatLayout *rm.Layout
}

func NewTableManager(isNew bool, tx *tx.Transation) *TableManager {
	tableMgr := &TableManager{}
	tcatSchema := rm.NewSchema()
	//创建两个表专门用于存储新建数据库表的元数据
	tcatSchema.AddStringField("tblname", MAX_NAME)
	tcatSchema.AddIntField("slotsize")
	tableMgr.tcatLayout = rm.NewLayoutWithSchema(tcatSchema)

	fcatSchema := rm.NewSchema()
	fcatSchema.AddStringField("tblname", MAX_NAME)
	fcatSchema.AddStringField("fldname", MAX_NAME)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	tableMgr.fcatLayout = rm.NewLayoutWithSchema(fcatSchema)

	if isNew {
		//如果当前数据表是第一次创建，那么为这个表创建两个元数据表
		tableMgr.CreateTable("tblcat", tcatSchema, tx)
		tableMgr.CreateTable("fldcat", fcatSchema, tx)
	}

	return tableMgr
}

func (t *TableManager) CreateTable(tblName string, sch *rm.Schema, tx *tx.Transation) {
	//在创建数据表前先创建tblcat, fldcat两个元数据表
	layout := rm.NewLayoutWithSchema(sch)
	tcat := rm.NewTableScan(tx, "tblcat", t.tcatLayout)
	tcat.Insert()
	tcat.SetString("tblname", tblName)
	tcat.SetInt("slotsize", layout.SlotSize())
	tcat.Close()
	fcat := rm.NewTableScan(tx, "fldcat", t.fcatLayout)
	for _, fldName := range sch.Fields() {
		fcat.Insert()
		fcat.SetString("tblname", tblName)
		fcat.SetString("fldname", fldName)
		fcat.SetInt("type", int(sch.Type(fldName)))
		fcat.SetInt("length", sch.Length(fldName))
		fcat.SetInt("offset", layout.Offset(fldName))
	}
	fcat.Close()
}

func (t *TableManager) GetLayout(tblName string, tx *tx.Transation) *rm.Layout {
	//获取给定表的layout结构
	size := -1
	tcat := rm.NewTableScan(tx, "tblcat", t.tcatLayout)
	for tcat.Next() {
		//找到给定表对应的元数据表
		if tcat.GetString("tblname") == tblName {
			size = tcat.GetInt("slotsize")
			break
		}
	}
	tcat.Close()
	sch := rm.NewSchema()
	offsets := make(map[string]int)
	fcat := rm.NewTableScan(tx, "fldcat", t.fcatLayout)
	for fcat.Next() {
		if fcat.GetString("tblname") == tblName {
			fldName := fcat.GetString("fldname")
			fldType := fcat.GetInt("type")
			fldLen := fcat.GetInt("length")
			offset := fcat.GetInt("offset")
			offsets[fldName] = offset
			sch.AddField(fldName, rm.FIELD_TYPE(fldType), fldLen)
		}
	}
	fcat.Close()
	return rm.NewLayout(sch, offsets, size)
}
