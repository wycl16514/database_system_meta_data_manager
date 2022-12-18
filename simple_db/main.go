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
