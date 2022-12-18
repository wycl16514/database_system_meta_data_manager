package metadata_manager

import (
	bmg "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	mm "metadata_management"
	record_mgr "record_manager"
	"tx"
)

func TestTableManager() {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
	buffer_manager := bmg.NewBufferManager(file_manager, log_manager, 3)

	tx := tx.NewTransation(file_manager, log_manager, buffer_manager)
	sch := record_mgr.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)

	tm := mm.NewTableManager(true, tx)
	tm.CreateTable("MyTable", sch, tx)
	layout := tm.GetLayout("MyTable", tx)
	size := layout.SlotSize()
	sch2 := layout.Schema()
	fmt.Printf("MyTable has slot size: %d\n", size)
	fmt.Println("Its fields are: ")
	for _, fldName := range sch2.Fields() {
		fldType := ""
		if sch2.Type(fldName) == record_mgr.INTEGER {
			fldType = "int"
		} else {
			strlen := sch2.Length(fldName)
			fldType = fmt.Sprintf("varchar( %d )", strlen)
		}
		fmt.Printf("%s : %s\n", fldName, fldType)
	}

	tx.Commit()

}
