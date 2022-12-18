package record_manager

import (
	fm "file_manager"
	"tx"
)

type TableScan struct {
	tx           *tx.Transation
	layout       LayoutInterface
	rp           RecordManagerInterface
	file_name    string
	current_slot int
}

func NewTableScan(tx *tx.Transation, table_name string, layout LayoutInterface) *TableScan {
	table_scan := &TableScan{
		tx:        tx,
		layout:    layout,
		file_name: table_name + ".tbl",
	}

	size, err := tx.Size(table_scan.file_name)
	if err != nil {
		panic(err)
	}
	if size == 0 {
		//如果文件为空，那么增加一个区块
		table_scan.MoveToNewBlock()
	} else {
		//先读取第一个区块
		table_scan.MoveToBlock(0)
	}

	return table_scan
}

func (t *TableScan) Close() {
	if t.rp != nil {
		t.tx.UnPin(t.rp.Block())
	}
}

func (t *TableScan) BeforeFirst() {
	t.MoveToBlock(0)
}

func (t *TableScan) Next() bool {
	/*
		如果在当前区块找不到给定有效记录则遍历后续区块，直到所有区块都遍历为止
	*/
	t.current_slot = t.rp.NextAfter(t.current_slot)
	for t.current_slot < 0 {
		if t.AtLastBlock() {
			//直到最后一个区块都找不到给定插槽
			return false
		}

		t.MoveToBlock(int(t.rp.Block().Number() + 1))
		t.current_slot = t.rp.NextAfter(t.current_slot)
	}

	return true
}

func (t *TableScan) GetInt(field_name string) int {
	return t.rp.GetInt(t.current_slot, field_name)
}

func (t *TableScan) GetString(field_name string) string {
	return t.rp.GetString(t.current_slot, field_name)
}

func (t *TableScan) GetVal(field_name string) *Constant {
	if t.layout.Schema().Type(field_name) == INTEGER {
		return NewConstantInt(t.GetInt(field_name))
	}

	return NewConstantString(t.GetString(field_name))
}

func (t *TableScan) HasField(field_name string) bool {
	return t.layout.Schema().HasFields(field_name)
}

func (t *TableScan) SetInt(field_name string, val int) {
	t.rp.SetInt(t.current_slot, field_name, val)
}

func (t *TableScan) SetString(field_name string, val string) {
	t.rp.SetString(t.current_slot, field_name, val)
}

func (t *TableScan) SetVal(field_name string, val *Constant) {
	if t.layout.Schema().Type(field_name) == INTEGER {
		t.SetInt(field_name, val.IVal)
	} else {
		t.SetString(field_name, val.SVal)
	}
}

func (t *TableScan) Insert() {
	/*
		将当前插槽号指向下一个可用插槽
	*/
	t.current_slot = t.rp.InsertAfter(t.current_slot)
	for t.current_slot < 0 { //当前区块找不到可用插槽
		if t.AtLastBlock() {
			//如果当前处于最后一个区块，那么新增一个区块
			t.MoveToNewBlock()
		} else {
			t.MoveToBlock(int(t.rp.Block().Number() + 1))
		}

		t.current_slot = t.rp.InsertAfter(t.current_slot)
	}
}

func (t *TableScan) Delete() {
	t.rp.Delete(t.current_slot)
}

func (t *TableScan) MoveToRid(r RIDInterface) {
	t.Close()
	blk := fm.NewBlockId(t.file_name, uint64(r.BlockNumber()))
	t.rp = NewRecordPage(t.tx, blk, t.layout)
	t.current_slot = r.Slot()
}

func (t *TableScan) GetRid() RIDInterface {
	return NewRID(int(t.rp.Block().Number()), t.current_slot)
}

func (t *TableScan) MoveToBlock(blk_num int) {
	t.Close()
	blk := fm.NewBlockId(t.file_name, uint64(blk_num))
	t.rp = NewRecordPage(t.tx, blk, t.layout)
	t.current_slot = -1
}

func (t *TableScan) MoveToNewBlock() {
	t.Close()
	blk, err := t.tx.Append(t.file_name)
	if err != nil {
		panic(err)
	}
	t.rp = NewRecordPage(t.tx, blk, t.layout)
	t.rp.Format()
	t.current_slot = -1
}

func (t *TableScan) AtLastBlock() bool {
	size, err := t.tx.Size(t.file_name)
	if err != nil {
		panic(err)
	}
	return t.rp.Block().Number() == size-1
}
