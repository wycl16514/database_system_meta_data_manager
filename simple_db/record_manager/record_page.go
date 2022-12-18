package record_manager

import (
	fm "file_manager"
	"fmt"
	"tx"
)

type SLOT_FLAG int

const (
	EMPTY SLOT_FLAG = iota
	USED
)

type RecordPage struct {
	tx     *tx.Transation
	blk    *fm.BlockId
	layout LayoutInterface
}

func NewRecordPage(tx *tx.Transation, blk *fm.BlockId, layout LayoutInterface) *RecordPage {
	rp := &RecordPage{
		tx:     tx,
		blk:    blk,
		layout: layout,
	}
	tx.Pin(blk)

	return rp
}

func (r *RecordPage) offset(slot int) uint64 {
	return uint64(slot * r.layout.SlotSize())
}

func (r *RecordPage) GetInt(slot int, field_name string) int {
	field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
	val, err := r.tx.GetInt(r.blk, field_pos)
	if err == nil {
		return int(val)
	}

	return -1
}

func (r *RecordPage) GetString(slot int, field_name string) string {
	field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
	val, _ := r.tx.GetString(r.blk, field_pos)
	return val
}

func (r *RecordPage) SetInt(slot int, field_name string, val int) {
	field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
	r.tx.SetInt(r.blk, field_pos, int64(val), true)
}

func (r *RecordPage) SetString(slot int, field_name string, val string) {
	field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
	r.tx.SetString(r.blk, field_pos, val, true)
}

func (r *RecordPage) Delete(slot int) {
	r.setFlag(slot, EMPTY)
}

func (r *RecordPage) Format() {
	slot := 0
	for r.isValidSlot(slot) {
		r.tx.SetInt(r.blk, r.offset(slot), int64(EMPTY), false)
		sch := r.layout.Schema()
		for _, field_name := range sch.Fields() {
			field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
			if sch.Type(field_name) == INTEGER {
				r.tx.SetInt(r.blk, field_pos, 0, false)
			} else {
				r.tx.SetString(r.blk, field_pos, "", false)
			}
			slot += 1
		}
	}
}

func (r *RecordPage) NextAfter(slot int) int {
	return r.searchAfter(slot, USED)
}

func (r *RecordPage) InsertAfter(slot int) int {
	new_slot := r.searchAfter(slot, EMPTY)
	if new_slot >= 0 {
		r.setFlag(new_slot, USED)
	}

	return new_slot
}

func (r *RecordPage) Block() *fm.BlockId {
	return r.blk
}

func (r *RecordPage) setFlag(slot int, flag SLOT_FLAG) {
	r.tx.SetInt(r.blk, r.offset(slot), int64(flag), true)
}

func (r *RecordPage) searchAfter(slot int, flag SLOT_FLAG) int {
	slot += 1
	for r.isValidSlot(slot) {
		val, err := r.tx.GetInt(r.blk, r.offset(slot))
		if err != nil {
			fmt.Printf("SearchAfter has err %v\n", err)
			return -1
		}

		if SLOT_FLAG(val) == flag {
			return slot
		}
		slot += 1
	}

	return -1
}

func (r *RecordPage) isValidSlot(slot int) bool {
	return r.offset(slot+1) <= r.tx.BlockSize()
}
