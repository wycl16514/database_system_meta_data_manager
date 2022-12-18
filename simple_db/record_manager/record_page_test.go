package record_manager

import (
	bmg "buffer_manager"
	fm "file_manager"
	"fmt"
	"github.com/stretchr/testify/require"
	lm "log_manager"
	"math/rand"
	"testing"
	"tx"
)

func TestRecordPageInsertAndDelete(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
	buffer_manager := bmg.NewBufferManager(file_manager, log_manager, 3)

	tx := tx.NewTransation(file_manager, log_manager, buffer_manager)
	sch := NewSchema()

	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := NewLayoutWithSchema(sch)
	for _, field_name := range layout.Schema().Fields() {
		offset := layout.Offset(field_name)
		fmt.Printf("%s has offset %d\n", field_name, offset)
	}

	blk, err := tx.Append("testfile")
	require.Nil(t, err)

	tx.Pin(blk)
	rp := NewRecordPage(tx, blk, LayoutInterface(layout))
	rp.Format()
	fmt.Println("Filling the page with random records")
	slot := rp.InsertAfter(-1) //找到第一条可用插槽
	val_for_field_A := make([]int, 0)
	for slot >= 0 {
		n := rand.Intn(50)
		val_for_field_A = append(val_for_field_A, n)
		rp.SetInt(slot, "A", n)                          //找到可用插槽后随机设定字段A的值
		rp.SetString(slot, "B", fmt.Sprintf("rec%d", n)) //设定字段B
		fmt.Printf("inserting into slot :%d :{ %d , rec%d}\n", slot, n, n)
		slot = rp.InsertAfter(slot) //查找当前插槽之后可用的插槽
	}

	slot = rp.NextAfter(-1) //测试插入字段是否正确
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		require.Equal(t, a, val_for_field_A[slot])
		require.Equal(t, b, fmt.Sprintf("rec%d", a))
		slot = rp.NextAfter(slot)
	}

	fmt.Println("Deleted these records with A-values < 25.")
	count := 0
	slot = rp.NextAfter(-1)
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		if a < 25 {
			count += 1
			fmt.Printf("slot %d: {%d, %s}\n", slot, a, b)
			rp.Delete(slot)
		}
		slot = rp.NextAfter(slot)
	}
	fmt.Printf("%d values under 25 were deleted.\n", count)
	fmt.Println("Here are the remaining records")
	slot = rp.NextAfter(-1)
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")

		require.Equal(t, a >= 25, true)

		fmt.Printf("slot %d : {%d, %s}\n", slot, a, b)
		slot = rp.NextAfter(slot)
	}

	tx.UnPin(blk)
	tx.Commit()
}
