package record_manager

import (
	"github.com/stretchr/testify/require"
	"testing"
	"tx"
)

func TestLayoutOffset(t *testing.T) {
	sch := NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	sch.AddIntField("C")
	layout := NewLayoutWithSchema(sch)
	fields := sch.Fields()
	/*
			字段A前面用一个int做占用标志位，因此字段A的偏移是8，
		    字段A的类型是int,在go中该类型长度为8，因此字段B的偏移就是16
		    字段B是字符串类型，它的偏移是9，它自身长度为9，同时存入page时会
		    先存入8字节的无符号整形用来记录字符串的长度，因此字段C的偏移是16+8+9=33
	*/
	offsetA := layout.Offset(fields[0])
	require.Equal(t, tx.UINT64_LENGTH, offsetA)

	offsetB := layout.Offset(fields[1])
	require.Equal(t, 16, offsetB)

	offsetC := layout.Offset(fields[2])
	require.Equal(t, 33, offsetC)
}
