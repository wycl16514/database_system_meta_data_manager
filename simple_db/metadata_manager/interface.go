package metadata_manager

import (
	rm "record_manager"
	"tx"
)

type TableManagerInterface interface {
	CreateTable(tblName string, sch *rm.Schema, tx *tx.Transation)
	GetLayout(tblName string, tx *tx.Transation) *rm.Layout
}
