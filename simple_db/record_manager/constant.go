package record_manager

import (
	"strconv"
)

type Constant struct {
	IVal  int
	SVal  string
	isInt bool
}

func NewConstantInt(val int) *Constant {
	return &Constant{
		IVal:  val,
		isInt: true,
	}
}

func NewConstantString(val string) *Constant {
	return &Constant{
		SVal:  val,
		isInt: false,
	}
}

func (c *Constant) AsInt() int {
	return c.IVal
}

func (c *Constant) AsString() string {
	return c.SVal
}

func (c *Constant) Equals(other *Constant) bool {
	if c.isInt {
		return c.IVal == other.IVal
	}

	return c.SVal == other.SVal
}

func (c *Constant) CompareTo(other *Constant) int {
	if c.isInt {
		return c.IVal - other.IVal
	}

	if c.SVal > other.SVal {
		return 1
	} else if c.SVal == other.SVal {
		return 0
	} else {
		return -1
	}
}

func (c *Constant) ToString() string {
	if c.isInt {
		return strconv.Itoa(c.IVal)
	}

	return c.SVal
}
