package page

import (
	"goostub/common"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestLSN(t *testing.T) {
	page := NewPage()
	numTries := 10

	a := assert.New(t)
	for i := 0; i < numTries; i++ {
		lsn := common.LSN(rand.Int31())
		page.SetLSN(lsn)
		a.Equal(lsn, page.GetLSN())
		data := page.GetData()
		a.Equal(lsn, *(*common.LSN)(unsafe.Pointer(&data[offsetLSN])))
	}
}
