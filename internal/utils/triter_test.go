package utils_test

import (
	"testing"
	"github.com/AnchorFree/tsdb-remote-write/internal/utils"
)

func TestTimeRangeForwardIterator(t *testing.T) {
	iter := utils.NewTimeRangeIter(10, 100, 10, false)
	for iter.Next() {
		t.Logf("%#v", iter.At())
	}
}

func TestTimeRangeBackwardIterator(t *testing.T) {
	iter := utils.NewTimeRangeIter(10, 100, 10, true)
	for iter.Next() {
		t.Logf("%#v", iter.At())
	}
}
