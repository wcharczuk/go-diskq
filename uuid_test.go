package diskq

import (
	"fmt"
	"testing"
)

func Test_UUID(t *testing.T) {
	u := UUIDv4()

	assert_notequal(t, zero, u)
	assert_equal(t, false, u.IsZero())
	assert_equal(t, true, zero.IsZero())
	assert_equal(t, u.String(), fmt.Sprint(u))
	assert_notequal(t, string(u[12:]), u.Short())
}
