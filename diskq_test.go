package diskq

import (
	"os"
	"testing"

	"github.com/wcharczuk/go-incr/testutil"
)

type Measurement struct {
	Name  string
	Temp  float64
	Humid float64
	CO2   float64
	PM25  float64
}

func Test_Diskq(t *testing.T) {
	dq, err := New[Measurement]("test.dq", "test.off.dq")
	testutil.NoError(t, err)
	defer func() {
		_ = dq.Close()
		_ = os.Remove("test.dq")
		_ = os.Remove("test.off.dq")
	}()

	err = dq.Push(Measurement{
		Name:  "first",
		Temp:  1,
		Humid: 2,
		CO2:   3,
		PM25:  4,
	})
	testutil.NoError(t, err)

	err = dq.Push(Measurement{
		Name:  "second",
		Temp:  2,
		Humid: 3,
		CO2:   4,
		PM25:  5,
	})
	testutil.NoError(t, err)

	err = dq.Push(Measurement{
		Name:  "third",
		Temp:  3,
		Humid: 4,
		CO2:   5,
		PM25:  6,
	})
	testutil.NoError(t, err)

	iter, err := NewIter[Measurement]("test.dq", "test.off.dq")
	testutil.NoError(t, err)
	defer iter.Close()

	var m Measurement
	var ok bool
	m, ok, err = iter.Next()
	testutil.NoError(t, err)

	var index int
	expectedNames := [...]string{"first", "second", "third"}
	for ok {
		testutil.Equal(t, expectedNames[index], m.Name)
		index++
		m, ok, err = iter.Next()
		testutil.NoError(t, err)
	}

	m, ok, err = GetOffset[Measurement]("test.dq", "test.off.dq", 1)
	testutil.NoError(t, err)
	testutil.Equal(t, true, ok)
	testutil.Equal(t, "second", m.Name)
	testutil.Equal(t, 2, m.Temp)
	testutil.Equal(t, 3, m.Humid)
	testutil.Equal(t, 4, m.CO2)
	testutil.Equal(t, 5, m.PM25)
}

func Test_Diskq_resume(t *testing.T) {
	dq, err := New[Measurement]("test2.dq", "test2.off.dq")
	testutil.NoError(t, err)
	defer func() {
		_ = os.Remove("test2.dq")
		_ = os.Remove("test2.off.dq")
	}()

	err = dq.Push(Measurement{
		Name:  "first",
		Temp:  1,
		Humid: 2,
		CO2:   3,
		PM25:  4,
	})
	testutil.NoError(t, err)

	err = dq.Push(Measurement{
		Name:  "second",
		Temp:  2,
		Humid: 3,
		CO2:   4,
		PM25:  5,
	})
	testutil.NoError(t, err)

	err = dq.Push(Measurement{
		Name:  "third",
		Temp:  3,
		Humid: 4,
		CO2:   5,
		PM25:  6,
	})
	testutil.NoError(t, err)
	_ = dq.Close()

	dq2, err := New[Measurement]("test2.dq", "test2.off.dq")
	testutil.NoError(t, err)
	defer dq2.Close()

	testutil.Equal(t, 2, dq2.offset)
}
