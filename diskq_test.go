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

var testConfig = Config{
	DataPath:    "test.dq",
	OffsetsPath: "test.off.dq",
}

func Test_Diskq(t *testing.T) {
	dq, err := New[Measurement](testConfig)
	testutil.NoError(t, err)
	defer func() {
		_ = dq.Close()
		_ = os.Remove(testConfig.DataPath)
		_ = os.Remove(testConfig.OffsetsPath)
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

	iter, err := NewIter[Measurement](testConfig)
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

	m, ok, err = GetOffset[Measurement](testConfig, 1)
	testutil.NoError(t, err)
	testutil.Equal(t, true, ok)
	testutil.Equal(t, "second", m.Name)
	testutil.Equal(t, 2, m.Temp)
	testutil.Equal(t, 3, m.Humid)
	testutil.Equal(t, 4, m.CO2)
	testutil.Equal(t, 5, m.PM25)
}

func Test_Diskq_resume(t *testing.T) {
	testConfig2 := Config{
		DataPath:    "test2.dq",
		OffsetsPath: "test2.off.dq",
	}
	dq, err := New[Measurement](testConfig2)
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

	dq2, err := New[Measurement](testConfig2)
	testutil.NoError(t, err)
	defer dq2.Close()

	testutil.Equal(t, 2, dq2.offset)

	err = dq2.Push(Measurement{
		Name:  "fourth",
		Temp:  4,
		Humid: 5,
		CO2:   6,
		PM25:  7,
	})
	testutil.NoError(t, err)
	testutil.Equal(t, 3, dq2.offset)
}
