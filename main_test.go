package diskq

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func tempDir() (string, func()) {
	dir := filepath.Join(os.TempDir(), UUIDv4().String())
	_ = os.Mkdir(dir, 0755)
	return dir, func() {
		_ = os.RemoveAll(dir)
	}
}

func assert_noerror(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("expected err to be unset; %v", err)
		t.FailNow()
	}
}

func assert_notnil(t *testing.T, a any) {
	t.Helper()
	if isNil(a) {
		t.Errorf("expected value to not be nil")
		t.FailNow()
	}
}

func assert_equal(t *testing.T, expected, actual any) {
	t.Helper()
	if !areEqual(expected, actual) {
		t.Errorf("equal expectation failed; expected value: %v, actual value: %v", expected, actual)
		t.FailNow()
	}
}

func assert_notequal(t *testing.T, expected, actual any) {
	t.Helper()
	if areEqual(expected, actual) {
		t.Errorf("not equal expectation failed; expected value: %v, actual value: %v", expected, actual)
		t.FailNow()
	}
}

func areEqual(expected, actual any) bool {
	if isNil(expected) && isNil(actual) {
		return true
	}
	if (isNil(expected) && !isNil(actual)) || (!isNil(expected) && isNil(actual)) {
		return false
	}
	actualType := reflect.TypeOf(actual)
	if actualType == nil {
		return false
	}
	expectedValue := reflect.ValueOf(expected)
	if expectedValue.IsValid() && expectedValue.Type().ConvertibleTo(actualType) {
		return reflect.DeepEqual(expectedValue.Convert(actualType).Interface(), actual)
	}
	return reflect.DeepEqual(expected, actual)
}

// isNil returns if a given reference is nil, but also returning true
// if the reference is a valid typed pointer to nil, which may not strictly
// be equal to nil.
func isNil(object any) bool {
	if object == nil {
		return true
	}

	value := reflect.ValueOf(object)
	kind := value.Kind()
	if kind >= reflect.Chan && kind <= reflect.Slice && value.IsNil() {
		return true
	}
	return false
}
