package diskq

import "fmt"

// Config are the options for the diskq.
type Config struct {
	DataPath    string
	OffsetsPath string
}

// Validate validates the config, this will be called
// when you call `New(cfg)`.
func (c Config) Validate() error {
	if c.DataPath == "" {
		return fmt.Errorf("diskq config; validation; data path unset")
	}
	if c.OffsetsPath == "" {
		return fmt.Errorf("diskq config; validation; offsets path unset")
	}
	if c.DataPath == c.OffsetsPath {
		return fmt.Errorf("diskq config; validation; data path and offsets path must be different")
	}
	return nil
}
