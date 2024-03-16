module github.com/wcharczuk/go-diskq/cmd/diskq

go 1.21.3

replace github.com/wcharczuk/go-diskq => ../..

require (
	github.com/urfave/cli/v3 v3.0.0-alpha9
	github.com/wcharczuk/go-diskq v0.0.0-00010101000000-000000000000
)

require (
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	golang.org/x/sys v0.4.0 // indirect
)
