package filemanager

import "io"

type InputManagerInterface interface {
	io.Closer
	CanRead() bool
	ReadLine() string
	Err() error
}

type OutputManagerInterface interface {
	io.Closer
	WriteLine(line string) error
}
