package filemanager

import "io"

type InputManagerInterface interface {
	io.Closer
	CanRead() bool
	ReadLine() string
	ReadLineAsBytes() []byte
	Err() error
}

type OutputManagerInterface interface {
	io.Closer
	WriteLine(line string) error
}
