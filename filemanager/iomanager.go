package filemanager

type IOManagerInterface interface {
	Close() error
}

type InputManagerInterface interface {
	IOManagerInterface
	CanRead() (bool, error)
	ReadLine() string
	Err() error
}

type OutputManagerInterface interface {
	IOManagerInterface
	WriteLine(line string) error
}
