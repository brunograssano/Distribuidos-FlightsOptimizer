package getters

type GetterConfig struct {
	ID              string
	FileNames       []string
	Address         string
	MaxLinesPerSend uint
}

func NewGetterConfig(ID string, toReadFileNames []string, address string, linesPerSend uint) *GetterConfig {
	return &GetterConfig{
		ID:              ID,
		FileNames:       toReadFileNames,
		Address:         address,
		MaxLinesPerSend: linesPerSend,
	}
}
