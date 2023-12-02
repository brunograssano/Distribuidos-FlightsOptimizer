package filemanager

// SkipHeader Reads a line to skip the header
func SkipHeader(reader *FileReader) {
	if reader.CanRead() {
		_ = reader.ReadLine()
	}
}
