package file

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"strings"
	"unicode/utf16"
)

const (
	int32bites = 4
	utf16size  = 2
)

type BlockID struct {
	fileName string
	blkNum   int64
}

func NewBlockID(fileName string, blkNum int64) *BlockID {
	return &BlockID{
		fileName: fileName,
		blkNum:   blkNum,
	}
}

type Page struct {
	buffer []byte
}

func NewPage(blockSize int64) *Page {
	return &Page{
		buffer: make([]byte, blockSize),
	}
}

func (p *Page) GetInt(offset int) int32 {
	return int32(binary.LittleEndian.Uint32(p.buffer[offset : offset+int32bites]))
}

func (p *Page) SetInt(offset int, value int32) {
	binary.LittleEndian.PutUint32(p.buffer[offset:offset+int32bites], uint32(value))
}

func (p *Page) GetBytes(offset int) []byte {
	// length is written at the top of sequence
	// the disired bites are allowcated followingly
	length := p.GetInt(offset)
	return p.buffer[offset+int32bites : offset+int32bites+int(length)]
}

func (p *Page) SetBytes(offset int, bytes []byte) {
	length := len(bytes)
	p.SetInt(offset, int32(length))
	copy(p.buffer[offset+int32bites:offset+int32bites+length], bytes)
}

func (p *Page) GetString(offset int) string {
	length := int(p.GetInt(offset)) / utf16size
	runes := make([]uint16, length)
	for i := range length {
		runes[i] = p.getUint16(offset + int32bites + i*utf16size)
	}
	return string(utf16.Decode(runes))
}

func (p *Page) SetString(offset int, value string) {
	runes := utf16.Encode([]rune(value))

	p.SetInt(offset, int32(len(runes)*utf16size))

	for i, rune := range runes {
		p.setUint16(offset+int32bites+utf16size*i, rune)
	}
}

func (p *Page) getUint16(offset int) uint16 {
	return binary.LittleEndian.Uint16(p.buffer[offset : offset+utf16size])
}

func (p *Page) setUint16(offset int, value uint16) {
	binary.LittleEndian.PutUint16(p.buffer[offset:offset+utf16size], value)
}

func MaxLength(length int) int {
	return int32bites + utf16size*length
}

// bridge the file and the page
type FileManager struct {
	dbDir     string
	blockSize int64
	files     map[string]*os.File
}

func NewFileManager(dbDir string, blockSize int) (*FileManager, error) {
	if _, err := os.Stat(dbDir); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("os.Stat: %w", err)
		}

		// only user have all permissions
		err := os.MkdirAll(dbDir, 0o700)
		if err != nil {
			return nil, fmt.Errorf("os.MkdirAll: %w", err)
		}
	}

	files, err := os.ReadDir(dbDir)
	if err != nil {
		return nil, fmt.Errorf("os.ReadDir: %w", err)
	}
	for _, file := range files {
		if !strings.HasPrefix(file.Name(), "temp") {
			continue
		}
		if err := os.Remove(file.Name()); err != nil {
			return nil, fmt.Errorf("os.Remove: %w", err)
		}
	}
	return &FileManager{
		dbDir:     dbDir,
		blockSize: int64(blockSize),
		files:     make(map[string]*os.File),
	}, nil
}

func (fm *FileManager) BlockSize() int64 {
	return fm.blockSize
}

func (fm *FileManager) openFile(fileName string) (*os.File, error) {
	if file, ok := fm.files[fileName]; ok {
		return file, nil
	}
	file, err := os.OpenFile((path.Join(fm.dbDir, fileName)), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}
	// files map stores hundle for file (not the data in file itself)
	fm.files[fileName] = file
	// return the hunfle for file (not the data in file itself)
	return file, nil
}

// Read the content of block and write it into pages
// (openFile is just searching for the hundle for the file)
func (fm *FileManager) Read(blockID BlockID, page *Page) error {
	// openFile just returns hundle(id for resouce)
	// (hunfle is like pointer though slightly different)
	// the file is not actually deployed into memory
	file, err := fm.openFile(blockID.fileName)
	if err != nil {
		return fmt.Errorf("fm.openFile: %w", err)
	}

	_, err = file.Seek(blockID.blkNum*fm.blockSize, 0)
	if err != nil {
		return fmt.Errorf("file.Seek: %w", err)
	}

	// the file is read until buffer gets ful
	_, err = file.Read(page.buffer)
	if err != nil {
		return fmt.Errorf("file.Read: %w", err)
	}
	return nil
}

func (fm *FileManager) Write(blockID BlockID, page *Page) error {
	file, err := fm.openFile(blockID.fileName)
	if err != nil {
		return fmt.Errorf("fm.openFile: %w", err)
	}

	_, err = file.Seek(blockID.blkNum*fm.blockSize, 0)
	if err != nil {
		return fmt.Errorf("file.Seek: %w", err)
	}

	_, err = file.Write(page.buffer)
	if err != nil {
		return fmt.Errorf("file.Read: %w", err)
	}
	return nil
}
