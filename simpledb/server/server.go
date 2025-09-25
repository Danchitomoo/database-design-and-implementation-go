package server

import (
	"fmt"
	"simpledb/file"
)

type SimpleDB struct {
	fileManager *file.FileManager
}

func NewSimpleDB(dbDir string, blockSize int) (*SimpleDB, error) {
	fileManager, err := file.NewFileManager(dbDir, blockSize)
	if err != nil {
		return nil, fmt.Errorf("file.FileManager: %w", err)
	}

	return &SimpleDB{
		fileManager: fileManager,
	}, nil
}

func (db *SimpleDB) FileManager() *file.FileManager {
	return db.fileManager
}
