package file_test

import (
	"path"
	"simpledb/file"
	"simpledb/server"
	"testing"
)

func TestFile(t *testing.T) {
	t.Parallel()

	testPath := path.Join(t.TempDir(), "filetest")
	blockSize := 400
	db, err := server.NewSimpleDB(testPath, blockSize)
	if err != nil {
		t.Fatalf("NewSimpleDB: %v", err)
	}
	fm := db.FileManager()

	p1 := file.NewPage(fm.BlockSize())
	pos1 := 88
	strVal := "qwerty"
	p1.SetString(pos1, strVal)

	// it is page user's responsibility to avoid crush
	size := file.MaxLength(len(strVal))
	pos2 := pos1 + size
	intVal := int32(606)
	p1.SetInt(pos2, intVal)

	blk := file.NewBlockID("testfile", 2)
	err = fm.Write(*blk, p1)
	if err != nil {
		t.Fatalf("fm.Write: %v", err)
	}

	p2 := file.NewPage(fm.BlockSize())
	err = fm.Read(*blk, p2)
	if err != nil {
		t.Fatalf("fm.Read: %v", err)
	}

	if p2.GetInt(pos2) != intVal {
		t.Errorf("ecpected %d, got %d", intVal, p2.GetInt(pos2))
	}
	if p2.GetString(pos1) != strVal {
		t.Errorf("ecpected %q, got %q", strVal, p2.GetString(pos1))
	}
}
