package fqueue

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
)

type Interface interface {
	Put([]byte) error
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

type FileQueue struct {
	readFile, writeFile       *os.File
	readOffset                int
	writeOffset               int
	nextReadOffset            int
	readFileNum, writeFileNum int
	readChan                  chan []byte // expose for  caller to read from this channel
	writeChan                 chan []byte // caller try to push msg encoded to this channel
	errChan                   chan error  // used to push error for write caller
	closeChan                 chan struct{}
	deleteChan                chan struct{}
	emptyChan                 chan struct{}
	len                       int // when len == 0, queue is empty, not ready to read
	maxBytesPerFile           int
	dataPath                  string
	qname                     string
	readFileSize              int64
	syncEvery                 int64
	synced                    bool
	closeFlag                 int32
}

type FileQueueConfig struct {
	MaxBytesPerFile int
	SyncEveryStep   int
}

func NewFileQueue(dir, qname string, cfg FileQueueConfig) *FileQueue {
	q := &FileQueue{
		dataPath:        dir,
		qname:           qname,
		readChan:        make(chan []byte),
		writeChan:       make(chan []byte),
		closeChan:       make(chan struct{}),
		deleteChan:      make(chan struct{}),
		errChan:         make(chan error),
		maxBytesPerFile: cfg.MaxBytesPerFile,
		syncEvery:       int64(cfg.SyncEveryStep),
		synced:          true,
	}
	q.inititate()
	go q.processLoop()
	return q
}

func (q *FileQueue) Put(buf []byte) error {
	select {
	case q.writeChan <- buf:
		err := <-q.errChan
		return err
	case <-q.closeChan:
		return errors.New("queue is closed")
	}
}

func (q *FileQueue) ReadChan() <-chan []byte {
	return q.readChan
}

func (q *FileQueue) Close() error {
	if atomic.CompareAndSwapInt32(&q.closeFlag, 0, 1) {
		close(q.closeChan)
		return <-q.errChan
	}
	return errors.New("queue is exiting")
}

func (q *FileQueue) Delete() error {
	if atomic.CompareAndSwapInt32(&q.closeFlag, 0, 1) {
		close(q.deleteChan)
		return <-q.errChan
	}

	return errors.New("queue is exiting")
}

func (q *FileQueue) Depth() int64 {
	return int64(q.len)
}

func (q *FileQueue) Empty() error {
	select {
	case q.emptyChan <- struct{}{}:
		err := <-q.errChan
		return err
	case <-q.closeChan:
		return errors.New("queue is closed")
	}
}

// always ensure that ReadFileNum <= WriteFileNum, if equal, ReadOffset <= WriteOffset
type FileQueueMetadata struct {
	ReadFileNum  int `json:"readfile_no"`
	ReadOffset   int `json:"read_offset"`
	WriteFileNum int `json:"writefile_no"`
	WriteOffset  int `json:"write_offset"`
	Len          int `json:"len"`
}

// 1. bypass error handling first,
// should handle it later la ok dung

func (q *FileQueue) readOne() ([]byte, error) {
	if q.len == 0 {
		return nil, nil
	}
	// there are 2 cases :
	// 1. if readFile != writeFile
	err := q.mayOpenReadFile()
	if err != nil {
		return nil, err
	}

	q.mayReadNextFile()
	q.mayOpenReadFile()

	var szbuf [4]byte
	//fmt.Printf("read filenum: %d, readoffset: %d\n", q.readFileNum, q.readOffset)
	_, err = q.readFile.ReadAt(szbuf[:], int64(q.readOffset))
	if err != nil {
		log.Println("Failed to read file, size,", err.Error())
		return nil, err
	}
	var sz uint32 = binary.LittleEndian.Uint32(szbuf[:])
	var msg []byte = make([]byte, sz)
	_, err = q.readFile.ReadAt(msg, int64(q.readOffset)+4)
	//fmt.Println("read msg:", string(msg))

	if err != nil {
		log.Println("Failed to read file, msg", err)
		return nil, err
	}

	q.nextReadOffset = q.readOffset + 4 + len(msg)
	q.synced = false
	return msg, nil
}

func (q *FileQueue) mayOpenReadFile() error {
	if q.readFile != nil {
		return nil
	}
	readFileName := q.getFileName(q.readFileNum)
	rf, err := os.OpenFile(readFileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Println("Failed to open read file:", err)
		return err
	}

	q.readFile = rf

	if q.readFileNum != q.writeFileNum {
		fileinfo, err := rf.Stat()
		if err != nil {
			log.Println("Failed to read file stat", err)
			return err
		}
		q.readFileSize = fileinfo.Size()
	} else {
		q.readFileSize = 0
	}
	return nil
}

// always ensure that readfile!=nil when call this function
func (q *FileQueue) mayReadNextFile() {
	if q.readFileSize > 0 {
		/* when q.readFileSize > 0, it indicates that at the time opening read file,
		the writefile!=readFile, and readfile size is fixed
		*/
		if q.readOffset < int(q.readFileSize) {
			return
		}
		q.readFile.Close()
		q.readFileNum++
		q.readOffset = 0
		q.readFile = nil
		return
	}

	if q.readFileNum == q.writeFileNum {
		return
	}

	fileinfo, err := q.readFile.Stat()
	if err != nil {
		return
	}

	if q.readOffset >= int(fileinfo.Size()) {
		q.readFile.Close()
		q.readFileNum++
		q.readOffset = 0
		q.readFile = nil
	}
}

func (q *FileQueue) advanceRead() {
	q.readOffset = q.nextReadOffset
	q.len--
	q.synced = false
}

func (q *FileQueue) mayCutOffWriteFile(nextBytes int) {
	if q.writeOffset+nextBytes >= q.maxBytesPerFile {
		if q.writeFile != nil {
			q.writeFile.Close()
			q.writeFile = nil
		}
		q.writeFileNum++
		q.writeOffset = 0
	}
}

func (q *FileQueue) getFileName(filenum int) string {
	return fmt.Sprintf("%s/%s_%d", q.dataPath, q.qname, filenum)
}

func (q *FileQueue) writeOne(msg []byte) error {
	if len(msg) > q.maxBytesPerFile {
		return errors.New("message is too large")
	}

	n := 4 + len(msg)
	q.mayCutOffWriteFile(n)

	if q.writeFile == nil {
		writeFileName := q.getFileName(q.writeFileNum)
		wf, err := os.OpenFile(writeFileName, os.O_CREATE|os.O_WRONLY, 0644)

		if err != nil {
			log.Println("Failed to open file:", err)
			return err
		}

		q.writeFile = wf

		_, err = wf.Seek(int64(q.writeOffset), 0)
		if err != nil {
			// when error occur, it may be unrecoverable ???
			log.Println("Failed to Seek:", err)
			return err
		}
		// TODO: add other cases to recover from error later
	}
	var buf []byte = make([]byte, n)
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(msg)))
	copy(buf[4:], msg)
	_, err := q.writeFile.Write(buf)

	if err != nil {
		log.Println("Failed to Write Message to file: ", err)
		// TODO: may recover from error
		return err
	}

	//fmt.Printf("Write %d bytes to %s\n", n, q.getFileName(q.writeFileNum))
	q.writeOffset += n
	q.len++
	q.synced = false
	return nil
}

func (q *FileQueue) processLoop() {
	var readData []byte = nil
	var readChan chan []byte = nil
	var iter int64
	for {
		// should prepare data for each iteration
		iter++
		if readData == nil {
			readData, _ = q.readOne()

			if readData != nil {
				readChan = q.readChan
			} else {
				readChan = nil
			}
		}

		if iter%q.syncEvery == 0 {
			q.sync()
		}

		select {
		case <-q.closeChan:
			err := q.close()
			q.errChan <- err
			goto exit
		case <-q.deleteChan:
			err := q.empty()
			q.errChan <- err
			goto exit
		case <-q.emptyChan:
			q.empty()

		case readChan <- readData:
			q.advanceRead()
			readData = nil
		case msg := <-q.writeChan:
			err := q.writeOne(msg)
			q.errChan <- err

		}
	}
exit:
	log.Printf("[%s] FileQueue is  closed completelly!!!", q.qname)
}

// /data/

func (q *FileQueue) persistMetadata() error {
	f, err := os.CreateTemp(q.dataPath, fmt.Sprintf("%s_meta*", q.qname))

	if err != nil {
		log.Println("Failed to create metadata temporary:", err)
		return err
	}

	var meta = FileQueueMetadata{
		ReadFileNum:  q.readFileNum,
		ReadOffset:   q.readOffset,
		WriteFileNum: q.writeFileNum,
		WriteOffset:  q.writeOffset,
		Len:          q.len,
	}
	buf, _ := json.Marshal(&meta)
	_, err = f.Write(buf)
	if err != nil {
		log.Println("Failed to Write FileQueue Metadata", err)
		return err
	}
	return os.Rename(f.Name(), q.getMetadataFilename())
}

func (q *FileQueue) getMetadataFilename() string {
	return fmt.Sprintf("%s/%s_meta", q.dataPath, q.qname)
}

func (q *FileQueue) sync() error {
	if q.synced {
		return nil
	}
	if q.writeFile == nil {
		fmt.Println("Not need to sync write file, because it's not open")
	} else {
		err := q.writeFile.Sync()
		if err != nil {
			log.Println("Failed to sync Writefile, caused by:", err)
			q.writeFile.Close()
			return err
		}
	}
	err := q.persistMetadata()
	q.synced = true
	return err
}

func (q *FileQueue) resetMetadata() {
	if q.readFile != nil {
		q.readFile.Close()
	}
	q.readFileNum = 0
	q.readOffset = 0
	if q.writeFile != nil {
		q.writeFile.Close()
	}
	q.writeFileNum = 0
	q.writeOffset = 0
	q.len = 0
}

func (q *FileQueue) empty() error {
	entries, err := os.ReadDir(q.dataPath)
	if err != nil {
		log.Println("Failed to read dir", err)
		return err
	}
	q.resetMetadata()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), q.qname) {
			if err := os.Remove(q.dataPath + "/" + entry.Name()); err != nil {
				log.Println("Failed to remove file, ", err)
			}
		}
	}

	return nil
}

func (q *FileQueue) close() error {
	err := q.sync()
	return err
}

func (q *FileQueue) inititate() {
	buf, err := os.ReadFile(q.getMetadataFilename())
	if err != nil {
		log.Println("Failed to initiate File Queue Metadata", err)
		return
	}
	var metadata FileQueueMetadata
	json.Unmarshal(buf, &metadata)

	q.readFileNum = metadata.ReadFileNum
	q.readOffset = metadata.ReadOffset
	q.writeFileNum = metadata.WriteFileNum
	q.writeOffset = metadata.WriteOffset
	q.len = metadata.Len
}

// for testing
func (q *FileQueue) GetMetadata() FileQueueMetadata {
	return FileQueueMetadata{
		ReadFileNum:  q.readFileNum,
		ReadOffset:   q.readOffset,
		WriteFileNum: q.writeFileNum,
		WriteOffset:  q.writeOffset,
		Len:          q.len,
	}
}
