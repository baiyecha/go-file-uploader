package nos

import (
	"fmt"
	"github.com/NetEase-Object-Storage/nos-golang-sdk/config"
	"github.com/NetEase-Object-Storage/nos-golang-sdk/nosclient"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	. "github.com/wq1019/go-file-uploader"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"testing"
	"time"
)

var (
	Endpoint         = "nos-eastchina1.126.net"
	ExternalEndpoint = "https://zm-cloud.nos-eastchina1.126.net"
	AccessKey        = "5fdde629014441f080b10d3f0299f85e"
	SecretKey        = "db67cb47b58d4bcb832684a17c97f29a"
	BucketName       = "zm-dev"
	uploader         Uploader
)

func TestMain(m *testing.M) {
	nosClient, err := nosclient.New(&config.Config{
		Endpoint:  Endpoint,
		AccessKey: AccessKey,
		SecretKey: SecretKey,
	})
	if err != nil {
		log.Fatalf("nos client 创建失败! error: %+v", err)
	}
	store := NewDBStore(setupGorm())

	uploader = NewNosUploader(
		HashFunc(MD5HashFunc),
		nosClient,
		store,
		BucketName,
		Hash2StorageNameFunc(TwoCharsPrefixHash2StorageNameFunc),
		Endpoint,
		ExternalEndpoint,
	)
	m.Run()
}

func TestNosUploader_Upload(t *testing.T) {
	filename := "./go1.11.linux-amd64.tar.gz"
	fi, err := os.Stat(filename)
	if err != nil {
		log.Fatalln(err)
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err)
	}
	fModel, err := uploader.Upload(FileHeader{Filename: file.Name(), Size: fi.Size(), File: file}, "")
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(fModel.Filename)
}

func TestNosUploader_UploadChunk(t *testing.T) {
	filename := "./b.apk"
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err)
	}
	fileHashValue, err := MD5HashFunc(file)
	if err != nil {
		log.Fatalln(err)
	}
	fileStat, err := os.Stat(filename)
	if err != nil {
		log.Fatalln(err)
	}
	// 跳转到文件的开头
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatalln(err)
	}
	var (
		totalSize   int64
		partNum     int
		chunkSize   = 50 << 20 // 50MB 每片
		isLastChunk bool
		uploadId    string
		tmpFileName = "tmp.tmp"
		chunkCount  = math.Ceil(float64(fileStat.Size()) / float64(chunkSize))
	)
	for {
		partNum++
		buffers := make([]byte, chunkSize)
		readLen, err := file.Read(buffers)
		if err != nil || readLen == 0 {
			break
		}
		tmpFile, err := writeTmpFile(tmpFileName, buffers[:readLen])
		totalSize += int64(readLen)
		// 如果当前读取的数据块大小加上以前的大小等于文件的大小 则表示是最后一次上传
		if totalSize == fileStat.Size() {
			isLastChunk = true
		}
		_, uploadId, err = uploader.UploadChunk(ChunkHeader{
			ChunkNumber:    partNum,
			UploadId:       uploadId,
			OriginFilename: file.Name(),
			OriginFileHash: fileHashValue,
			OriginFileSize: fileStat.Size(),
			IsLastChunk:    isLastChunk,
			ChunkContent:   tmpFile,
			ChunkCount:     int(chunkCount),
		}, "")
		if err != nil {
			log.Fatalln(err)
		}
		err = tmpFile.Close()
		if err != nil {
			log.Fatalln(err)
		}
		if isLastChunk {
			break
		}
	}
	removeFiles(tmpFileName)
}

func TestNosUploader_ReadChunk(t *testing.T) {
	filename := "./b.apk"
	inputFile, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err)
	}
	hashValue, err := MD5HashFunc(inputFile)
	if err != nil {
		log.Fatalln(err)
	}
	fileStat, err := os.Stat(filename)
	if err != nil {
		log.Fatalln(err)
	}
	outFileName := "out" + path.Ext(filename)
	removeFiles(outFileName)
	outputFile, err := os.OpenFile(outFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalln(err)
	}
	// 跳转到文件的开头
	_, err = inputFile.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatalln(err)
	}
	var (
		start    int64 = 0
		end      int64 = 0
		fileSize int64 = fileStat.Size()
		maxLimit int64 = 50 << 20 // 每次下载50MB
	)
	if fileSize < maxLimit {
		end = fileSize
	} else {
		end = maxLimit
	}
	for {
		rangeValue := fmt.Sprintf("bytes=%d-%d", start, end)
		fmt.Printf("\nfileSize: %d, rangeValue: %s\n", fileSize, rangeValue)
		readFile, err := uploader.ReadChunk(hashValue, rangeValue)
		if err != nil {
			log.Fatalln(err)
		}
		bytes, err := ioutil.ReadAll(readFile)
		if err != nil {
			log.Fatalln(err)
		}
		wn, err := outputFile.Write(bytes)
		if err != nil {
			log.Fatalln(err)
		}
		if wn <= 0 {
			_ = fmt.Errorf("写入失败")
		}
		if end+1 <= fileSize {
			start = end + 1
		} else {
			start = end
		}
		if end+maxLimit <= fileSize {
			end += maxLimit
		} else {
			end = fileSize
		}
		if start == end {
			break
		}
	}
}

func TestNosUploader_PresignedGetObject(t *testing.T) {
	url, err := uploader.PresignedGetObject("c40bc37c94a906e506f90e61374b46ef", time.Duration(10), nil)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(url.String())
}

func TestNosUploader_ReadFile(t *testing.T) {
	fileSize := 2357
	rf, err := uploader.ReadFile("c40bc37c94a906e506f90e61374b46ef")
	if err != nil {
		log.Fatalln(err)
	}
	bytes1 := make([]byte, fileSize)
	_, err = rf.Read(bytes1)
	if err != nil {
		log.Fatalln(err)
	}
	f, err := os.OpenFile("./tmp.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalln(err)
	}
	_, err = f.Write(bytes1)
	if err != nil {
		log.Fatalln(err)
	}
}

func TestReadFile_ReadAt(t *testing.T) {
	fileSize := 2357
	rf, err := uploader.ReadFile("c40bc37c94a906e506f90e61374b46ef")
	if err != nil {
		log.Fatalln(err)
	}
	bytes2 := make([]byte, fileSize)
	_, err = rf.ReadAt(bytes2, 0)
	if err != nil {
		log.Fatalln(err)
	}
	f, err := os.OpenFile("./tmp1.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalln(err)
	}
	_, err = f.Write(bytes2)
	if err != nil {
		log.Fatalln(err)
	}
}

func setupGorm() *gorm.DB {
	var (
		db  *gorm.DB
		err error
	)
	for i := 0; i < 10; i++ {
		db, err = gorm.Open("sqlite3", "file::memory:?cache=shared")
		if err == nil {
			autoMigrate(db)
			return db
		}
		log.Println(err)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("数据库链接失败！ error: %+v", err)
	return nil
}

func autoMigrate(db *gorm.DB) {
	err := db.AutoMigrate(
		&FileModel{},
	).Error
	if err != nil {
		log.Fatalf("AutoMigrate 失败！ error: %+v", err)
	}
}

func writeTmpFile(filename string, bytes []byte) (file *os.File, err error) {
	// 如果文件存在则删除他
	file, err = os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	n, err := file.Write(bytes)
	if n <= 0 {
		return nil, fmt.Errorf("文件写入失败")
	}
	return
}

func removeFiles(filenames ...string) {
	for _, filename := range filenames {
		_ = os.Remove(filename)
	}
}
