package nos

import (
	"crypto/md5"
	"fmt"
	"github.com/NetEase-Object-Storage/nos-golang-sdk/config"
	"github.com/NetEase-Object-Storage/nos-golang-sdk/nosclient"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	. "github.com/wq1019/go-file-uploader"
	"io"
	"log"
	"os"
	"testing"
	"time"
)

var uploader Uploader

func TestMain(m *testing.M) {
	nosClient, err := nosclient.New(&config.Config{
		Endpoint:  "nos-eastchina1.126.net",
		AccessKey: "5fdde629014441f080b10d3f0299f85e",
		SecretKey: "db67cb47b58d4bcb832684a17c97f29a",
	})
	if err != nil {
		log.Fatalf("nos client 创建失败! error: %+v", err)
	}
	store := NewDBStore(setupGorm())

	uploader = NewNosUploader(
		HashFunc(MD5HashFunc),
		nosClient,
		store,
		"zm-dev",
		Hash2StorageNameFunc(TwoCharsPrefixHash2StorageNameFunc),
		"nos-eastchina1.126.net",
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
	filename := "./uploader.go"
	fi, err := os.Stat(filename)
	if err != nil {
		log.Fatalln(err)
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err)
	}
	h := md5.New()
	_, err = io.Copy(h, file)
	if err != nil {
		_ = fmt.Errorf("%+v", err)
		return
	}

	fmt.Println(fmt.Sprintf("%x\n", h.Sum(nil)))

	_, err = uploader.UploadChunk(FileHeader{Filename: file.Name(), Size: fi.Size(), File: file}, "")
	if err != nil {
		log.Fatalln(err)
	}
}

func TestNosUploader_PresignedGetObject(t *testing.T) {
	url, err := uploader.PresignedGetObject("c40bc37c94a906e506f90e61374b46ef", time.Duration(10), nil)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(url.String())
	fmt.Println(100 << 20)
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
		db, err = gorm.Open("sqlite3", "cloud.db")
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
