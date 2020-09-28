package minio

import (
	"log"
	"os"
	"testing"
	"time"

	. "github.com/baiyecha/go-file-uploader"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/minio/minio-go"
)

var uploader Uploader

func TestMain(m *testing.M) {
	minioClient, err := minio.New(
		"59.111.58.150:9000",
		"zm2018",
		"zhiming2018",
		false,
	)

	if err != nil {
		log.Fatalf("minio client 创建失败! error: %+v", err)
	}
	store := NewDBStore(setupGorm())

	uploader = NewMinioUploader(
		HashFunc(MD5HashFunc),
		minioClient,
		store,
		"test",
		Hash2StorageNameFunc(TwoCharsPrefixHash2StorageNameFunc),
	)
	m.Run()
}

func TestMinioUploader_Upload(t *testing.T) {
	filename := "./uploader.go"
	fi, err := os.Stat(filename)
	if err != nil {
		log.Fatalln(err)
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err)
	}
	_, err = uploader.Upload(FileHeader{file.Name(), fi.Size(), file}, "")
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
