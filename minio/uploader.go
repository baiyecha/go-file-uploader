package minio

import (
	"fmt"
	"github.com/minio/minio-go"
	. "github.com/wq1019/go-file-uploader"
	"io"
	"mime"
	"net/url"
	"path/filepath"
	"time"
)

type minioUploader struct {
	h           Hasher
	minioClient *minio.Client
	bucketName  string
	h2sn        Hash2StorageName
	s           Store
}

type readFile struct {
	*minio.Object
}

func (mu *minioUploader) UploadChunk(ch ChunkHeader, extra string) (f *FileModel, uploadId string, err error) {
	panic("暂时没有实现")
}

func (mu *minioUploader) ReadChunk(hashValue, rangeValue string) (rf ReadFile, err error) {
	panic("暂时没有实现")
}

func (mu *minioUploader) Upload(fh FileHeader, extra string) (f *FileModel, err error) {
	hashValue, err := mu.h.Hash(fh.File)
	if err != nil {
		return nil, err
	}

	if exist, err := mu.s.FileExist(hashValue); exist && err == nil {
		// 文件已经存在
		file, err := mu.s.FileLoad(hashValue)
		return file, err
	} else if err != nil {
		return nil, err
	}

	err = mu.saveToMinio(hashValue, fh)
	if err != nil {
		return nil, err
	}

	return SaveToStore(mu.s, hashValue, fh, extra)
}

func (mu *minioUploader) PresignedGetObject(hashValue string, expires time.Duration, reqParams url.Values) (u *url.URL, err error) {
	name, err := mu.h2sn.Convent(hashValue)
	if err != nil {
		return nil, err
	}
	return mu.minioClient.PresignedGetObject(mu.bucketName, name, expires, reqParams)
}

func (mu *minioUploader) Store() Store {
	return mu.s
}

func (rf *readFile) Stat() (*FileInfo, error) {
	info, err := rf.Object.Stat()
	if err != nil {
		return nil, err
	}
	return &FileInfo{
		LastModified: info.LastModified,
		Size:         info.Size,
		ContentType:  info.ContentType,
	}, nil
}

func (mu *minioUploader) ReadFile(hashValue string) (rf ReadFile, err error) {
	name, err := mu.h2sn.Convent(hashValue)
	if err != nil {
		return
	}
	obj, err := mu.minioClient.GetObject(mu.bucketName, name, minio.GetObjectOptions{})
	if err != nil {
		return
	}
	return &readFile{obj}, nil
}

func (mu *minioUploader) saveToMinio(hashValue string, fh FileHeader) error {
	name, err := mu.h2sn.Convent(hashValue)
	if err != nil {
		return fmt.Errorf("hash to storage name error. err:%+v", err)
	}
	// 跳转到文件的开头
	_, err = fh.File.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	ext := filepath.Ext(fh.Filename)
	// 在 apline 镜像中 mime.TypeByExtension 只能用 jpg
	if ext == "jpeg" {
		ext = "jpg"
	}

	_, err = mu.minioClient.PutObject(
		mu.bucketName,
		name,
		fh.File,
		fh.Size,
		minio.PutObjectOptions{ContentType: mime.TypeByExtension(ext)},
	)

	if err != nil {
		return fmt.Errorf("minio client put object error. err:%+v", err)
	}

	return nil
}

func NewMinioUploader(h Hasher, minioClient *minio.Client, s Store, bucketName string, h2sn Hash2StorageName) Uploader {
	if h2sn == nil {
		h2sn = Hash2StorageNameFunc(DefaultHash2StorageNameFunc)
	}
	return &minioUploader{h: h, minioClient: minioClient, bucketName: bucketName, h2sn: h2sn, s: s}
}
