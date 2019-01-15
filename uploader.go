package go_file_uploader

import (
	"context"
	"errors"
	"io"
	"net/url"
	"time"
)

type FileHeader struct {
	Filename string
	Size     int64
	File     io.ReadSeeker
}

type Uploader interface {
	// 普通上传
	Upload(fh FileHeader, extra string) (f *FileModel, err error)
	// 分片上传(暂时就这样写吧, 其实不应该这样写的)
	UploadChunk(fh FileHeader, extra string) (f *FileModel, err error)
	// 获取文件链接
	PresignedGetObject(hashValue string, expires time.Duration, reqParams url.Values) (u *url.URL, err error)
	// 读文件
	ReadFile(hashValue string) (rf ReadFile, err error)
	// Store
	Store() Store
}

type FileInfo struct {
	LastModified time.Time `json:"lastModified"` // Date and time the object was last modified.
	Size         int64     `json:"size"`         // Size in bytes of the object.
	ContentType  string    `json:"contentType"`  // A standard MIME type describing the format of the object data.
}

type ReadFile interface {
	io.Reader
	io.Closer
	io.Seeker
	io.ReaderAt
	Stat() (*FileInfo, error)
}

func Upload(ctx context.Context, fh FileHeader, extra string) (f *FileModel, err error) {
	u, ok := FromContext(ctx)
	if !ok {
		return nil, errors.New("uploader不存在")
	}
	return u.Upload(fh, extra)
}
