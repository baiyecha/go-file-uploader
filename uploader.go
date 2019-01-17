package go_file_uploader

import (
	"context"
	"errors"
	"io"
	"net/url"
	"time"
)

type FileHeader struct {
	Filename string        // file name
	Size     int64         // file size
	File     io.ReadSeeker // file body
}

type ChunkHeader struct {
	ChunkNumber    int           // 分片 ID
	UploadId       string        // 同一个对象的分块上传都要携带该 uploadId
	OriginFilename string        // 对象的名称
	OriginFileHash string        // 对象 Hash
	OriginFileSize int64         // 对象 Size
	IsLastChunk    bool          // 是否是最后一个分片
	ChunkContent   io.ReadSeeker // 分片内容
	ChunkCount     int           // 分片数量
}

type Uploader interface {
	// 普通上传
	Upload(fh FileHeader, extra string) (f *FileModel, err error)
	// 获取文件链接
	PresignedGetObject(hashValue string, expires time.Duration, reqParams url.Values) (u *url.URL, err error)
	// 读文件
	ReadFile(hashValue string) (rf ReadFile, err error)
	// Store
	Store() Store
	// 分片上传
	UploadChunk(ch ChunkHeader, extra string) (f *FileModel, uploadId string, err error)
	// 分片读取 (Range)
	ReadChunk(hashValue, rangeValue string) (rf ReadFile, err error)
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
