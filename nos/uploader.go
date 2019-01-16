package nos

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/NetEase-Object-Storage/nos-golang-sdk/model"
	"github.com/NetEase-Object-Storage/nos-golang-sdk/nosclient"
	"github.com/NetEase-Object-Storage/nos-golang-sdk/nosconst"
	. "github.com/wq1019/go-file-uploader"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"time"
)

type nosUploader struct {
	h                Hasher
	client           *nosclient.NosClient
	bucketName       string
	endPoint         string // 无奈
	externalEndpoint string // 外网链接
	h2sn             Hash2StorageName
	s                Store
}

const (
	protocol       = "https://"
	AllowMaxUpload = 100 << 20 // 网易云规定普通上传接口最大只允许上传 100MB
	ChunkSize      = 100 << 20 // 每片只允许 100MB
	MaxFileSize    = 1 << 40
)

func (n *nosUploader) initMultiUpload(object, ext string) (res *model.InitMultiUploadResult, err error) {
	res, err = n.client.InitMultiUpload(&model.InitMultiUploadRequest{
		Bucket: n.bucketName,
		Object: object,
		Metadata: &model.ObjectMetadata{
			// 自定义 meta data
			Metadata: map[string]string{
				nosconst.CONTENT_TYPE: mime.TypeByExtension(ext),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (n *nosUploader) Upload(fh FileHeader, extra string) (f *FileModel, err error) {
	hashValue, err := n.h.Hash(fh.File)
	if err != nil {
		return nil, err
	}
	if fh.Size > MaxFileSize {
		return nil, fmt.Errorf("file size is too large")
	}

	if exist, err := n.s.FileExist(hashValue); exist && err == nil {
		// 文件已经存在
		file, err := n.s.FileLoad(hashValue)
		return file, err
	} else if err != nil {
		return nil, err
	}

	err = n.saveStreamToNos(hashValue, fh)
	if err != nil {
		return nil, err
	}
	return SaveToStore(n.s, hashValue, fh, extra)
}

func (n *nosUploader) UploadChunk(fh FileHeader, extra string) (f *FileModel, err error) {
	hashValue, err := n.h.Hash(fh.File)
	if err != nil {
		return nil, err
	}

	if exist, err := n.s.FileExist(hashValue); exist && err == nil {
		// 文件已经存在
		file, err := n.s.FileLoad(hashValue)
		return file, err
	} else if err != nil {
		return nil, err
	}
	objectName, err := n.h2sn.Convent(hashValue)
	if err != nil {
		return nil, fmt.Errorf("hash to storage name error. err: %+v", err)
	}
	ext := filepath.Ext(fh.Filename)
	if ext == "jpeg" {
		ext = "jpg"
	}
	// 获取 UploadId
	initRes, err := n.initMultiUpload(objectName, ext)
	if err != nil {
		return nil, err
	}
	// 开始分块上传文件
	err = n.savePartToNos(objectName, hashValue, initRes.UploadId, fh)
	if err != nil {
		return nil, err
	}
	// 保存到数据库
	return SaveToStore(n.s, hashValue, fh, extra)
}

func (nu *nosUploader) savePartToNos(objectName, objectHash, uploadId string, fh FileHeader) error {
	// 跳转到文件的开头
	_, err := fh.File.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	uploadPartRequest := &model.UploadPartRequest{
		Bucket:   nu.bucketName,
		Object:   objectName,
		UploadId: uploadId,
	}
	var (
		partNum int
		etags   = make([]model.UploadPart, 0, 10)
	)
	for {
		buffer := make([]byte, ChunkSize)
		partNum++
		readLen, err := fh.File.Read(buffer)
		if err != nil || readLen == 0 {
			break
		}
		md5hash := md5.New()
		n, err := md5hash.Write(buffer[:readLen])
		if err != nil || n == 0 {
			break
		}
		uploadPartRequest.PartSize = int64(readLen)
		uploadPartRequest.PartNumber = partNum
		uploadPartRequest.Content = buffer
		uploadPartRequest.ContentMd5 = fmt.Sprintf("%x", md5hash.Sum(nil))
		uploadPart, err := nu.client.UploadPart(uploadPartRequest)
		if err != nil {
			return err
		}
		etags = append(etags, model.UploadPart{
			PartNumber: partNum,
			Etag:       uploadPart.Etag,
		})
	}
	_, err = nu.client.CompleteMultiUpload(&model.CompleteMultiUploadRequest{
		Bucket:    nu.bucketName,
		Object:    objectName,
		UploadId:  uploadId,
		Parts:     etags,      // map: partnum, etag
		ObjectMd5: objectHash, // big file md5
	})
	return err
}

func (n *nosUploader) saveStreamToNos(hashValue string, fh FileHeader) error {
	name, err := n.h2sn.Convent(hashValue)
	if err != nil {
		return fmt.Errorf("hash to storage name error. err: %+v", err)
	}
	// 跳转到文件的开头
	_, err = fh.File.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	if fh.Size > AllowMaxUpload {
		return fmt.Errorf("file size is too large")
	}
	ext := filepath.Ext(fh.Filename)
	// 在 apline 镜像中 mime.TypeByExtension 只能用 jpg
	if ext == "jpeg" {
		ext = "jpg"
	}
	// Nos 只允许 最大 100MB 的文件
	_, err = n.client.PutObjectByStream(&model.PutObjectRequest{
		Bucket: n.bucketName,
		Object: name,
		Body:   fh.File,
		Metadata: &model.ObjectMetadata{
			ContentLength: fh.Size,
			Metadata: map[string]string{
				nosconst.CONTENT_TYPE: mime.TypeByExtension(ext),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("nos client put object stream error. err: %+v", err)
	}

	return nil
}

// 网易云的对象存储暂时不知道怎么设置过期时间
// 而且这里官方 API 居然没有提供这种方法
// endPoint 还是 TM 私有变量....
// 不知道是哪个垃圾网易云程序员写的代码
func (n *nosUploader) PresignedGetObject(hashValue string, expires time.Duration, reqParams url.Values) (u *url.URL, err error) {
	name, err := n.h2sn.Convent(hashValue)
	if err != nil {
		return nil, err
	}
	urlStr := /*protocol + n.bucketName + "." +*/ n.externalEndpoint + "/" + name
	req, err := http.NewRequest(http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	return req.URL, nil
}

func (n *nosUploader) ReadFile(hashValue string) (rf ReadFile, err error) {
	name, err := n.h2sn.Convent(hashValue)
	if err != nil {
		return
	}
	obj, err := n.client.GetObject(&model.GetObjectRequest{
		Bucket: n.bucketName,
		Object: name,
	})
	if err != nil {
		return
	}

	return &readFile{obj}, nil
}

func (n *nosUploader) Store() Store {
	return n.s
}

type readFile struct {
	*model.NOSObject
}

func (rf *readFile) Read(p []byte) (n int, err error) {
	return rf.Body.Read(p)
}

func (rf *readFile) Close() error {
	return rf.Body.Close()
}

func (rf *readFile) Seek(offset int64, whence int) (int64, error) {
	body, err := ioutil.ReadAll(rf.Body)
	if err != nil {
		return 0, err
	}
	reader := bytes.NewReader(body)
	return reader.Seek(offset, whence)
}

func (rf *readFile) ReadAt(p []byte, off int64) (n int, err error) {
	body, err := ioutil.ReadAll(rf.Body)
	if err != nil {
		return 0, err
	}
	reader := bytes.NewReader(body)
	return reader.ReadAt(p, off)
}

// TODO
// Deprecated: not be allow use.
func (rf *readFile) Stat() (fi *FileInfo, err error) {
	fi = &FileInfo{}
	body, err := ioutil.ReadAll(rf.Body)
	if err != nil {
		return nil, err
	}
	return &FileInfo{LastModified: time.Now(), Size: int64(len(body)), ContentType: ""}, nil
}

func NewNosUploader(h Hasher, client *nosclient.NosClient, s Store, bucketName string, h2sn Hash2StorageName, endPrint, externalEndpoint string) Uploader {
	if h2sn == nil {
		h2sn = Hash2StorageNameFunc(DefaultHash2StorageNameFunc)
	}
	return &nosUploader{h: h, client: client, bucketName: bucketName, h2sn: h2sn, s: s, endPoint: endPrint, externalEndpoint: externalEndpoint}
}
