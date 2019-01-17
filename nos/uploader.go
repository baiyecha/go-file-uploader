package nos

import (
	"bytes"
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
	AllowMaxUpload = 100 << 20 // 网易云规定普通上传接口最大只允许上传 100MB
	ChunkMaxSize   = 100 << 20 // 每片只允许 100MB
	ChunkMinSize   = 16 << 10  // 除最后一片外每片最小必须是 16KB
	MaxFileSize    = 1 << 40   // 最大文件大小
	MaxChunkNumber = 10000     // 最大片的数量
)

func (nu *nosUploader) Upload(fh FileHeader, extra string) (f *FileModel, err error) {
	hashValue, err := nu.h.Hash(fh.File)
	if err != nil {
		return nil, err
	}

	if fh.Size > MaxFileSize {
		return nil, fmt.Errorf("file size is too large")
	}

	if exist, err := nu.s.FileExist(hashValue); exist && err == nil {
		// 文件已经存在
		file, err := nu.s.FileLoad(hashValue)
		return file, err
	} else if err != nil {
		return nil, err
	}

	err = nu.saveStreamToNos(hashValue, fh)
	if err != nil {
		return nil, err
	}
	return SaveToStore(nu.s, hashValue, fh, extra)
}

func (nu *nosUploader) UploadChunk(ch ChunkHeader, extra string) (fileModel *FileModel, uploadId string, err error) {
	// 对源文件 hash 改造成 XX/XXXXXXXX 形式作为文件名
	objectName, err := nu.h2sn.Convent(ch.OriginFileHash)
	if err != nil {
		return nil, "", fmt.Errorf("hash to storage name error. err: %+v", err)
	}
	// 判断有没有传 uploadID
	if ch.UploadId == "" {
		ext := filepath.Ext(ch.OriginFilename)
		if ext == "jpeg" {
			ext = "jpg"
		}
		// 第一次上传时需要获取 UploadId, 以后每次传分块时都要带上这个 uploadId
		initRes, err := nu.initChunkUpload(objectName, ext)
		if err != nil {
			return nil, "", err
		}
		// chunk 上传完成后返回 uploadID 以便下次上传时使用
		ch.UploadId = initRes.UploadId
	}
	// 上传分片
	// 跳转到文件的开头
	_, err = ch.ChunkContent.Seek(0, io.SeekStart)
	if err != nil {
		return nil, "", err
	}
	// 读取分片
	contentBytes, err := ioutil.ReadAll(ch.ChunkContent)
	if err != nil {
		return nil, "", err
	}
	// 获取分片长度
	contentLen := int64(len(contentBytes))
	if contentLen <= 0 {
		return nil, "", fmt.Errorf("分片读取失败")
	}
	if contentLen > ChunkMaxSize {
		return nil, "", fmt.Errorf("分片太大, 不允许上传")
	}
	if ch.IsLastChunk == false && contentLen < ChunkMinSize {
		return nil, "", fmt.Errorf("分片太小, 不允许上传")
	}
	if ch.ChunkNumber > MaxChunkNumber {
		return nil, "", fmt.Errorf("分片数量超过 %d, 不允许上传", MaxChunkNumber)
	}
	chunkHashValue, err := nu.h.Hash(ch.ChunkContent)
	if err != nil {
		return nil, "", fmt.Errorf("分片 Hash 计算失败: %+v", err)
	}
	_, err = nu.client.UploadPart(&model.UploadPartRequest{
		Bucket:     nu.bucketName,
		Object:     objectName,
		UploadId:   ch.UploadId,
		PartSize:   contentLen,
		PartNumber: ch.ChunkNumber,
		Content:    contentBytes,
		ContentMd5: chunkHashValue,
	})
	if err != nil {
		return nil, "", fmt.Errorf("分片上传失败: %+v", err)
	}
	// 如果是最后一次上传分片, 则在最后一片上传完成后合并文件
	if ch.IsLastChunk {
		completeResult, err := nu.client.ListUploadParts(&model.ListUploadPartsRequest{
			Bucket:   nu.bucketName,
			Object:   objectName,
			UploadId: ch.UploadId,
			MaxParts: 1000,
		})
		if err != nil {
			return nil, "", fmt.Errorf("合并分片失败: %+v", err)
		}
		var (
			etags = make([]model.UploadPart, 0, 10)
		)
		for _, part := range completeResult.Parts {
			etags = append(etags, model.UploadPart{
				PartNumber: part.PartNumber,
				Etag:       part.Etag,
			})
		}
		if len(etags) > 0 {
			_, err = nu.client.CompleteMultiUpload(&model.CompleteMultiUploadRequest{
				Bucket:    nu.bucketName,
				Object:    objectName,
				UploadId:  ch.UploadId,
				Parts:     etags,             // map: partNumber, etag
				ObjectMd5: ch.OriginFileHash, // big file md5
			})
		} else {
			return nil, "", fmt.Errorf("ons 分片读取失败")
		}
		// 保存到数据库
		fModel, err := SaveToStore(nu.s, ch.OriginFileHash, FileHeader{
			Filename: ch.OriginFilename,
			Size:     ch.OriginFileSize,
		}, extra)
		if err != nil {
			return nil, "", fmt.Errorf("文件保存到数据库失败: %+v", err)
		}
		// 最后一次返回 fModel, uploadID 可以不用返回
		return fModel, ch.UploadId, nil
	} else {
		return nil, ch.UploadId, nil
	}
}

func (nu *nosUploader) ReadChunk(hashValue, rangeValue string) (rf ReadFile, err error) {
	objectName, err := nu.h2sn.Convent(hashValue)
	if err != nil {
		return
	}

	obj, err := nu.client.GetObject(&model.GetObjectRequest{
		Bucket:   nu.bucketName,
		Object:   objectName,
		ObjRange: rangeValue,
	})
	if err != nil {
		return
	}
	return &readFile{obj}, nil
}

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

func (nu *nosUploader) ReadFile(hashValue string) (rf ReadFile, err error) {
	name, err := nu.h2sn.Convent(hashValue)
	if err != nil {
		return
	}
	obj, err := nu.client.GetObject(&model.GetObjectRequest{
		Bucket: nu.bucketName,
		Object: name,
	})
	if err != nil {
		return
	}

	return &readFile{obj}, nil
}

func (nu *nosUploader) Store() Store {
	return nu.s
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

func (rf *readFile) Stat() (fileInfo *FileInfo, err error) {
	return nil, fmt.Errorf("nos client does not support access to fileinfo")
}

func (nu *nosUploader) initChunkUpload(objectName, ext string) (uploadResult *model.InitMultiUploadResult, err error) {
	uploadResult, err = nu.client.InitMultiUpload(&model.InitMultiUploadRequest{
		Bucket: nu.bucketName,
		Object: objectName,
		Metadata: &model.ObjectMetadata{
			Metadata: map[string]string{
				nosconst.CONTENT_TYPE: mime.TypeByExtension(ext),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return uploadResult, nil
}

func (nu *nosUploader) saveStreamToNos(hashValue string, fh FileHeader) error {
	name, err := nu.h2sn.Convent(hashValue)
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
	_, err = nu.client.PutObjectByStream(&model.PutObjectRequest{
		Bucket: nu.bucketName,
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

func NewNosUploader(h Hasher, client *nosclient.NosClient, s Store, bucketName string, h2sn Hash2StorageName, endPrint, externalEndpoint string) Uploader {
	if h2sn == nil {
		h2sn = Hash2StorageNameFunc(DefaultHash2StorageNameFunc)
	}
	return &nosUploader{h: h, client: client, bucketName: bucketName, h2sn: h2sn, s: s, endPoint: endPrint, externalEndpoint: externalEndpoint}
}
