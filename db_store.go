package go_file_uploader

import (
	"github.com/jinzhu/gorm"
)

type dbStore struct {
	db *gorm.DB
}

func (s *dbStore) FileIsNotExistError(err error) bool {
	return FileIsNotExistError(err)
}

func (s *dbStore) FileLoad(hash string) (fileModel *FileModel, err error) {
	fileModel = &FileModel{}
	err = s.db.Where(FileModel{Hash: hash}).First(&fileModel).Error
	if gorm.IsRecordNotFoundError(err) {
		err = ErrFileNotExist
	}
	return
}

func (s *dbStore) FileCreate(fileModel *FileModel) error {
	return s.db.FirstOrCreate(&fileModel, "`hash` = ?", fileModel.Hash).Error
}

func (s *dbStore) FileExist(hash string) (bool, error) {
	var count uint
	err := s.db.Model(FileModel{}).Where(FileModel{Hash: hash}).Count(&count).Error
	return count > 0, err
}

func NewDBStore(db *gorm.DB) Store {
	return &dbStore{db}
}
