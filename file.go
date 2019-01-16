package go_file_uploader

import "time"

type FileModel struct {
	//Id        int64     `json:"id" gorm:"type:BIGINT;AUTO_INCREMENT;PRIMARY_KEY;NOT NUll"`
	Hash      string    `json:"hash" gorm:"UNIQUE_INDEX;TYPE:CHAR(32)"`
	Format    string    `json:"format" gorm:"NOT NULL"`
	Filename  string    `json:"filename" gorm:"NOT NULL"`
	Size      int64     `json:"size" gorm:"NOT NULL"`
	Extra     string    `json:"extra" gorm:"NOT NULL;TYPE:TEXT"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (FileModel) TableName() string {
	return "files"
}
