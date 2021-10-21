package migration

import "gorm.io/gorm"

// DummyUpDown is a dummy migration function.
func DummyUpDown(db *gorm.DB) error {
	return nil
}
