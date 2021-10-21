package migration

import "gorm.io/gorm"

// ApplyFunc declares func type for migration functions.
type ApplyFunc func(db *gorm.DB) error

// Migration declares a migration data structure.
type Migration struct {
	Version int64     `gorm:"primaryKey"`
	Name    string    `gorm:"name"`
	Up      ApplyFunc `gorm:"-"`
	Down    ApplyFunc `gorm:"-"`
	Stored  bool      `gorm:"-"`
}

// Less returns `true` if an argument is more than current.
func (mig *Migration) Less(migration *Migration) bool {
	return CompareMigrations(mig, migration)
}

// Eq returns `true` if migrations version are equal.
func (mig *Migration) Eq(migration *Migration) bool {
	return migration.Version == mig.Version
}

// Migrations type declares a slice-type of `Migration` with an implementation of `sort.Sort` interface.
type Migrations []Migration

func (ms Migrations) Len() int {
	return len(ms)
}

func (ms Migrations) Swap(i int, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

func (ms Migrations) Less(i int, j int) bool {
	return CompareMigrations(&ms[i], &ms[j])
}

// CompareMigrations compares two migrations and returns `true` if `left` migration is less.
func CompareMigrations(left *Migration, right *Migration) bool {
	return left.Version < right.Version
}
