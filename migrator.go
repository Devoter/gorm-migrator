package migrator

import (
	"sort"
	"strconv"

	"gorm.io/gorm"

	"github.com/Devoter/gorm-migrator/migration"
)

// Migrator declares GORM migrations manager.
type Migrator struct {
	db         *gorm.DB
	migrations []migration.Migration
}

// NewMigrator returns a new instance of Migrator.
func NewMigrator(db *gorm.DB, migrations []migration.Migration) *Migrator {
	all := append(migrations, migration.Migration{Version: 1, Name: "-", Up: migration.DummyUpDown, Down: migration.DummyUpDown})
	sort.Sort(migration.Migrations(all))

	return &Migrator{db: db, migrations: all}
}

// Run interprets commands.
func (m *Migrator) Run(args ...string) (oldVersion int64, newVersion int64, err error) {
	if len(args) == 0 {
		err = ErrorCommandRequired
		return
	}

	switch args[0] {
	case "init":
		return m.Init()
	case "up":
		var target int64

		if target, err = m.parseVersion(false, args[1:]...); err != nil {
			return
		}

		return m.Up(target)
	case "down":
		return m.Down()
	case "reset":
		return m.Reset()
	case "version":
		return m.Version()
	case "set_version":
		var target int64

		if target, err = m.parseVersion(true, args[1:]...); err != nil {
			return
		}

		return m.SetVersion(target)
	default:
		err = ErrorUnexpectedCommand
		return
	}
}

// Init creates `migrations` table if it does not exist and records the initial zero-migration.
func (m *Migrator) Init() (oldVersion int64, newVersion int64, err error) {
	migr := &migration.Migration{Version: 1, Name: "-"}
	var mig migration.Migration

	if err = m.db.Migrator().CreateTable(&mig); err != nil {
		// ToDo: check error details
		return
	}

	result := m.db.Create(&migr)
	err = result.Error

	return
}

// Up upgrades database revision to the target or next version.
func (m *Migrator) Up(target int64) (oldVersion int64, newVersion int64, err error) {
	var history = []migration.Migration{}

	if result := m.db.Order("version ASC").Find(&history); result.Error != nil {
		err = result.Error
		return
	}

	for i := range history {
		history[i].Stored = true
	}

	length := len(history)

	if length > 0 {
		version := history[length-1].Version
		oldVersion = version
		newVersion = version
	}

	merged := m.mergeMigrations(history, m.migrations, target)

	for _, migr := range merged {
		if !migr.Stored {
			if err = migr.Up(m.db); err != nil {
				return
			}

			newVersion = migr.Version
			migr.Stored = true

			if result := m.db.Create(&migr); result.Error != nil {
				err = result.Error
				return
			}
		}
	}

	return
}

// Down downgrades database revision to the previous version.
func (m *Migrator) Down() (oldVersion int64, newVersion int64, err error) {
	var old migration.Migration

	if result := m.db.Order("version DESC").First(&old); result.Error != nil {
		err = result.Error
		return
	}

	oldVersion = old.Version
	newVersion = old.Version

	for i := len(m.migrations) - 1; i >= 0; i-- {
		mig := m.migrations[i]

		if mig.Version == old.Version {
			if i > 0 {
				if err = mig.Down(m.db); err != nil {
					return
				}

				newVersion = m.migrations[i-1].Version

				if result := m.db.Delete(&mig); result.Error != nil {
					err = result.Error
					return
				}
			}

			return
		}
	}

	return
}

// Reset resets database to the zero-revision.
func (m *Migrator) Reset() (oldVersion int64, newVersion int64, err error) {
	history := []migration.Migration{}

	if result := m.db.Order("version ASC").Find(&history); result.Error != nil {
		err = result.Error
		return
	}

	for i := range history {
		history[i].Stored = true
	}

	length := len(history)

	if length > 0 {
		version := history[length-1].Version
		oldVersion = version
		newVersion = version
	}

	var correlated []migration.Migration

	if correlated, err = m.correlateMigrations(history, m.migrations); err != nil {
		return
	}

	for i := len(correlated) - 1; i >= 0; i-- {
		migr := correlated[i]

		if err = migr.Down(m.db); err != nil {
			return
		}

		if i > 0 {
			newVersion = correlated[i-1].Version
		} else {
			newVersion = migr.Version
		}

		migr.Stored = true

		// don't delete zero migration
		if migr.Version > 1 {
			if result := m.db.Delete(&migr); result.Error != nil {
				err = result.Error
				return
			}
		}
	}

	return
}

// Version returns current database revision version.
func (m *Migrator) Version() (oldVersion int64, newVersion int64, err error) {
	var mig migration.Migration

	if result := m.db.Last(&mig); result.Error != nil {
		err = result.Error
		return
	}

	oldVersion = mig.Version
	newVersion = mig.Version

	return
}

// SetVersion forces database revisiton version.
func (m *Migrator) SetVersion(target int64) (oldVersion int64, newVersion int64, err error) {
	oldVersion, _, err = m.Version()
	if err != nil {
		return
	}

	index := -1
	migs := make([]migration.Migration, 0, len(m.migrations))

	for i, migr := range m.migrations {
		migs = append(migs, migr)

		if migr.Version == target {
			index = i
			break
		}
	}

	if index == -1 {
		err = ErrorTargetVersionNotFound
		return
	} else if oldVersion == m.migrations[index].Version {
		newVersion = oldVersion
	}

	if result := m.db.Exec("DELETE FROM migrations"); result.Error != nil {
		err = result.Error
		return
	}

	if result := m.db.Create(&migs); result.Error != nil {
		err = result.Error
		return
	}

	newVersion = migs[len(migs)-1].Version

	return
}

func (m *Migrator) parseVersion(required bool, args ...string) (version int64, err error) {
	if len(args) == 0 {
		if required {
			err = ErrorVersionNumberRequired
			return
		}

		version = -1
		return
	}

	version, err = strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		err = ErrorInvalidVersionArgumentFormat
		return
	}

	return
}

// mergreMigrations returns a slice contains a sorted list of all migrations (applied and actual).
func (m *Migrator) mergeMigrations(applied, actual []migration.Migration, target int64) []migration.Migration {
	appliedLength := len(applied)
	actualLength := len(actual)
	merged := make([]migration.Migration, 0, appliedLength+actualLength)
	i := 0
	j := 0
	var max int64

	if actualLength > 0 {
		if target == -1 {
			max = actual[actualLength-1].Version + 1
		} else {
			max = target + 1
		}
	}

	for (i < appliedLength) && (j < actualLength) && (actualLength == 0 || actual[j].Version < max) {
		if applied[i].Less(&actual[j]) {
			merged = append(merged, applied[i])
			i++
		} else if actual[j].Less(&applied[j]) {
			merged = append(merged, actual[j])
			j++
		} else {
			merged = append(merged, applied[i])
			i++
			j++
		}
	}

	for i < appliedLength {
		merged = append(merged, applied[i])
		i++
	}

	for j < actualLength && (actualLength == 0 || actual[j].Version < max) {
		merged = append(merged, actual[j])
		j++
	}

	return merged
}

// CorrelateMigrations returns a list of correlated migrations.
// This method replaces stored migrations with actual migrations. If some actual migration is absent
// the method returns an error and a list which contains missing migration as the last item.
func (m *Migrator) correlateMigrations(applied, actual []migration.Migration) (correlated []migration.Migration, err error) {
	appliedLength := len(applied)
	actualLength := len(actual)
	i := 0
	j := 0
	correlated = make([]migration.Migration, 0, appliedLength)

	for (i < appliedLength) && (j < actualLength) {
		if applied[i].Less(&actual[j]) {
			correlated = append(correlated, applied[i])
			err = ErrorSomeMigrationsAreAbsent
			return
		} else if actual[j].Less(&applied[i]) {
			// skip unapplied migrations
			j++
		} else {
			correlated = append(correlated, actual[j])
			i++
			j++
		}
	}

	if i < appliedLength {
		correlated = append(correlated, applied[i])
		err = ErrorSomeMigrationsAreAbsent
	}

	return
}
