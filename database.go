package csvquery

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
	"sync"
)

// Database that contains all CSV files as tables. Adding and reading tables
// from the database is not thread-safe. Adding tables should happen before
// they are going to be read.
type Database struct {
	sync.RWMutex
	name          string
	tables        map[string]sql.Table
	tablesInLower map[string]sql.Table
}

func (d *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	d.RLock()
	defer d.RUnlock()
	if _, ok := d.tables[tblName]; ok {
		return d.tables[tblName], true, nil
	}
	if _, ok := d.tablesInLower[tblName]; ok {
		return d.tablesInLower[tblName], true, nil
	}
	return nil, false, nil
}

func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	d.RLock()
	tbNames := make([]string, 0, len(d.tables))
	d.RUnlock()
	for k := range d.tables {
		tbNames = append(tbNames, k)
	}
	return tbNames, nil
}

// NewDatabase creates a new database with the given name.
func NewDatabase(name string) *Database {
	return &Database{
		name:          name,
		tables:        make(map[string]sql.Table),
		tablesInLower: make(map[string]sql.Table),
	}
}

// Name returns the name of the database.
func (d *Database) Name() string {
	return d.name
}

// Tables returns a map of the tables indexed by name.
func (d *Database) Tables() map[string]sql.Table {
	d.RLock()
	defer d.RUnlock()
	return d.tables
}

// AddTable adds a new table with the given name and path.
func (d *Database) AddTable(name, path string) error {
	d.Lock()
	defer d.Unlock()
	if _, ok := d.tables[name]; ok {
		return fmt.Errorf("table with name %q already registered", name)
	}

	t, err := NewTable(name, path)
	if err != nil {
		return fmt.Errorf("unable to add table %q: %s", name, err)
	}

	d.tables[name] = t
	d.tablesInLower[strings.ToLower(name)] = t
	return nil
}
func (d *Database) DropTable(name string) error {
	d.Lock()
	defer d.Unlock()
	delete(d.tables, name)
	delete(d.tablesInLower, strings.ToLower(name))
	return nil
}
