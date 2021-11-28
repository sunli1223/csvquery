package command

import (
	"csvquery"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unicode"
)

type baseCmd struct {
	Name  string   `long:"dbname" short:"d" default:"csv" description:"Database name."`
	Files []string `long:"file" short:"f" description:"Add file as a table. You can use the flag in the format '/path/to/file:NAME' to give the file a specific table name. Otherwise, the file name without extension will be the table name with only alphanumeric characters and underscores."`
}

func (b baseCmd) engine() (*sqle.Engine, *csvquery.Database, error) {
	db := csvquery.NewDatabase(b.Name)
	for _, f := range b.Files {
		if IsDir(f) {
			fileList, err := ListCSVFilesDir(f)
			if err != nil {
				panic(err)
			}
			addDir(f)
			for _, file := range fileList {
				name, path := splitFile(file)
				if err := db.AddTable(name, path); err != nil {
					return nil, nil, err
				}
				addCsvFile(path, name)
			}
		} else {
			name, path := splitFile(f)
			if err := db.AddTable(name, path); err != nil {
				return nil, nil, err
			}
			addCsvFile(path, name)
		}
	}

	engine := sqle.NewDefault(sql.NewDatabaseProvider(db, information_schema.NewInformationSchemaDatabase()))

	return engine, db, nil
}
func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}
func ListCSVFilesDir(directory string) ([]string, error) {
	var files []string
	err := filepath.Walk(directory, func(fullPath string, info os.FileInfo, err error) error {
		if strings.ToLower(path.Ext(fullPath)) == ".csv" {
			files = append(files, fullPath)
		}
		return nil
	})
	return files, err
}
func splitFile(s string) (name, path string) {
	suffixIdx := strings.LastIndex(s, ".")
	if idx := strings.LastIndex(s, ":"); idx > suffixIdx {
		path = s[:idx]
		name = s[idx+1:]
	} else {
		path = s
	}

	if name == "" {
		name = nameFromPath(path)
	}

	return name, path
}

func nameFromPath(path string) string {
	name := filepath.Base(path)
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[:idx]
	}

	return removeIllegalChars(name)
}

func removeIllegalChars(name string) string {
	var result []rune
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' {
			result = append(result, r)
		}
	}
	return string(result)
}
