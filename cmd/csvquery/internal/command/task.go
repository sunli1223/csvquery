package command

import (
	"csvquery"
	"fmt"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	fileLitsMap     = make(map[string]string)
	dirList         []string
	db              *csvquery.Database
	durationSeconds = 3
)

func run(database *csvquery.Database) {
	db = database
	c := cron.New()
	c.AddFunc("@every "+strconv.Itoa(durationSeconds)+"s", func() {
		logrus.Infof("refesh the csv files every %d seconds", durationSeconds)
		watch()
	})
	c.Start()
	time.Sleep(time.Second * 5)
}
func addCsvFile(file string, tableName string) {
	fileLitsMap[file] = tableName
}
func addDir(dir string) {
	dirList = append(dirList, dir)
}
func watch() {
	currentFiles := listAllDirFiles()
	var currentFileLitsMap = make(map[string]string)
	for _, file := range currentFiles {
		currentFileLitsMap[file] = ""
		//是新文件
		if _, exists := fileLitsMap[file]; !exists {
			name, path := splitFile(file)
			if err := db.AddTable(name, path); err != nil {
				logrus.Errorf("add new csv file  [%s] error %s", path, err)
			}
			addCsvFile(path, name)
			logrus.Infof("add new csv file  [%s]", file)
		}
	}
	//检查文件是否被删除
	for key := range fileLitsMap {
		if _, exists := currentFileLitsMap[key]; !exists {
			//文件被删除
			logrus.Infof("csv file  [%s] have been deleted", key)
			db.DropTable(fileLitsMap[key])
			delete(fileLitsMap, key)
		}
	}

}
func listAllDirFiles() (allFiles []string) {
	var files []string
	for _, f := range dirList {

		err := filepath.Walk(f, func(fullPath string, info os.FileInfo, err error) error {
			fileSuffix := path.Ext(fullPath)
			if !info.IsDir() && strings.ToLower(fileSuffix) == ".csv" {
				files = append(files, fullPath)
			}
			return nil
		})
		if err != nil {
			fmt.Errorf("csvquery: unable to list dir[%s]: %s", f, err)
		}
	}
	return files
}
