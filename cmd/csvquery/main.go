package main

import (
	"bytes"
	"csvquery/cmd/csvquery/internal/command"
	"fmt"
	flags "github.com/jessevdk/go-flags"
	"github.com/keepeye/logrus-filename"
	"github.com/sirupsen/logrus"
	"os"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type LogFormatter struct{}

func (m *LogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	timestamp := entry.Time.Format("2006-01-02 15:04:05")
	var newLog string

	//HasCaller()为true才会有调用信息
	if entry.HasCaller() {
		if v, ok := entry.Data["query"]; ok && logrus.GetLevel() < logrus.InfoLevel {
			newLog = fmt.Sprintf("[%s] [%s] [%s] %s %s\n", timestamp, entry.Level, entry.Data["line"], v)
		} else {
			newLog = fmt.Sprintf("[%s] [%s] [%s] %s\n", timestamp, entry.Level, entry.Data["line"], entry.Message)
		}
	} else {
		newLog = fmt.Sprintf("[%s] [%s] %s\n", timestamp, entry.Level, entry.Message)
	}

	b.WriteString(newLog)
	return b.Bytes(), nil
}

func main() {
	filenameHook := filename.NewHook()
	filenameHook.Field = "line"
	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.InfoLevel)
	logrus.AddHook(filenameHook)
	logrus.SetFormatter(&LogFormatter{})
	parser := flags.NewNamedParser("csvquery", flags.Default)

	_, err := parser.AddCommand(
		"server",
		"Start a MySQL-compatible server to query CSV files.",
		"",
		new(command.Server),
	)
	if err != nil {
		logrus.Fatal(err)
	}

	if err != nil {
		logrus.Fatal(err)
	}

	_, err = parser.AddCommand(
		"version",
		"Show version of the program.",
		"",
		&command.Version{Version: version, Commit: commit, Date: date},
	)
	if err != nil {
		logrus.Fatal(err)
	}

	if _, err := parser.Parse(); err != nil {
		if e, ok := err.(*flags.Error); ok && e.Type == flags.ErrCommandRequired {
			parser.WriteHelp(os.Stdout)
		}

		os.Exit(1)
	}
}
