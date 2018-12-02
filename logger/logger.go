package logger

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

const produceLogs = false

// ------------------------ Log Type definition- ----------------------

// MasterLog allows all master's threads to log into a logfile.
type Log struct {
	logFile *log.Logger
}

// Global variable for all threads to log.
var Logger *Log

// --------------------------- Log Functions --------------------------

// StartupMasterLog prepares the MasterLog and leaves it ready to use.
// The given logFile path provided is used for logging if produceLogs
// is set (otherwise, logs to stdout).
func StartLog(logFile string) {
	Logger = &Log{}
	if produceLogs {
		l, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

		if err != nil {
			fmt.Printf("Error opening logfile: %v", err)
			return
		}

		Logger.logFile = log.New(l, "", log.Ldate|log.Ltime)
		Logger.logFile.SetOutput(&lumberjack.Logger{
			Filename:   logFile,
			MaxSize:    1,  // MB after which new logfile is created
			MaxBackups: 3,  // old logfiles kept at the same time
			MaxAge:     10, // days until automagically delete logfiles
		})
	} else {
		Logger.logFile = log.New(os.Stdout, "", log.Ldate|log.Ltime)
	}
}

// Warning logs a warning message into the logfile.
func (l *Log) Warning(msg string) {
	l.logFile.Printf("WARNING: " + msg + "\n")
}

// Error logs an error message into the logfile.
func (l *Log) Error(msg string) {
	l.logFile.Printf("ERROR: " + msg + "\n")
}

// Info logs an info message into the logfile.
func (l *Log) Info(msg string) {
	l.logFile.Printf("INFO: " + msg + "\n")
}
