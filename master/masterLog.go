package master

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

const produceLogs = true

// ------------------------ Log Type definition- ----------------------

// MasterLog allows all master's threads to log into a logfile.
type MasterLog struct {
	logFile *log.Logger
}

// Global variable for all threads to log.
var masterLog *MasterLog

// --------------------------- Log Functions --------------------------

// StartupMasterLog prepares the MasterLog and leaves it ready to use.
// The given logFile path provided is used for logging if produceLogs
// is set (otherwise, logs to stdout).
func StartupMasterLog(logFile string) {
	masterLog = &MasterLog{}
	if produceLogs {
		l, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

		if err != nil {
			fmt.Printf("Error opening logfile: %v", err)
			return
		}

		masterLog.logFile = log.New(l, "", log.Ldate|log.Ltime)
		masterLog.logFile.SetOutput(&lumberjack.Logger{
			Filename:   logFile,
			MaxSize:    1,  // MB after which new logfile is created
			MaxBackups: 3,  // old logfiles kept at the same time
			MaxAge:     10, // days until automagically delete logfiles
		})
	} else {
		masterLog.logFile = log.New(os.Stdout, "", log.Ldate|log.Ltime)
	}
}

// Warning logs a warning message into the logfile.
func (ml *MasterLog) Warning(msg string) {
	ml.logFile.Printf("WARNING: " + msg + "\n")
}

// Error logs an error message into the logfile.
func (ml *MasterLog) Error(msg string) {
	ml.logFile.Printf("ERROR: " + msg + "\n")
}

// Info logs an info message into the logfile.
func (ml *MasterLog) Info(msg string) {
	ml.logFile.Printf("INFO: " + msg + "\n")
}
