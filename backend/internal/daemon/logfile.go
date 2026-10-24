package daemon

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

const (
	daemonLogDir      = "logs"
	daemonLogName     = "daemon.log"
	daemonLogMaxBytes = 10 << 20
)

// openLogFile opens <dataDir>/logs/daemon.log for append. A log from a previous
// run larger than daemonLogMaxBytes is moved to daemon.log.1 first, so the file
// stays bounded without a background rotator. Any failure returns nil: file
// logging is diagnostic and must never stop the daemon from starting.
func openLogFile(dataDir string) io.WriteCloser {
	if dataDir == "" {
		return nil
	}
	dir := filepath.Join(dataDir, daemonLogDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil
	}
	path := filepath.Join(dir, daemonLogName)
	if info, err := os.Stat(path); err == nil && info.Size() > daemonLogMaxBytes {
		_ = os.Remove(path + ".1")
		_ = os.Rename(path, path+".1")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil
	}
	return f
}

// newLogger returns the daemon's slog logger. Debug and above go to stderr so
// supervisors can capture it separately from any structured stdout protocol.
// Info and above are also appended to the data dir's daemon log, which is the
// only record that survives the supervisor closing the daemon's stderr.
func newLogger(dataDir string) *slog.Logger {
	stderr := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	file := openLogFile(dataDir)
	if file == nil {
		return slog.New(stderr)
	}
	return slog.New(slog.NewMultiHandler(stderr, slog.NewTextHandler(file, &slog.HandlerOptions{Level: slog.LevelInfo})))
}
