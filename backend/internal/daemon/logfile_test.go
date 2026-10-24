package daemon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewLoggerWritesInfoToDataDirFile(t *testing.T) {
	dir := t.TempDir()
	log := newLogger(dir)
	log.Debug("debug-only")
	log.Info("recorded line")
	data, err := os.ReadFile(filepath.Join(dir, daemonLogDir, daemonLogName))
	if err != nil {
		t.Fatalf("read daemon log: %v", err)
	}
	if !strings.Contains(string(data), "recorded line") || strings.Contains(string(data), "debug-only") {
		t.Fatalf("daemon log = %q, want the info line only", data)
	}
}
