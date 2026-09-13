//go:build linux || darwin || dragonfly || freebsd || illumos || netbsd || openbsd

package ingot

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const (
	lockHelperEnv     = "INGOT_LOCK_HELPER"
	lockHelperDataDir = "INGOT_LOCK_HELPER_DATA_DIR"
)

func TestOpenRejectsSecondOpen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if second, err := Open(dir, Options{}); !errors.Is(err, ErrDataDirLocked) || second != nil {
		t.Fatalf("second open: db=%v err=%v, want nil DB and ErrDataDirLocked", second, err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close first DB: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, dataDirLockFilename)); err != nil {
		t.Fatalf("persistent lock file: %v", err)
	}

	reopened, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened DB: %v", err)
	}
}

func TestDataDirLockReleasedAfterProcessExit(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDataDirLockHelper$")
	cmd.Env = append(os.Environ(), lockHelperEnv+"=1", lockHelperDataDir+"="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("helper stdout: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})

	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "ready" {
		cmd.Process.Kill()
		waitErr := cmd.Wait()
		stopped = true
		t.Fatalf("helper did not become ready: output=%q stderr=%q wait=%v", scanner.Text(), stderr.String(), waitErr)
	}
	if db, err := Open(dir, Options{}); !errors.Is(err, ErrDataDirLocked) || db != nil {
		t.Fatalf("open while helper owns lock: db=%v err=%v", db, err)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill helper: %v", err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("killed helper exited successfully")
	}
	stopped = true

	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open after helper exit: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close DB: %v", err)
	}
}

func TestDataDirLockHelper(t *testing.T) {
	if os.Getenv(lockHelperEnv) != "1" {
		return
	}
	db, err := Open(os.Getenv(lockHelperDataDir), Options{})
	if err != nil {
		t.Fatalf("helper open: %v", err)
	}
	defer db.Close()
	fmt.Println("ready")
	select {}
}

func TestOpenFailureReleasesDataDirLock(t *testing.T) {
	dir := t.TempDir()
	broken := filepath.Join(dir, "broken-block")
	if err := os.Mkdir(broken, 0755); err != nil {
		t.Fatalf("create broken block: %v", err)
	}
	if err := os.WriteFile(filepath.Join(broken, "meta.json"), []byte("{"), 0644); err != nil {
		t.Fatalf("write broken metadata: %v", err)
	}
	if db, err := Open(dir, Options{}); err == nil || db != nil {
		t.Fatalf("open broken store: db=%v err=%v", db, err)
	}
	if err := os.RemoveAll(broken); err != nil {
		t.Fatalf("remove broken block: %v", err)
	}

	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open after startup failure: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close DB: %v", err)
	}
}
