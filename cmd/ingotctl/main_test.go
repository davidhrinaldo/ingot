package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"git.dvdt.dev/david/ingot/internal/block"
	"git.dvdt.dev/david/ingot/internal/chunkenc"
	"git.dvdt.dev/david/ingot/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeChunk(t *testing.T, samples []struct{ t int64; v float64 }) []byte {
	t.Helper()
	c := chunkenc.NewXORChunk()
	a, err := c.Appender()
	require.NoError(t, err)
	for _, s := range samples {
		a.Append(s.t, s.v)
	}
	return append([]byte(nil), c.Bytes()...)
}

func setupTestData(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	samples := []struct{ t int64; v float64 }{
		{0, 1.0}, {15000, 2.0}, {30000, 3.0},
	}
	chunk := makeChunk(t, samples)

	series := []block.SeriesFlush{
		{
			Ref:    1,
			Labels: labels.FromStrings("__name__", "temp", "room", "office"),
			Chunks: []block.ChunkData{
				{MinT: 0, MaxT: 30000, Data: chunk},
			},
		},
		{
			Ref:    2,
			Labels: labels.FromStrings("__name__", "humidity", "room", "office"),
			Chunks: []block.ChunkData{
				{MinT: 0, MaxT: 30000, Data: chunk},
			},
		},
	}

	_, err := block.Flush(dir, series)
	require.NoError(t, err)
	return dir
}

// blockDir returns the first block directory inside dataDir.
func blockDir(t *testing.T, dataDir string) string {
	t.Helper()
	entries, err := os.ReadDir(dataDir)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() && e.Name() != "wal" {
			return filepath.Join(dataDir, e.Name())
		}
	}
	t.Fatal("no block directory found")
	return ""
}

// captureStdout calls fn with stdout redirected and returns the output.
func captureStdout(fn func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	fn()
	w.Close()
	os.Stdout = old
	out := make([]byte, 16384)
	n, _ := r.Read(out)
	return string(out[:n])
}

func TestCmdBlocks(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantErr     string // "" = no error expected
		wantOutputs []string
	}{
		{
			name:        "list_blocks",
			args:        nil, // replaced in loop with setupTestData result
			wantErr:     "",
			wantOutputs: []string{"ULID", "1 block(s) total"},
		},
		{
			name:        "no_blocks",
			args:        nil, // replaced with empty TempDir
			wantErr:     "",
			wantOutputs: []string{"no blocks found"},
		},
		{
			name:        "missing_args",
			args:        []string{},
			wantErr:     "usage",
			wantOutputs: nil,
		},
	}

	// Set up args that need dynamic dirs.
	dataDir := setupTestData(t)
	emptyDir := t.TempDir()
	tests[0].args = []string{dataDir}
	tests[1].args = []string{emptyDir}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var output string
			err := func() error {
				var cmdErr error
				output = captureStdout(func() { cmdErr = cmdBlocks(tc.args) })
				return cmdErr
			}()

			assert.Equal(t, tc.wantErr != "", err != nil, "error presence")
			assert.Contains(t, errString(err), tc.wantErr)
			for _, want := range tc.wantOutputs {
				assert.Contains(t, output, want)
			}
		})
	}
}

func TestCmdInspect(t *testing.T) {
	dataDir := setupTestData(t)
	bd := blockDir(t, dataDir)

	tests := []struct {
		name        string
		args        []string
		wantErr     string
		wantOutputs []string
	}{
		{
			name:        "valid_block",
			args:        []string{bd},
			wantErr:     "",
			wantOutputs: []string{"Block Meta", "__name__", "Postings"},
		},
		{
			name:        "missing_args",
			args:        []string{},
			wantErr:     "usage",
			wantOutputs: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var output string
			err := func() error {
				var cmdErr error
				output = captureStdout(func() { cmdErr = cmdInspect(tc.args) })
				return cmdErr
			}()

			assert.Equal(t, tc.wantErr != "", err != nil, "error presence")
			assert.Contains(t, errString(err), tc.wantErr)
			for _, want := range tc.wantOutputs {
				assert.Contains(t, output, want)
			}
		})
	}
}

func TestCmdChunks(t *testing.T) {
	dataDir := setupTestData(t)
	bd := blockDir(t, dataDir)

	tests := []struct {
		name        string
		args        []string
		wantErr     string
		wantOutputs []string
	}{
		{
			name:        "valid_ref",
			args:        []string{bd, "1"},
			wantErr:     "",
			wantOutputs: []string{"Series 1", "(3 samples)"},
		},
		{
			name:        "ref_not_found",
			args:        []string{bd, "999"},
			wantErr:     "not found",
			wantOutputs: nil,
		},
		{
			name:        "missing_ref_arg",
			args:        []string{bd},
			wantErr:     "usage",
			wantOutputs: nil,
		},
		{
			name:        "missing_all_args",
			args:        []string{},
			wantErr:     "usage",
			wantOutputs: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var output string
			err := func() error {
				var cmdErr error
				output = captureStdout(func() { cmdErr = cmdChunks(tc.args) })
				return cmdErr
			}()

			assert.Equal(t, tc.wantErr != "", err != nil, "error presence")
			assert.Contains(t, errString(err), tc.wantErr)
			for _, want := range tc.wantOutputs {
				assert.Contains(t, output, want)
			}
		})
	}
}

func TestCmdFsck(t *testing.T) {
	dataDir := setupTestData(t)
	emptyDir := t.TempDir()

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "valid_data",
			args:    []string{dataDir},
			wantErr: "",
		},
		{
			name:    "no_blocks",
			args:    []string{emptyDir},
			wantErr: "",
		},
		{
			name:    "missing_args",
			args:    []string{},
			wantErr: "usage",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := func() error {
				var cmdErr error
				captureStdout(func() { cmdErr = cmdFsck(tc.args) })
				return cmdErr
			}()

			assert.Equal(t, tc.wantErr != "", err != nil, "error presence")
			assert.Contains(t, errString(err), tc.wantErr)
		})
	}
}

// errString returns the error message or "" for nil.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return strings.ToLower(err.Error())
}
