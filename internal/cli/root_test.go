package cli

import (
	"bytes"
	"testing"
)

func TestExecute_ConfigFailure(t *testing.T) {
	// Point to a non-existent configuration file path to guarantee config.Load fails
	CfgPath = "/nonexistent/path/to/config.yaml"

	// Capture Cobra output buffers
	buf := new(bytes.Buffer)
	RootCmd.SetOut(buf)
	RootCmd.SetErr(buf)

	// Invoke the command via Args array targeting our bad path
	RootCmd.SetArgs([]string{"--config", CfgPath})

	err := RootCmd.Execute()
	if err == nil {
		t.Fatal("expected command execution to fail due to a missing configuration file, but got no error")
	}

	expectedMsg := "failed to process config"
	if !bytes.Contains(buf.Bytes(), []byte(expectedMsg)) && !bytes.Contains([]byte(err.Error()), []byte(expectedMsg)) {
		t.Errorf("expected error message to contain %q; got error: %v, log output: %s", expectedMsg, err, buf.String())
	}
}
