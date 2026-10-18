package devenv

import (
	"errors"
	"slices"
	"strings"
)

// EnvMarker is the fixed line Script emits between the bootstrap task's own
// report and the environment capture, so output parsers can split the two.
// It is a fixed constant: never interpolated with brief- or model-supplied
// values.
const EnvMarker = "@@COURIER_ENV"

// errNoEnvMarker reports task output without the marker line: the bootstrap
// never reached the environment capture, so nothing downstream can be trusted
// from it.
var errNoEnvMarker = errors.New("bootstrap environment marker not found in task output")

// errMalformedEnvCapture reports a record in the NUL-separated environment
// capture that is not a well-formed KEY=VALUE pair.
var errMalformedEnvCapture = errors.New("malformed environment capture")

// Script returns the fixed POSIX sh bootstrap task script used on the secure
// worker: `set -eu`, run the bootstrap task, emit the marker on its own line,
// then capture `mise x -- env -0`. It is fixed constants only — no
// interpolation, no brief- or model-supplied values — because the worker
// executes whatever bytes it is given and the contract's trust comes from the
// repository, not from this script.
func Script() string {
	return "set -eu\n" +
		"mise run bootstrap\n" +
		"echo '" + EnvMarker + "'\n" +
		"mise x -- env -0\n"
}

// ParseScriptOutput splits a worker task's captured stdout into the
// bootstrap's own report — everything before the LAST EnvMarker line — and
// the NUL-separated environment records after it, filtered into the delta
// over baseEnv exactly as Run filters its capture. A missing marker or a
// malformed record is an error; callers treat that as a bootstrap failure.
// baseEnv is the worker's base environment, filtered the same way.
func ParseScriptOutput(stdout string, baseEnv []string) (report string, delta []string, keys []string, err error) {
	lines := strings.Split(stdout, "\n")
	last := -1
	for i, line := range lines {
		if line == EnvMarker {
			last = i
		}
	}
	if last < 0 {
		return "", nil, nil, errNoEnvMarker
	}
	report = strings.Join(lines[:last], "\n")
	capture := strings.Join(lines[last+1:], "\n")
	records, err := splitNUL(capture)
	if err != nil {
		return "", nil, nil, err
	}
	delta, err = envDelta(records, baseEnvMap(baseEnv))
	if err != nil {
		return "", nil, nil, errMalformedEnvCapture
	}
	keys = make([]string, 0, len(delta))
	for _, kv := range delta {
		name, _, _ := strings.Cut(kv, "=")
		keys = append(keys, name)
	}
	slices.Sort(keys)
	return report, delta, keys, nil
}
