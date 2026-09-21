package executor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCoordinatorDockerfileUsesNumericIdentity(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	dockerfile := string(data)
	if !strings.Contains(dockerfile, "groupadd --gid 65532 courier") ||
		!strings.Contains(dockerfile, "useradd --create-home --uid 65532 --gid 65532 courier") {
		t.Fatal("coordinator Dockerfile does not create the documented 65532 user/group")
	}
	if !strings.Contains(dockerfile, "USER 65532:65532") {
		t.Fatal("coordinator Dockerfile does not select the numeric 65532:65532 user")
	}
	if strings.Contains(dockerfile, "\nUSER courier\n") {
		t.Fatal("coordinator Dockerfile regressed to a named-only runtime user")
	}
}
