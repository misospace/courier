package skills

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

// The bounds of one approved bundle. A bundle is operator-authored prompt
// and asset content, so it stays small by construction; the bounds fail
// startup loudly on a misconfigured mount (a whole filesystem accidentally
// mounted, say) rather than silently digesting unbounded data.
const (
	// maxBundleFiles is the largest entry count one bundle may carry.
	maxBundleFiles = 512
	// maxBundleBytes is the largest total content one bundle may carry.
	maxBundleBytes = 8 << 20 // 8 MiB
)

// bundleFile is one regular file's materialized content, keyed by its
// slash-separated path relative to the resolved root.
type bundleFile struct {
	rel     string
	content []byte
}

// Resolve validates and digests one bundle: the source root may be a
// symlink (kubelet ConfigMap/Secret projections place per-key symlinks and a
// ..data directory at the mount root), but the interior must be a plain
// directory tree — any symlink inside is rejected, which closes the
// traversal-escape class by construction since the walker only yields paths
// under the resolved root. SKILL.md is required at the bundle root and
// non-empty. The digest is deterministic and domain-separated, hashed over
// files in sorted relative-path order so equal content in different
// locations digests identically and any content change, addition, or rename
// changes the digest.
func Resolve(spec bundleSpec) (Bundle, error) {
	// The source root MAY be a symlink: kubelet ConfigMap/Secret mounts
	// place per-key symlinks and a ..data -> ..<timestamp> directory at the
	// mount root. EvalSymlinks lands on the real content dir; the timestamp
	// lives only in the resolved root path, never in relative paths, so
	// digests stay stable across kubelet remounts.
	root, err := filepath.EvalSymlinks(spec.Source)
	if err != nil {
		return Bundle{}, fmt.Errorf("resolve source %s: %w", spec.Source, err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return Bundle{}, fmt.Errorf("stat source %s: %w", spec.Source, err)
	}
	if !info.IsDir() {
		return Bundle{}, fmt.Errorf("source %s: not a directory", spec.Source)
	}
	var (
		files  []bundleFile
		skill  []byte
		hasSK  bool
		nfiles int
		bytes  int64
	)
	err = fs.WalkDir(os.DirFS(root), ".", func(rel string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("%s: %w", rel, err)
		}
		if rel == "." {
			return nil
		}
		full := filepath.Join(root, filepath.FromSlash(rel))
		entryInfo, err := os.Lstat(full)
		if err != nil {
			return fmt.Errorf("%s: %w", rel, err)
		}
		if entryInfo.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink %s: bundles must be plain directory trees", rel)
		}
		if entryInfo.IsDir() {
			return nil
		}
		if !entryInfo.Mode().IsRegular() {
			return fmt.Errorf("%s: not a regular file", rel)
		}
		if entryInfo.Size() > maxBundleBytes {
			return fmt.Errorf("%s: file exceeds %d byte limit", rel, maxBundleBytes)
		}
		content, err := os.ReadFile(full)
		if err != nil {
			return fmt.Errorf("read %s: %w", rel, err)
		}
		nfiles++
		bytes += int64(len(content))
		if rel == "SKILL.md" {
			hasSK = true
			skill = content
		}
		files = append(files, bundleFile{rel: rel, content: content})
		if nfiles > maxBundleFiles {
			return fmt.Errorf("bundle exceeds %d file limit", maxBundleFiles)
		}
		if bytes > maxBundleBytes {
			return fmt.Errorf("bundle exceeds %d byte limit", maxBundleBytes)
		}
		return nil
	})
	if err != nil {
		return Bundle{}, err
	}
	if !hasSK {
		return Bundle{}, fmt.Errorf("SKILL.md is required at the bundle root")
	}
	if len(skill) == 0 {
		return Bundle{}, fmt.Errorf("SKILL.md is empty")
	}
	// Never hash in walk order: sort by relative slash path first so the
	// digest depends only on content, not on filesystem enumeration order.
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	h := sha256.New()
	fmt.Fprintf(h, "courier-skill-bundle-v1\n")
	fmt.Fprintf(h, "files %d bytes %d\n", len(files), bytes)
	for _, file := range files {
		fmt.Fprintf(h, "file %s %d\n", file.rel, len(file.content))
		h.Write(file.content)
	}
	return Bundle{
		Name:   spec.Name,
		Source: spec.Source,
		Digest: "sha256:" + hex.EncodeToString(h.Sum(nil)),
		Path:   root,
		Files:  nfiles,
		Bytes:  bytes,
	}, nil
}
