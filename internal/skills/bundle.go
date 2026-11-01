package skills

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// File is one materialized bundle file: its relative slash path and its
// exact bytes.
type File struct {
	Path    string
	Content []byte
}

// MaterializedFiles returns the bundle's materialized files: the exact
// bytes the digest was computed over, each with its relative slash path.
// The returned slice and its contents are copies, so a caller that builds
// a run's Secret out of them cannot mutate the registry's bytes.
//
// The accessor is not named Files: Bundle already carries the bundle's
// file count in a Files field, and one type cannot have a field and a
// method of the same name.
func (b Bundle) MaterializedFiles() []File {
	files := make([]File, 0, len(b.files))
	for _, file := range b.files {
		files = append(files, File{Path: file.rel, Content: append([]byte(nil), file.content...)})
	}
	return files
}

// resolveBundle resolves one bundle in the form its config entry declares:
// a materialization block names the exact files the bundle carries and the
// content is read from that projection; without one the source is walked as
// a directory tree. Both forms share one digest, so a digest pinned over
// the deployment's mount matches the digest the control pod recomputes
// from the projection it was handed.
func resolveBundle(spec bundleSpec) (Bundle, error) {
	if spec.Materialization != nil {
		return resolveMaterialized(spec)
	}
	return Resolve(spec)
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
	return Bundle{
		Name:   spec.Name,
		Source: spec.Source,
		Digest: bundleDigest(files),
		Path:   root,
		Files:  nfiles,
		Bytes:  bytes,
		files:  files,
	}, nil
}

// resolveMaterialized reads one bundle from a mounted projection: the
// registered source root plus the authoritative list of relative file
// paths. The list is the whole content — the mount is never walked, so a
// file the projection carries but the list omits is invisible to the digest
// and to the loader. Every listed path is re-validated before use and each
// read must resolve inside the mount root, so a projection can never steer
// a read outside itself. The bounds, the required non-empty SKILL.md, and
// the digest are the tree form's: a materialized bundle is the same bundle.
func resolveMaterialized(spec bundleSpec) (Bundle, error) {
	if spec.Materialization == nil {
		return Bundle{}, fmt.Errorf("bundle carries no materialization block")
	}
	root, err := filepath.EvalSymlinks(spec.Source)
	if err != nil {
		return Bundle{}, fmt.Errorf("resolve source %s: %w", spec.Source, err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return Bundle{}, fmt.Errorf("stat source %s: %w", spec.Source, err)
	}
	if !info.IsDir() {
		return Bundle{}, fmt.Errorf("source %s: not a directory", spec.Source)
	}
	files := make([]bundleFile, 0, len(spec.Materialization.Files))
	seen := make(map[string]bool, len(spec.Materialization.Files))
	var (
		skill []byte
		hasSK bool
		bytes int64
	)
	for _, rel := range spec.Materialization.Files {
		if err := validateRelativePath(rel); err != nil {
			return Bundle{}, fmt.Errorf("materialization file %q: %w", rel, err)
		}
		if seen[rel] {
			return Bundle{}, fmt.Errorf("materialization file %q: listed twice", rel)
		}
		seen[rel] = true
		content, err := readMaterializedFile(root, rel)
		if err != nil {
			return Bundle{}, fmt.Errorf("materialization file %q: %w", rel, err)
		}
		nfiles := len(files) + 1
		if nfiles > maxBundleFiles {
			return Bundle{}, fmt.Errorf("bundle exceeds %d file limit", maxBundleFiles)
		}
		bytes += int64(len(content))
		if bytes > maxBundleBytes {
			return Bundle{}, fmt.Errorf("bundle exceeds %d byte limit", maxBundleBytes)
		}
		if rel == "SKILL.md" {
			hasSK = true
			skill = content
		}
		files = append(files, bundleFile{rel: rel, content: content})
	}
	if !hasSK {
		return Bundle{}, fmt.Errorf("SKILL.md is required at the bundle root")
	}
	if len(skill) == 0 {
		return Bundle{}, fmt.Errorf("SKILL.md is empty")
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	return Bundle{
		Name:   spec.Name,
		Source: spec.Source,
		Digest: bundleDigest(files),
		Path:   root,
		Files:  len(files),
		Bytes:  bytes,
		files:  files,
	}, nil
}

// readMaterializedFile reads one projected file: the listed path is joined
// onto the mount root and resolved through the projection's own symlinks
// (kubelet projects every key as a symlink into its ..data directory), and
// the resolved target must still sit inside the mount root — a projection
// pointing a key out of the mount is rejected, never followed.
func readMaterializedFile(root, rel string) ([]byte, error) {
	full := filepath.Join(root, filepath.FromSlash(rel))
	resolved, err := filepath.EvalSymlinks(full)
	if err != nil {
		return nil, fmt.Errorf("resolve: %w", err)
	}
	if !withinRoot(root, resolved) {
		return nil, fmt.Errorf("resolves outside the bundle mount")
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("not a regular file")
	}
	if info.Size() > maxBundleBytes {
		return nil, fmt.Errorf("file exceeds %d byte limit", maxBundleBytes)
	}
	content, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	return content, nil
}

// withinRoot reports whether path sits inside root. Both arguments are
// already symlink-resolved, so a projection's per-key symlinks into its
// own ..data directory stay inside the mount while a link out of the mount
// does not.
func withinRoot(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// bundleDigest computes one bundle's content identity: a domain-separated
// sha256 over the files in sorted relative-path order with per-file
// lengths, so equal content in different locations digests identically and
// any content change, addition, or rename changes the digest. The tree and
// materialized forms share it, which is what lets a digest pinned over the
// deployment's mount match the digest a run's pods recompute from the
// projection they were handed.
func bundleDigest(files []bundleFile) string {
	sorted := make([]bundleFile, len(files))
	copy(sorted, files)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].rel < sorted[j].rel })
	var bytes int64
	for _, file := range sorted {
		bytes += int64(len(file.content))
	}
	h := sha256.New()
	fmt.Fprintf(h, "courier-skill-bundle-v1\n")
	fmt.Fprintf(h, "files %d bytes %d\n", len(sorted), bytes)
	for _, file := range sorted {
		fmt.Fprintf(h, "file %s %d\n", file.rel, len(file.content))
		h.Write(file.content)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}
