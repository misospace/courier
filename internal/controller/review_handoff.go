package controller

import "context"

// ReviewHandoffResolver reads the reviewer's machine-readable remediation
// handoff ("review-handoff/v1", #755) for a pull request through the
// configured forge capability — never a forge-specific CLI. The reviewer's
// handoff rides its own managed comment or review body; the resolver lists
// those bodies through the registry-selected forge provider for the run's
// repository, keeps only bodies the configured reviewer identity posted,
// validates the payload against the published upstream contract, and refuses
// a payload recorded for any head other than headSHA.
//
// found=false means no trusted handoff exists for that head — the reviewer
// has not reviewed it, published nothing, or published only for another head.
// An error means the read or validation failed; the caller decides whether
// the absence of structured fix input is survivable, and the fix-pr launch
// treats it as survivable because unstructured feedback remains available to
// the coordinator.
type ReviewHandoffResolver interface {
	ReviewHandoff(ctx context.Context, repo string, number int, reviewerLogin, headSHA string) (handoff string, found bool, err error)
}

// ReviewHandoffResolverFunc adapts a function to ReviewHandoffResolver.
type ReviewHandoffResolverFunc func(ctx context.Context, repo string, number int, reviewerLogin, headSHA string) (string, bool, error)

func (f ReviewHandoffResolverFunc) ReviewHandoff(ctx context.Context, repo string, number int, reviewerLogin, headSHA string) (string, bool, error) {
	return f(ctx, repo, number, reviewerLogin, headSHA)
}
