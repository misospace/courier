package source

import "testing"

func TestWorkItemSpecCarriesOpaqueIdentity(t *testing.T) {
	item := WorkItem{
		ID:   "dispatch-task/opaque-42",
		Mode: "resolve-issue",
		Repo: "acme/widgets",
		Ref:  42,
		Lane: "local",
	}

	got := item.Spec("dispatch")
	if got.WorkItemID != item.ID {
		t.Fatalf("WorkItem.Spec().WorkItemID = %q, want %q", got.WorkItemID, item.ID)
	}
	if got.Source != "dispatch" || got.Mode != item.Mode || got.Repo != item.Repo || got.Ref != item.Ref || got.Lane != item.Lane {
		t.Fatalf("WorkItem.Spec() did not preserve run identity: %#v", got)
	}
}
