package dispatch

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/misospace/courier/internal/source"
)

type fakeClient struct {
	items        []source.WorkItem
	claims       []string
	releases     []string
	updates      []statusUpdate
	resolves     []string
	claimErr     error
	releaseErr   error
	setErr       error
	reports      []source.Lifecycle
	reportErr    error
	resolveErr   error
	preLaunchErr error
	preLaunches  []string
}

type statusUpdate struct {
	id     string
	status string
}

func (f *fakeClient) Discover(_ context.Context) ([]source.WorkItem, error) {
	return f.items, nil
}

func (f *fakeClient) Claim(_ context.Context, id string) error {
	f.claims = append(f.claims, id)
	return f.claimErr
}

func (f *fakeClient) Release(_ context.Context, id string) error {
	f.releases = append(f.releases, id)
	return f.releaseErr
}

func (f *fakeClient) SetStatus(_ context.Context, id, status string) error {
	f.updates = append(f.updates, statusUpdate{id: id, status: status})
	return f.setErr
}

func (f *fakeClient) Report(_ context.Context, _ string, lifecycle source.Lifecycle) error {
	f.reports = append(f.reports, lifecycle)
	return f.reportErr
}

func (f *fakeClient) PreLaunch(_ context.Context, id string) error {
	f.preLaunches = append(f.preLaunches, id)
	return f.preLaunchErr
}

func (f *fakeClient) Resolve(_ context.Context, id string) error {
	f.resolves = append(f.resolves, id)
	return f.resolveErr
}

func TestAdapterImplementsGenericSourceAdapter(t *testing.T) {
	var _ source.Adapter = New(&fakeClient{})
}

func TestAdapterClaim(t *testing.T) {
	client := &fakeClient{}
	adapter := New(client)
	item := source.WorkItem{ID: "work-123"}

	if err := adapter.Claim(context.Background(), item); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if !reflect.DeepEqual(client.claims, []string{"work-123"}) {
		t.Fatalf("claims = %#v, want %#v", client.claims, []string{"work-123"})
	}
}

func TestAdapterDiscoverAndRunSpec(t *testing.T) {
	item := source.WorkItem{ID: "opaque/work-123", Mode: "resolve-issue", Repo: "acme/widgets", Ref: 12, Lane: "local"}
	client := &fakeClient{items: []source.WorkItem{item}}
	adapter := New(client)
	items, err := adapter.Discover(context.Background())
	if err != nil || len(items) != 1 || items[0].ID != item.ID {
		t.Fatalf("Discover() = %#v, %v", items, err)
	}
	got := items[0].Spec("dispatch")
	want := source.RunSpec{Mode: item.Mode, Source: "dispatch", WorkItemID: item.ID, Repo: item.Repo, Ref: item.Ref, Lane: item.Lane}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("WorkItem.Spec() = %#v, want %#v", got, want)
	}
}

func TestAdapterPreLaunch(t *testing.T) {
	client := &fakeClient{}
	adapter := New(client)
	client.preLaunchErr = source.ErrStaleWork
	if err := adapter.PreLaunch(context.Background(), source.WorkItem{ID: "work-123"}); !errors.Is(err, source.ErrStaleWork) {
		t.Fatalf("PreLaunch() error = %v, want ErrStaleWork", err)
	}
	if !reflect.DeepEqual(client.preLaunches, []string{"work-123"}) {
		t.Fatalf("pre-launch IDs = %#v, want %#v", client.preLaunches, []string{"work-123"})
	}
}

func TestAdapterReleaseAndResolve(t *testing.T) {
	client := &fakeClient{}
	adapter := New(client)
	item := source.WorkItem{ID: "work-123"}
	if err := adapter.Release(context.Background(), item); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := adapter.Resolve(context.Background(), item); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if !reflect.DeepEqual(client.releases, []string{"work-123"}) || !reflect.DeepEqual(client.resolves, []string{"work-123"}) {
		t.Fatalf("release/resolve calls = %#v/%#v", client.releases, client.resolves)
	}
}

func TestAdapterTransitions(t *testing.T) {
	client := &fakeClient{}
	adapter := New(client)
	item := source.WorkItem{ID: "work-123"}

	tests := []struct {
		name string
		call func() error
		want string
	}{
		{name: "in-progress", call: func() error { return adapter.InProgress(context.Background(), item) }, want: "in-progress"},
		{name: "in-review", call: func() error { return adapter.InReview(context.Background(), item) }, want: "in-review"},
		{name: "needs-human", call: func() error { return adapter.NeedsHuman(context.Background(), item) }, want: "needs-human"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); err != nil {
				t.Fatalf("transition error = %v", err)
			}
		})
	}

	want := []statusUpdate{
		{id: "work-123", status: "in-progress"},
		{id: "work-123", status: "in-review"},
		{id: "work-123", status: "needs-human"},
	}
	if !reflect.DeepEqual(client.updates, want) {
		t.Fatalf("updates = %#v, want %#v", client.updates, want)
	}
}

func TestAdapterTransitionRejectsUnknownState(t *testing.T) {
	client := &fakeClient{}
	adapter := New(client)

	err := adapter.Transition(context.Background(), source.WorkItem{ID: "work-123"}, source.State("done"))
	if !errors.Is(err, ErrUnsupportedState) {
		t.Fatalf("error = %v, want ErrUnsupportedState", err)
	}
	if len(client.updates) != 0 {
		t.Fatalf("updates = %#v, want no API calls", client.updates)
	}
}

func TestAdapterPropagatesClientErrors(t *testing.T) {
	want := errors.New("upstream unavailable")
	client := &fakeClient{claimErr: want, releaseErr: want, setErr: want, reportErr: want, resolveErr: want}
	adapter := New(client)
	item := source.WorkItem{ID: "work-123"}

	if err := adapter.Claim(context.Background(), item); !errors.Is(err, want) {
		t.Fatalf("Claim() error = %v, want %v", err, want)
	}
	if err := adapter.InReview(context.Background(), item); !errors.Is(err, want) {
		t.Fatalf("InReview() error = %v, want %v", err, want)
	}
	if err := adapter.Report(context.Background(), item, source.Lifecycle{State: source.StateInReview, Result: source.ResultReady}); !errors.Is(err, want) {
		t.Fatalf("Report() error = %v, want %v", err, want)
	}
	if err := adapter.Release(context.Background(), item); !errors.Is(err, want) {
		t.Fatalf("Release() error = %v, want %v", err, want)
	}
	if err := adapter.Resolve(context.Background(), item); !errors.Is(err, want) {
		t.Fatalf("Resolve() error = %v, want %v", err, want)
	}
}

func TestAdapterRejectsInvalidConfigurationAndItems(t *testing.T) {
	tests := []struct {
		name    string
		adapter *Adapter
		item    source.WorkItem
		wantErr error
	}{
		{name: "nil adapter", adapter: nil, item: source.WorkItem{ID: "work-123"}, wantErr: ErrNilClient},
		{name: "nil client", adapter: New(nil), item: source.WorkItem{ID: "work-123"}, wantErr: ErrNilClient},
		{name: "empty item", adapter: New(&fakeClient{}), item: source.WorkItem{}, wantErr: ErrEmptyWorkItemID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.adapter.Claim(context.Background(), tt.item); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Claim() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
