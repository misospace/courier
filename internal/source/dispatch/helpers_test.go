package dispatch

import "time"

// testOwnershipBindings binds every claimant the package's shared PR-repair
// fixtures use to one queue lane, so a fixture is authorized exactly when the
// client under test owns that lane. Tests that assert on the ownership gate
// install their own bindings, which replace this authority.
func testOwnershipBindings(lane string) map[string]string {
	return map[string]string{"worker": lane, "local": lane, "cloud": lane, "frontier": lane}
}

// newTestDispatchClient builds the Dispatch client the package's tests use:
// the production constructor plus the configured-ownership authority the
// shared PR-repair fixtures need, so the ordinary lifecycle tests exercise the
// authorized path instead of tripping the gate.
func newTestDispatchClient(serverURL, agentName, lane, token string, timeout time.Duration) (*HTTPClient, error) {
	client, err := NewClientWithLane(serverURL, agentName, lane, token, timeout)
	if err != nil {
		return nil, err
	}
	client.WithOwnershipBindings(testOwnershipBindings(lane))
	return client, nil
}
