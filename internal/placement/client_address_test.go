package placement

import "testing"

func TestNodeClientAddressRequiresCanonicalRemoteUnicast(t *testing.T) {
	_, snapshot := fixture()
	policy := *snapshot.Nodes[0].Policy
	for _, address := range []string{"", "10.20.30.40", "192.168.1.2", "203.0.113.9", "2001:db8::7", "fd12::10"} {
		t.Run("accept/"+address, func(t *testing.T) {
			policy.ClientAddress = address
			if err := policy.Validate(); err != nil {
				t.Fatalf("address %q: %v", address, err)
			}
		})
	}
	for _, address := range []string{"0.0.0.0", "::", "127.0.0.1", "127.1.2.3", "::1", "169.254.1.2", "fe80::1", "224.0.0.1", "ff02::1", "255.255.255.255", "::ffff:192.168.1.2", "2001:DB8::7", "2001:db8:0:0:0:0:0:7", "fd12::10%eth0", "192.168.1.2:5432", "[2001:db8::7]", "db.example.com", " 10.20.30.40"} {
		t.Run("refuse/"+address, func(t *testing.T) {
			policy.ClientAddress = address
			if err := policy.Validate(); err == nil {
				t.Fatalf("accepted address %q", address)
			}
		})
	}
}
