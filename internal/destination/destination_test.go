package destination

import (
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/repository"
)

func TestOwnDeviceLossWarning(t *testing.T) {
	source := uuid.New()
	local := Destination{NodeID: source, Location: repository.Location{Kind: "local"}}
	remote := Destination{Location: repository.Location{Kind: "s3"}}
	if !OnlyOnSource(source, []Destination{local, local}) {
		t.Fatal("two copies on one source device must warn")
	}
	if OnlyOnSource(source, nil) || OnlyOnSource(source, []Destination{local, remote}) || OnlyOnSource(uuid.New(), []Destination{local}) {
		t.Fatal("incorrect own-device warning")
	}
}
