package provision

import "testing"

func TestCreatePlanPermitsFirstInitialization(t *testing.T) {
	f := provisioned(t)
	plan := installedPlan(t, f)
	var starts int
	for _, step := range plan.Steps {
		if start := step.Command.Input.GetStartContainer(); start != nil {
			starts++
			if !start.AllowInitialization {
				t.Fatal("the create plan refuses first initialization")
			}
		}
	}
	if starts != 1 {
		t.Fatalf("start steps = %d, want 1", starts)
	}
}
