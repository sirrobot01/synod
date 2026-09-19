package controlstream

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func inspectionRequest(t *testing.T) *agentv1.CommandInspectionRequest {
	t.Helper()
	resourceID := uuid.NewString()
	command := &agentv1.Command{
		Epoch:       &agentv1.Epoch{InstallationId: uuid.NewString(), NodeId: uuid.NewString(), ClusterId: uuid.NewString(), RecoveryGeneration: uuid.NewString(), ExecutionEpoch: 1},
		OperationId: uuid.NewString(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: 1, PlanVersion: 1,
		Input: &agentv1.CommandInput{ResourceId: resourceID, PayloadVersion: 1, Timeout: durationpb.New(time.Minute), Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: resourceID}}},
	}
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	return &agentv1.CommandInspectionRequest{RequestId: uuid.NewString(), Command: command, PriorEffects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: resourceID, NativeOperationId: "docker-command:" + command.CommandId, Active: true, OutcomeUnknown: true, State: "unknown"}}}
}

func TestInspectionRequestValidation(t *testing.T) {
	request := inspectionRequest(t)
	epoch := request.Command.Epoch
	if err := CheckControllerMessage(&agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CommandInspection{CommandInspection: request}}, epoch.InstallationId, epoch.NodeId); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*agentv1.CommandInspectionRequest){
		"invalid request ID":   func(r *agentv1.CommandInspectionRequest) { r.RequestId = "invalid" },
		"missing command":      func(r *agentv1.CommandInspectionRequest) { r.Command = nil },
		"changed input":        func(r *agentv1.CommandInspectionRequest) { r.Command.DesiredRevision++ },
		"foreign node":         func(r *agentv1.CommandInspectionRequest) { r.Command.Epoch.NodeId = uuid.NewString() },
		"foreign installation": func(r *agentv1.CommandInspectionRequest) { r.Command.Epoch.InstallationId = uuid.NewString() },
		"bad epoch":            func(r *agentv1.CommandInspectionRequest) { r.Command.Epoch.ExecutionEpoch = 0 },
		"nil effect":           func(r *agentv1.CommandInspectionRequest) { r.PriorEffects[0] = nil },
		"foreign resource":     func(r *agentv1.CommandInspectionRequest) { r.PriorEffects[0].ResourceId = uuid.NewString() },
		"foreign command":      func(r *agentv1.CommandInspectionRequest) { r.PriorEffects[0].CommandId = uuid.NewString() },
		"duplicate identity":   func(r *agentv1.CommandInspectionRequest) { r.PriorEffects = append(r.PriorEffects, r.PriorEffects[0]) },
		"oversized effects":    func(r *agentv1.CommandInspectionRequest) { r.PriorEffects = make([]*agentv1.Effect, 1025) },
		"oversized envelope":   func(r *agentv1.CommandInspectionRequest) { r.Command.TraceId = strings.Repeat("a", 1<<20) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(request).(*agentv1.CommandInspectionRequest)
			mutate(changed)
			if err := CheckCommandInspectionRequest(changed, epoch.InstallationId, epoch.NodeId); !errors.Is(err, ErrMessage) {
				t.Fatalf("accepted invalid request: %v", err)
			}
		})
	}
}

func TestInspectionResultValidation(t *testing.T) {
	request := inspectionRequest(t)
	epoch := request.Command.Epoch
	result := &agentv1.CommandInspectionResult{RequestId: request.RequestId, Epoch: epoch, CommandId: request.Command.CommandId, InputHash: bytes.Clone(request.Command.InputHash), Effects: request.PriorEffects}
	if err := CheckAgentMessage(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_CommandInspection{CommandInspection: result}}, epoch.InstallationId, epoch.NodeId); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*agentv1.CommandInspectionResult){
		"bad request":        func(r *agentv1.CommandInspectionResult) { r.RequestId = "invalid" },
		"bad command":        func(r *agentv1.CommandInspectionResult) { r.CommandId = "invalid" },
		"bad hash":           func(r *agentv1.CommandInspectionResult) { r.InputHash = nil },
		"foreign node":       func(r *agentv1.CommandInspectionResult) { r.Epoch.NodeId = uuid.NewString() },
		"missing epoch":      func(r *agentv1.CommandInspectionResult) { r.Epoch = nil },
		"failure text":       func(r *agentv1.CommandInspectionResult) { r.FailureCode = "unbounded error text" },
		"nil effect":         func(r *agentv1.CommandInspectionResult) { r.Effects[0] = nil },
		"foreign command":    func(r *agentv1.CommandInspectionResult) { r.Effects[0].CommandId = uuid.NewString() },
		"invalid resource":   func(r *agentv1.CommandInspectionResult) { r.Effects[0].ResourceId = "invalid" },
		"native control":     func(r *agentv1.CommandInspectionResult) { r.Effects[0].NativeOperationId = "native\n" },
		"native length":      func(r *agentv1.CommandInspectionResult) { r.Effects[0].NativeOperationId = strings.Repeat("a", 257) },
		"state text":         func(r *agentv1.CommandInspectionResult) { r.Effects[0].State = "raw output" },
		"oversized effects":  func(r *agentv1.CommandInspectionResult) { r.Effects = make([]*agentv1.Effect, 1025) },
		"oversized envelope": func(r *agentv1.CommandInspectionResult) { r.FailureCode = strings.Repeat("a", 1<<20) },
		"mixed resources": func(r *agentv1.CommandInspectionResult) {
			r.Effects = append(r.Effects, &agentv1.Effect{CommandId: r.CommandId, ResourceId: uuid.NewString(), NativeOperationId: "another", State: "inactive"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(result).(*agentv1.CommandInspectionResult)
			mutate(changed)
			if err := CheckCommandInspectionResult(changed, epoch.InstallationId, epoch.NodeId); !errors.Is(err, ErrMessage) {
				t.Fatalf("accepted invalid result: %v", err)
			}
		})
	}
}
