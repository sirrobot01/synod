package postgres

import (
	"errors"
	"net/netip"
	"slices"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// PatroniMemberSettings binds typed container input to the command's owned native member.
func PatroniMemberSettings(input *agentv1.EnsureContainer, installation, cluster, resource string) (PatroniSettings, error) {
	var settings PatroniSettings
	if input == nil || input.Engine != "postgres" || input.Postgres == nil || input.Postgres.Patroni == nil || input.Postgres.ReplicaSource != nil {
		return settings, errors.New("Patroni container configuration is invalid")
	}
	config, native := input.Postgres, input.Postgres.Patroni
	if config.Port > 65535 || native.RestPort > 65535 {
		return settings, errors.New("Patroni container port is invalid")
	}
	for _, identity := range []struct {
		value  string
		target *uuid.UUID
	}{
		{installation, &settings.InstallationID}, {cluster, &settings.ClusterID}, {resource, &settings.ResourceID},
		{input.VolumeId, &settings.VolumeID}, {native.ProvisionId, &settings.ProvisionID},
		{config.ApplicationCredentialId, &settings.ApplicationCredentialID},
	} {
		value, err := uuid.Parse(identity.value)
		if err != nil || value == uuid.Nil || value.String() != identity.value {
			return PatroniSettings{}, errors.New("Patroni container identity is invalid")
		}
		*identity.target = value
	}
	address, err := netip.ParseAddr(native.Address)
	if err != nil || address.String() != native.Address {
		return PatroniSettings{}, errors.New("Patroni container address is invalid")
	}
	settings.Address, settings.DatabasePort, settings.RESTPort = address, uint16(config.Port), uint16(native.RestPort)
	settings.ApplicationCredentialVersion = config.ApplicationCredentialVersion
	settings.AllowInitialization, settings.Synchronous = native.AllowInitialization, config.SynchronousReplication
	settings.RequiredFreeBytes = native.RequiredFreeBytes
	settings.DCSEndpoints = slices.Clone(native.DcsEndpoints)
	for _, networks := range []struct {
		values []string
		target *[]netip.Prefix
	}{{native.PeerNetworks, &settings.PeerNetworks}, {native.ClientNetworks, &settings.ClientNetworks}} {
		if len(networks.values) > 32 {
			return PatroniSettings{}, errors.New("Patroni container has too many networks")
		}
		for _, value := range networks.values {
			prefix, err := netip.ParsePrefix(value)
			if err != nil || prefix.String() != value {
				return PatroniSettings{}, errors.New("Patroni container network is invalid")
			}
			*networks.target = append(*networks.target, prefix)
		}
	}
	if err := ValidatePatroniSettings(settings); err != nil {
		return PatroniSettings{}, err
	}
	return settings, nil
}
