package postgres

import (
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const PatroniDirectory = "/run/synod-patroni"
const PatroniDataDirectory = DataDirectory + "/patroni"

// PatroniGuardProgram is the fixed storage guard installed in the member's private tmpfs.
//
//go:embed patroni_guard.py
var PatroniGuardProgram string

// PatroniApplicationProgram installs and verifies the cluster's restricted application access.
//
//go:embed patroni_application.py
var PatroniApplicationProgram string

// PatroniSettings names one member and its declared network access.
type PatroniSettings struct {
	InstallationID               uuid.UUID
	ClusterID                    uuid.UUID
	ResourceID                   uuid.UUID
	VolumeID                     uuid.UUID
	ProvisionID                  uuid.UUID
	AllowInitialization          bool
	RequiredFreeBytes            int64
	Address                      netip.Addr
	DatabasePort                 uint16
	RESTPort                     uint16
	DCSEndpoints                 []string
	PeerNetworks                 []netip.Prefix
	ClientNetworks               []netip.Prefix
	Synchronous                  bool
	ApplicationCredentialID      uuid.UUID
	ApplicationCredentialVersion int64
}

// PatroniSecrets must come from the member's authorized credential versions.
type PatroniSecrets struct {
	Superuser           string
	Replication         string
	Rewind              string
	DCS                 string
	REST                string
	ApplicationVerifier string
}

type patroniLogin struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	SSLMode     string `json:"sslmode,omitempty"`
	SSLRootCert string `json:"sslrootcert,omitempty"`
}

type patroniParameters struct {
	SSL                   string `json:"ssl"`
	SSLCertFile           string `json:"ssl_cert_file"`
	SSLKeyFile            string `json:"ssl_key_file"`
	SSLMinProtocolVersion string `json:"ssl_min_protocol_version"`
	PasswordEncryption    string `json:"password_encryption"`
	WALLevel              string `json:"wal_level"`
	HotStandby            string `json:"hot_standby"`
	WALLogHints           string `json:"wal_log_hints"`
	MaxWALSenders         int    `json:"max_wal_senders"`
	MaxReplicationSlots   int    `json:"max_replication_slots"`
	MaxSlotWALKeepSize    string `json:"max_slot_wal_keep_size"`
	WALKeepSize           string `json:"wal_keep_size"`
	MaxConnections        int    `json:"max_connections"`
	SharedBuffers         string `json:"shared_buffers"`
}

type patroniConfiguration struct {
	Synod struct {
		InstallationID               string   `json:"installation_id"`
		ClusterID                    string   `json:"cluster_id"`
		ResourceID                   string   `json:"resource_id"`
		VolumeID                     string   `json:"volume_id"`
		ProvisionID                  string   `json:"provision_id"`
		AllowInitialization          bool     `json:"allow_initialization"`
		RequiredFreeBytes            int64    `json:"required_free_bytes"`
		PeerNetworks                 []string `json:"peer_networks"`
		ApplicationCredentialID      string   `json:"application_credential_id"`
		ApplicationCredentialVersion int64    `json:"application_credential_version"`
		ApplicationVerifier          string   `json:"application_verifier"`
	} `json:"synod"`
	Scope     string `json:"scope"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	RESTAPI   struct {
		Listen         string       `json:"listen"`
		ConnectAddress string       `json:"connect_address"`
		Authentication patroniLogin `json:"authentication"`
		CertFile       string       `json:"certfile"`
		KeyFile        string       `json:"keyfile"`
		CAFile         string       `json:"cafile"`
		VerifyClient   string       `json:"verify_client"`
	} `json:"restapi"`
	CTL struct {
		CACert   string `json:"cacert"`
		CertFile string `json:"certfile"`
		KeyFile  string `json:"keyfile"`
		Insecure bool   `json:"insecure"`
	} `json:"ctl"`
	Etcd3 struct {
		Hosts      []string `json:"hosts"`
		Protocol   string   `json:"protocol"`
		Username   string   `json:"username"`
		Password   string   `json:"password"`
		CACert     string   `json:"cacert"`
		Cert       string   `json:"cert"`
		Key        string   `json:"key"`
		UseProxies bool     `json:"use_proxies"`
	} `json:"etcd3"`
	Bootstrap struct {
		PostBootstrap string `json:"post_bootstrap"`
		Method        string `json:"method"`
		SynodGuard    struct {
			Command  string `json:"command"`
			NoParams bool   `json:"no_params"`
		} `json:"synod_guard"`
		DCS struct {
			TTL                   int    `json:"ttl"`
			LoopWait              int    `json:"loop_wait"`
			RetryTimeout          int    `json:"retry_timeout"`
			MaximumLagOnFailover  int    `json:"maximum_lag_on_failover"`
			FailsafeMode          bool   `json:"failsafe_mode"`
			CheckTimeline         bool   `json:"check_timeline"`
			SynchronousMode       string `json:"synchronous_mode"`
			SynchronousModeStrict bool   `json:"synchronous_mode_strict"`
			SynchronousNodeCount  int    `json:"synchronous_node_count"`
			PostgreSQL            struct {
				UsePGRewind bool `json:"use_pg_rewind"`
				UseSlots    bool `json:"use_slots"`
			} `json:"postgresql"`
		} `json:"dcs"`
	} `json:"bootstrap"`
	PostgreSQL struct {
		CreateReplicaMethods []string `json:"create_replica_methods"`
		SynodGuard           struct {
			Command  string `json:"command"`
			KeepData bool   `json:"keep_data"`
		} `json:"synod_guard"`
		Listen         string `json:"listen"`
		ConnectAddress string `json:"connect_address"`
		DataDir        string `json:"data_dir"`
		BinDir         string `json:"bin_dir"`
		PGPass         string `json:"pgpass"`
		Authentication struct {
			Superuser   patroniLogin `json:"superuser"`
			Replication patroniLogin `json:"replication"`
			Rewind      patroniLogin `json:"rewind"`
		} `json:"authentication"`
		Parameters                             patroniParameters `json:"parameters"`
		PGHBA                                  []string          `json:"pg_hba"`
		RemoveDataDirectoryOnRewindFailure     bool              `json:"remove_data_directory_on_rewind_failure"`
		RemoveDataDirectoryOnDivergedTimelines bool              `json:"remove_data_directory_on_diverged_timelines"`
	} `json:"postgresql"`
	Watchdog struct {
		Mode string `json:"mode"`
	} `json:"watchdog"`
}

// RenderPatroni returns secret-bearing JSON for a private tmpfs file, never operation metadata.
func RenderPatroni(settings PatroniSettings, secrets PatroniSecrets) ([]byte, error) {
	if err := ValidatePatroniSettings(settings); err != nil {
		return nil, err
	}
	verifier, ok := strings.CutPrefix(secrets.ApplicationVerifier, "SCRAM-SHA-256$4096:")
	salt, keys, separated := strings.Cut(verifier, "$")
	stored, server, paired := strings.Cut(keys, ":")
	if !ok || !separated || !paired || len(secrets.ApplicationVerifier) > 256 {
		return nil, errors.New("Patroni requires a supported application verifier")
	}
	for index, part := range []string{salt, stored, server} {
		decoded, err := base64.StdEncoding.Strict().DecodeString(part)
		length := 32
		if index == 0 {
			length = 16
		}
		if err != nil || len(decoded) != length || base64.StdEncoding.EncodeToString(decoded) != part {
			return nil, errors.New("Patroni requires a canonical application verifier")
		}
	}
	seen := make(map[string]bool)
	for _, secret := range []string{secrets.Superuser, secrets.Replication, secrets.Rewind, secrets.DCS, secrets.REST} {
		if len(secret) < 32 || len(secret) > 256 || seen[secret] {
			return nil, errors.New("Patroni requires distinct credentials of 32 to 256 visible ASCII bytes")
		}
		for _, value := range []byte(secret) {
			if value < 33 || value > 126 {
				return nil, errors.New("Patroni credentials must use visible ASCII bytes")
			}
		}
		seen[secret] = true
	}
	var config patroniConfiguration
	config.Synod.InstallationID, config.Synod.ClusterID, config.Synod.ResourceID = settings.InstallationID.String(), settings.ClusterID.String(), settings.ResourceID.String()
	config.Synod.VolumeID, config.Synod.ProvisionID = settings.VolumeID.String(), settings.ProvisionID.String()
	config.Synod.AllowInitialization = settings.AllowInitialization
	config.Synod.RequiredFreeBytes = settings.RequiredFreeBytes
	config.Synod.ApplicationCredentialID = settings.ApplicationCredentialID.String()
	config.Synod.ApplicationCredentialVersion = settings.ApplicationCredentialVersion
	config.Synod.ApplicationVerifier = secrets.ApplicationVerifier
	for _, network := range settings.PeerNetworks {
		config.Synod.PeerNetworks = append(config.Synod.PeerNetworks, network.String())
	}
	config.Scope = settings.ClusterID.String()
	config.Namespace = "/synod-patroni/" + settings.InstallationID.String() + "/"
	config.Name = settings.ResourceID.String()
	address := settings.Address.String()
	config.RESTAPI.Listen = net.JoinHostPort(address, strconv.Itoa(int(settings.RESTPort)))
	config.RESTAPI.ConnectAddress = config.RESTAPI.Listen
	config.RESTAPI.Authentication = patroniLogin{Username: "synod_patroni", Password: secrets.REST}
	config.RESTAPI.CertFile, config.RESTAPI.KeyFile, config.RESTAPI.CAFile = PatroniDirectory+"/member.crt", PatroniDirectory+"/member.key", PatroniDirectory+"/ca.crt"
	config.RESTAPI.VerifyClient = "required"
	config.CTL.CACert, config.CTL.CertFile, config.CTL.KeyFile = config.RESTAPI.CAFile, config.RESTAPI.CertFile, config.RESTAPI.KeyFile
	config.Etcd3.Hosts = slices.Clone(settings.DCSEndpoints)
	config.Etcd3.Protocol, config.Etcd3.UseProxies = "https", true
	config.Etcd3.Username, config.Etcd3.Password = "synod_patroni_"+settings.ClusterID.String(), secrets.DCS
	config.Etcd3.CACert, config.Etcd3.Cert, config.Etcd3.Key = PatroniDirectory+"/dcs-ca.crt", PatroniDirectory+"/dcs-client.crt", PatroniDirectory+"/dcs-client.key"
	config.Bootstrap.DCS.TTL, config.Bootstrap.DCS.LoopWait, config.Bootstrap.DCS.RetryTimeout = 20, 2, 3
	config.Bootstrap.DCS.MaximumLagOnFailover, config.Bootstrap.DCS.CheckTimeline = 1<<20, true
	config.Bootstrap.DCS.SynchronousMode = "off"
	if settings.Synchronous {
		config.Bootstrap.DCS.SynchronousMode = "on"
	}
	config.Bootstrap.DCS.SynchronousModeStrict, config.Bootstrap.DCS.SynchronousNodeCount = settings.Synchronous, 1
	config.Bootstrap.DCS.PostgreSQL.UsePGRewind, config.Bootstrap.DCS.PostgreSQL.UseSlots = true, true
	config.Bootstrap.Method = "synod_guard"
	config.Bootstrap.PostBootstrap = "/usr/bin/python3 " + PatroniDirectory + "/application.py bootstrap"
	config.Bootstrap.SynodGuard.Command = "/usr/bin/python3 " + PatroniDirectory + "/guard.py init"
	config.Bootstrap.SynodGuard.NoParams = true
	config.PostgreSQL.CreateReplicaMethods = []string{"synod_guard"}
	config.PostgreSQL.SynodGuard.Command = "/usr/bin/python3 " + PatroniDirectory + "/guard.py clone"
	config.PostgreSQL.SynodGuard.KeepData = true
	// The agent proves readiness on loopback with TLS for the member IP. Patroni accepts
	// several listen addresses followed by one port. Patroni 4.1.5 switches its own local
	// connection to localhost when the list holds 127.0.0.1, and that fails verify-full
	// against the member certificate. ReadinessLoopback never returns 127.0.0.1, and it keeps the member IP first.
	loopback := ReadinessLoopback(settings.Address)
	port := strconv.Itoa(int(settings.DatabasePort))
	config.PostgreSQL.Listen = address + "," + net.JoinHostPort(loopback.Addr().String(), port)
	config.PostgreSQL.ConnectAddress = net.JoinHostPort(address, port)
	config.PostgreSQL.DataDir, config.PostgreSQL.BinDir, config.PostgreSQL.PGPass = PatroniDataDirectory, "/usr/local/bin", PatroniDirectory+"/pgpass"
	config.PostgreSQL.Authentication.Superuser = patroniLogin{Username: "synod_admin", Password: secrets.Superuser, SSLMode: "verify-full", SSLRootCert: config.RESTAPI.CAFile}
	config.PostgreSQL.Authentication.Replication = patroniLogin{Username: "synod_repl", Password: secrets.Replication, SSLMode: "verify-full", SSLRootCert: config.RESTAPI.CAFile}
	config.PostgreSQL.Authentication.Rewind = patroniLogin{Username: "synod_rewind", Password: secrets.Rewind, SSLMode: "verify-full", SSLRootCert: config.RESTAPI.CAFile}
	config.PostgreSQL.Parameters = patroniParameters{SSL: "on", SSLCertFile: config.RESTAPI.CertFile, SSLKeyFile: config.RESTAPI.KeyFile, SSLMinProtocolVersion: "TLSv1.2", PasswordEncryption: "scram-sha-256", WALLevel: "replica", HotStandby: "on", WALLogHints: "on", MaxWALSenders: 10, MaxReplicationSlots: 10, MaxSlotWALKeepSize: "1GB", WALKeepSize: "256MB", MaxConnections: 64, SharedBuffers: "32MB"}
	config.PostgreSQL.PGHBA = []string{"local all synod_admin trust", "local all all reject", "hostnossl all all 0.0.0.0/0 reject", "hostnossl all all ::/0 reject", "hostnossl replication all 0.0.0.0/0 reject", "hostnossl replication all ::/0 reject",
		"hostssl all synod_admin,synod_app " + loopbackSources(settings.Address).String() + " scram-sha-256"}
	for _, network := range settings.PeerNetworks {
		config.PostgreSQL.PGHBA = append(config.PostgreSQL.PGHBA, "hostssl replication synod_repl "+network.String()+" scram-sha-256", "hostssl all synod_admin,synod_rewind "+network.String()+" scram-sha-256")
	}
	config.PostgreSQL.PGHBA = append(config.PostgreSQL.PGHBA, "host all synod_admin,synod_repl,synod_rewind 0.0.0.0/0 reject", "host all synod_admin,synod_repl,synod_rewind ::/0 reject")
	for _, network := range settings.ClientNetworks {
		config.PostgreSQL.PGHBA = append(config.PostgreSQL.PGHBA, "hostssl all all "+network.String()+" scram-sha-256")
	}
	config.Watchdog.Mode = "off"
	return json.Marshal(config)
}

// ValidatePatroniSettings checks the native member identity and declared network boundaries.
func ValidatePatroniSettings(settings PatroniSettings) error {
	if settings.RequiredFreeBytes <= 0 {
		return errors.New("Patroni requires a positive initial storage allowance")
	}
	if settings.ApplicationCredentialID == uuid.Nil || settings.ApplicationCredentialVersion <= 0 {
		return errors.New("Patroni requires an application credential identity and version")
	}
	if settings.InstallationID == uuid.Nil || settings.ClusterID == uuid.Nil || settings.ResourceID == uuid.Nil || settings.VolumeID == uuid.Nil || settings.ProvisionID == uuid.Nil {
		return errors.New("Patroni requires installation, cluster, resource, volume, and provision identities")
	}
	if !settings.Address.IsGlobalUnicast() || settings.Address.Zone() != "" || settings.Address.Is4In6() || settings.Address.IsLoopback() || settings.DatabasePort == 0 || settings.RESTPort == 0 || settings.DatabasePort == settings.RESTPort {
		return errors.New("Patroni requires a routable member address and distinct database and REST ports")
	}
	if len(settings.DCSEndpoints) < 1 || len(settings.DCSEndpoints) > 8 {
		return errors.New("Patroni requires one to eight DCS endpoints")
	}
	seen := make(map[string]bool)
	for _, endpoint := range settings.DCSEndpoints {
		host, port, err := net.SplitHostPort(endpoint)
		number, parseErr := strconv.ParseUint(port, 10, 16)
		if err != nil || parseErr != nil || number == 0 || host == "" || len(endpoint) > 320 || endpoint != net.JoinHostPort(host, port) {
			return errors.New("Patroni DCS endpoints must be distinct plain host and port pairs")
		}
		if address, err := netip.ParseAddr(host); err == nil {
			if !address.IsGlobalUnicast() || address.IsLoopback() || address.Is4In6() || address.Zone() != "" {
				return errors.New("Patroni DCS requires routable addresses")
			}
			host = address.String()
		} else {
			host = strings.ToLower(strings.TrimSuffix(host, "."))
			if len(host) > 253 {
				return errors.New("Patroni DCS requires valid DNS names or IP addresses")
			}
			for label := range strings.SplitSeq(host, ".") {
				if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
					return errors.New("Patroni DCS requires valid DNS names or IP addresses")
				}
				for _, value := range []byte(label) {
					if !(value >= 'a' && value <= 'z' || value >= '0' && value <= '9' || value == '-') {
						return errors.New("Patroni DCS requires valid DNS names or IP addresses")
					}
				}
			}
		}
		canonical := net.JoinHostPort(host, strconv.FormatUint(number, 10))
		if seen[canonical] {
			return errors.New("Patroni DCS endpoints must be distinct")
		}
		seen[canonical] = true
	}
	for _, networks := range [][]netip.Prefix{settings.PeerNetworks, settings.ClientNetworks} {
		if len(networks) < 1 || len(networks) > 32 {
			return errors.New("Patroni requires one to 32 declared peer and client networks")
		}
		clear(seen)
		for _, network := range networks {
			if !network.IsValid() || network != network.Masked() || network.Addr().Is4In6() || seen[network.String()] {
				return errors.New("Patroni networks must be distinct canonical IP prefixes")
			}
			seen[network.String()] = true
		}
	}
	return nil
}

// ReadinessLoopback returns the loopback prefix where a Patroni member also listens for agent readiness.
//
// An IPv4 member a.b.c.d uses 127.b.c.d, so members on one host with different addresses do not
// share a loopback listener. 127.0.0.1 and the loopback broadcast address are never used, so those
// two cases use 127.0.0.2. IPv6 has only ::1, so two IPv6 members on one host need different ports.
func ReadinessLoopback(member netip.Addr) netip.Prefix {
	if member.Is6() {
		return netip.MustParsePrefix("::1/128")
	}
	octets := member.As4()
	loopback := netip.AddrFrom4([4]byte{127, octets[1], octets[2], octets[3]})
	if loopback == netip.MustParseAddr("127.0.0.1") || loopback == netip.MustParseAddr("127.255.255.255") {
		loopback = netip.MustParseAddr("127.0.0.2")
	}
	return netip.PrefixFrom(loopback, 32)
}

// loopbackSources is where a local readiness connection comes from. Linux sends from
// 127.0.0.1 to any 127.0.0.0/8 destination, and only local processes use that range.
func loopbackSources(member netip.Addr) netip.Prefix {
	if member.Is6() {
		return netip.MustParsePrefix("::1/128")
	}
	return netip.MustParsePrefix("127.0.0.0/8")
}
