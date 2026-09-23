package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Controller holds the resolved controller settings.
type Controller struct {
	AgentListen        string `json:"agent_listen"`
	AgentTLSCertFile   string `json:"agent_tls_cert_file"`
	AgentTLSKeyFile    string `json:"agent_tls_key_file"`
	AgentCAFile        string `json:"agent_ca_file"`
	InstallationID     string `json:"installation_id"`
	Listen             string `json:"listen"`
	DatabaseURL        string `json:"database_url"`
	AdminTokenFile     string `json:"admin_token_file"`
	ActorID            string `json:"actor_id"`
	TLSCertFile        string `json:"tls_cert_file"`
	TLSKeyFile         string `json:"tls_key_file"`
	CACertFile         string `json:"ca_cert_file"`
	CAKeyFile          string `json:"ca_key_file"`
	EtcdEndpoints      string `json:"etcd_endpoints"`
	EtcdCAFile         string `json:"etcd_ca_file"`
	EtcdTLSCertFile    string `json:"etcd_tls_cert_file"`
	EtcdTLSKeyFile     string `json:"etcd_tls_key_file"`
	EtcdUsername       string `json:"etcd_username"`
	EtcdPasswordFile   string `json:"etcd_password_file"`
	EtcdAllowPlaintext bool   `json:"etcd_allow_plaintext,omitzero"`
	PublicOrigin       string `json:"public_origin"`
	EtcdPrefix         string `json:"etcd_prefix"`
	LogLevel           string `json:"log_level"`
	// SecretKeyFile holds the base64 of one 32-byte wrapping key, and SecretKeyID
	// names it. A controller without both seals nothing, so it admits clusters and
	// cannot provision them: a credential this installation cannot name later is
	// worse than a provision that did not start.
	SecretKeyFile string `json:"secret_key_file"`
	SecretKeyID   string `json:"secret_key_id"`
	// The node shape this installation places onto. A cluster spec names none of it
	// and placement matches a node's operator-declared policy exactly, so it is
	// configuration: a controller on one architecture routinely manages nodes on
	// another, and a compile-time default cannot describe a real fleet.
	PlacementArchitecture   string `json:"placement_architecture"`
	PlacementRuntimeVersion string `json:"placement_runtime_version"`
	PlacementStorageID      string `json:"placement_storage_id"`
	// The host ports an engine may publish on. This profile uses host networking, so
	// a port is a host resource the plan reserves, and placement chooses one from this
	// span. A node carries as many clusters as the span has free ports.
	EngineEndpointFirstPort int `json:"engine_endpoint_first_port"`
	EngineEndpointLastPort  int `json:"engine_endpoint_last_port"`
}

// LoadController resolves settings from flags, the environment, and a JSON file.
// A flag overrides an environment variable, which overrides the file.
func LoadController(args []string, output io.Writer) (Controller, error) {
	config := Controller{Listen: "127.0.0.1:8443", ActorID: "bootstrap-admin", EtcdPrefix: "/synod", LogLevel: "info"}
	override := config
	flags := flag.NewFlagSet("synodd", flag.ContinueOnError)
	flags.SetOutput(output)
	path := flags.String("config", os.Getenv("SYNOD_CONFIG"), "JSON configuration file")
	flags.StringVar(&override.AgentListen, "agent-listen", "", "optional authenticated agent gRPC listen address")
	flags.StringVar(&override.AgentTLSCertFile, "agent-tls-cert-file", "", "controller identity certificate for agent transport")
	flags.StringVar(&override.AgentTLSKeyFile, "agent-tls-key-file", "", "protected controller identity private key")
	flags.StringVar(&override.AgentCAFile, "agent-ca-file", "", "installation CA trust bundle for agent transport")
	flags.StringVar(&override.InstallationID, "installation-id", "", "stable installation UUID")
	flags.StringVar(&override.Listen, "listen", config.Listen, "HTTPS listen address")
	flags.StringVar(&override.DatabaseURL, "database-url", "", "metadata PostgreSQL URL")
	flags.StringVar(&override.AdminTokenFile, "admin-token-file", "", "protected administrator token file")
	flags.StringVar(&override.ActorID, "actor-id", config.ActorID, "administrator audit identity")
	flags.StringVar(&override.TLSCertFile, "tls-cert-file", "", "API TLS certificate file")
	flags.StringVar(&override.TLSKeyFile, "tls-key-file", "", "API TLS private key file")
	flags.StringVar(&override.CACertFile, "ca-cert-file", "", "installation enrollment CA certificate file")
	flags.StringVar(&override.CAKeyFile, "ca-key-file", "", "protected enrollment CA private key file")
	flags.StringVar(&override.SecretKeyFile, "secret-key-file", "", "protected file holding the base64 of one 32-byte credential wrapping key")
	flags.StringVar(&override.SecretKeyID, "secret-key-id", "", "UUID naming the active credential wrapping key")
	flags.StringVar(&override.PlacementArchitecture, "placement-architecture", "", "node CPU architecture to place onto, amd64 or arm64")
	flags.StringVar(&override.PlacementRuntimeVersion, "placement-runtime-version", "", "container runtime version the nodes report")
	flags.StringVar(&override.PlacementStorageID, "placement-storage-id", "", "storage pool identifier the nodes declare")
	flags.IntVar(&override.EngineEndpointFirstPort, "engine-endpoint-first-port", 0, "lowest host port an engine may publish on (default 5432)")
	flags.IntVar(&override.EngineEndpointLastPort, "engine-endpoint-last-port", 0, "highest host port an engine may publish on (default 5531)")
	flags.StringVar(&override.EtcdEndpoints, "etcd-endpoints", "", "comma-separated etcd endpoints that elect one cluster owner")
	flags.StringVar(&override.EtcdCAFile, "etcd-ca-file", "", "CA trust bundle for coordination TLS")
	flags.StringVar(&override.EtcdTLSCertFile, "etcd-tls-cert-file", "", "optional coordination client certificate")
	flags.StringVar(&override.EtcdTLSKeyFile, "etcd-tls-key-file", "", "protected coordination client private key")
	flags.StringVar(&override.EtcdUsername, "etcd-username", "", "coordination account with access to this installation prefix")
	flags.StringVar(&override.EtcdPasswordFile, "etcd-password-file", "", "protected coordination account password file")
	flags.BoolVar(&override.EtcdAllowPlaintext, "etcd-allow-plaintext", false, "allow unauthenticated plaintext coordination for development")
	flags.StringVar(&override.PublicOrigin, "public-origin", "", "browser origin of this API, such as https://synod.example.com")
	flags.StringVar(&override.EtcdPrefix, "etcd-prefix", config.EtcdPrefix, "absolute etcd key prefix for elections")
	flags.StringVar(&override.LogLevel, "log-level", config.LogLevel, "debug, info, warn, or error")
	if err := flags.Parse(args); err != nil {
		return Controller{}, err
	}
	if flags.NArg() != 0 {
		return Controller{}, errors.New("unexpected controller arguments")
	}
	if *path != "" {
		file, err := os.Open(*path)
		if err != nil {
			return Controller{}, fmt.Errorf("open controller configuration: %w", err)
		}
		data, readErr := io.ReadAll(io.LimitReader(file, (1<<20)+1))
		closeErr := file.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return Controller{}, fmt.Errorf("read controller configuration: %w", err)
		}
		if len(data) > 1<<20 {
			return Controller{}, errors.New("controller configuration exceeds 1 MiB")
		}
		if trimmed := bytes.TrimSpace(data); len(trimmed) == 0 || trimmed[0] != '{' {
			return Controller{}, errors.New("controller configuration must be a JSON object")
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		// DisallowUnknownFields makes the decoder name the unknown key, or the offset
		// the syntax broke at. That is the whole value of the check to an operator.
		if err := decoder.Decode(&config); err != nil {
			return Controller{}, fmt.Errorf("controller configuration must be a valid JSON object with known fields: %w", err)
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			if err != nil {
				return Controller{}, fmt.Errorf("controller configuration must contain one JSON object: %w", err)
			}
			return Controller{}, errors.New("controller configuration must contain one JSON object")
		}
	}
	for _, field := range []struct {
		name  string
		value *string
	}{
		{"SYNOD_AGENT_LISTEN", &config.AgentListen},
		{"SYNOD_AGENT_TLS_CERT_FILE", &config.AgentTLSCertFile},
		{"SYNOD_AGENT_TLS_KEY_FILE", &config.AgentTLSKeyFile},
		{"SYNOD_AGENT_CA_FILE", &config.AgentCAFile},
		{"SYNOD_INSTALLATION_ID", &config.InstallationID},
		{"SYNOD_LISTEN", &config.Listen},
		{"SYNOD_DATABASE_URL", &config.DatabaseURL},
		{"SYNOD_ADMIN_TOKEN_FILE", &config.AdminTokenFile},
		{"SYNOD_ACTOR_ID", &config.ActorID},
		{"SYNOD_TLS_CERT_FILE", &config.TLSCertFile},
		{"SYNOD_TLS_KEY_FILE", &config.TLSKeyFile},
		{"SYNOD_CA_CERT_FILE", &config.CACertFile},
		{"SYNOD_CA_KEY_FILE", &config.CAKeyFile},
		{"SYNOD_ETCD_ENDPOINTS", &config.EtcdEndpoints},
		{"SYNOD_ETCD_CA_FILE", &config.EtcdCAFile},
		{"SYNOD_ETCD_TLS_CERT_FILE", &config.EtcdTLSCertFile},
		{"SYNOD_ETCD_TLS_KEY_FILE", &config.EtcdTLSKeyFile},
		{"SYNOD_ETCD_USERNAME", &config.EtcdUsername},
		{"SYNOD_ETCD_PASSWORD_FILE", &config.EtcdPasswordFile},
		{"SYNOD_PUBLIC_ORIGIN", &config.PublicOrigin},
		{"SYNOD_ETCD_PREFIX", &config.EtcdPrefix},
		{"SYNOD_LOG_LEVEL", &config.LogLevel},
		{"SYNOD_SECRET_KEY_FILE", &config.SecretKeyFile},
		{"SYNOD_SECRET_KEY_ID", &config.SecretKeyID},
		{"SYNOD_PLACEMENT_ARCHITECTURE", &config.PlacementArchitecture},
		{"SYNOD_PLACEMENT_RUNTIME_VERSION", &config.PlacementRuntimeVersion},
		{"SYNOD_PLACEMENT_STORAGE_ID", &config.PlacementStorageID},
	} {
		if value, present := os.LookupEnv(field.name); present {
			*field.value = value
		}
	}
	for _, field := range []struct {
		name  string
		value *int
	}{
		{"SYNOD_ENGINE_ENDPOINT_FIRST_PORT", &config.EngineEndpointFirstPort},
		{"SYNOD_ENGINE_ENDPOINT_LAST_PORT", &config.EngineEndpointLastPort},
	} {
		if value, present := os.LookupEnv(field.name); present {
			number, err := strconv.Atoi(value)
			if err != nil {
				return Controller{}, fmt.Errorf("%s must be a number: %w", field.name, err)
			}
			*field.value = number
		}
	}
	if value, present := os.LookupEnv("SYNOD_ETCD_ALLOW_PLAINTEXT"); present {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Controller{}, errors.New("SYNOD_ETCD_ALLOW_PLAINTEXT must be a boolean")
		}
		config.EtcdAllowPlaintext = parsed
	}
	flags.Visit(func(value *flag.Flag) {
		switch value.Name {
		case "agent-listen":
			config.AgentListen = override.AgentListen
		case "agent-tls-cert-file":
			config.AgentTLSCertFile = override.AgentTLSCertFile
		case "agent-tls-key-file":
			config.AgentTLSKeyFile = override.AgentTLSKeyFile
		case "agent-ca-file":
			config.AgentCAFile = override.AgentCAFile
		case "installation-id":
			config.InstallationID = override.InstallationID
		case "listen":
			config.Listen = override.Listen
		case "database-url":
			config.DatabaseURL = override.DatabaseURL
		case "admin-token-file":
			config.AdminTokenFile = override.AdminTokenFile
		case "actor-id":
			config.ActorID = override.ActorID
		case "tls-cert-file":
			config.TLSCertFile = override.TLSCertFile
		case "tls-key-file":
			config.TLSKeyFile = override.TLSKeyFile
		case "ca-cert-file":
			config.CACertFile = override.CACertFile
		case "placement-architecture":
			config.PlacementArchitecture = override.PlacementArchitecture
		case "placement-runtime-version":
			config.PlacementRuntimeVersion = override.PlacementRuntimeVersion
		case "placement-storage-id":
			config.PlacementStorageID = override.PlacementStorageID
		case "engine-endpoint-first-port":
			config.EngineEndpointFirstPort = override.EngineEndpointFirstPort
		case "engine-endpoint-last-port":
			config.EngineEndpointLastPort = override.EngineEndpointLastPort
		case "secret-key-file":
			config.SecretKeyFile = override.SecretKeyFile
		case "secret-key-id":
			config.SecretKeyID = override.SecretKeyID
		case "ca-key-file":
			config.CAKeyFile = override.CAKeyFile
		case "etcd-endpoints":
			config.EtcdEndpoints = override.EtcdEndpoints
		case "etcd-ca-file":
			config.EtcdCAFile = override.EtcdCAFile
		case "etcd-tls-cert-file":
			config.EtcdTLSCertFile = override.EtcdTLSCertFile
		case "etcd-tls-key-file":
			config.EtcdTLSKeyFile = override.EtcdTLSKeyFile
		case "etcd-username":
			config.EtcdUsername = override.EtcdUsername
		case "etcd-password-file":
			config.EtcdPasswordFile = override.EtcdPasswordFile
		case "etcd-allow-plaintext":
			config.EtcdAllowPlaintext = override.EtcdAllowPlaintext
		case "public-origin":
			config.PublicOrigin = override.PublicOrigin
		case "etcd-prefix":
			config.EtcdPrefix = override.EtcdPrefix
		case "log-level":
			config.LogLevel = override.LogLevel
		}
	})
	_, port, err := net.SplitHostPort(config.Listen)
	if err != nil {
		return Controller{}, fmt.Errorf("listen must contain a host and port: %w", err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		return Controller{}, fmt.Errorf("listen port %q must be a number: %w", port, err)
	}
	if portNumber < 0 || portNumber > 65535 {
		return Controller{}, errors.New("listen port must be between 0 and 65535")
	}
	// url.Error repeats the URL it failed on, and a PostgreSQL DSN carries the
	// database password. The cause stays out of this message for that reason, not
	// because it was overlooked.
	database, err := url.Parse(config.DatabaseURL)
	if err != nil || (database.Scheme != "postgres" && database.Scheme != "postgresql") || database.Hostname() == "" {
		return Controller{}, errors.New("database_url must be a PostgreSQL URL with a host")
	}
	if id, err := uuid.Parse(config.InstallationID); err != nil || id == uuid.Nil {
		return Controller{}, errors.New("installation_id must be a nonzero UUID")
	}
	if (config.CACertFile == "") != (config.CAKeyFile == "") {
		return Controller{}, errors.New("ca_cert_file and ca_key_file must be supplied together")
	}
	if (config.SecretKeyFile == "") != (config.SecretKeyID == "") {
		return Controller{}, errors.New("secret_key_file and secret_key_id must be supplied together")
	}
	if config.SecretKeyID != "" {
		if id, err := uuid.Parse(config.SecretKeyID); err != nil || id == uuid.Nil {
			return Controller{}, errors.New("secret_key_id must be a nonzero UUID")
		}
	}
	// A controller that can provision must say what it places onto. Guessing would
	// produce a profile that silently matches no node in the fleet.
	if config.SecretKeyFile != "" {
		if config.PlacementArchitecture != "amd64" && config.PlacementArchitecture != "arm64" {
			return Controller{}, errors.New("placement_architecture must be amd64 or arm64 when provisioning is configured")
		}
		if config.PlacementRuntimeVersion == "" || config.PlacementStorageID == "" {
			return Controller{}, errors.New("placement_runtime_version and placement_storage_id are required when provisioning is configured")
		}
	}
	if config.EngineEndpointFirstPort == 0 {
		config.EngineEndpointFirstPort = 5432
	}
	if config.EngineEndpointLastPort == 0 {
		// A hundred ports is a hundred clusters on one node, which is past what any
		// other limit here allows. An operator narrows it when the host is shared.
		config.EngineEndpointLastPort = config.EngineEndpointFirstPort + 99
	}
	if config.EngineEndpointFirstPort < 1 || config.EngineEndpointLastPort > 65535 || config.EngineEndpointFirstPort > config.EngineEndpointLastPort {
		return Controller{}, errors.New("engine_endpoint_first_port and engine_endpoint_last_port must be a port range")
	}
	agentFields := 0
	for _, value := range []string{config.AgentListen, config.AgentTLSCertFile, config.AgentTLSKeyFile, config.AgentCAFile} {
		if value != "" {
			agentFields++
		}
	}
	if agentFields != 0 && agentFields != 4 {
		return Controller{}, errors.New("agent_listen, agent_tls_cert_file, agent_tls_key_file, and agent_ca_file must be supplied together")
	}
	if config.AgentListen != "" {
		_, agentPort, err := net.SplitHostPort(config.AgentListen)
		if err != nil {
			return Controller{}, fmt.Errorf("agent_listen must contain a host and port: %w", err)
		}
		port, err := strconv.Atoi(agentPort)
		if err != nil {
			return Controller{}, fmt.Errorf("agent_listen port %q must be a number: %w", agentPort, err)
		}
		if port < 0 || port > 65535 {
			return Controller{}, errors.New("agent_listen port must be between 0 and 65535")
		}
	}
	if config.EtcdEndpoints != "" {
		if config.AgentListen == "" {
			return Controller{}, errors.New("etcd_endpoints requires agent_listen, because a worker dispatches over the agent transport")
		}
		endpoints := strings.Split(config.EtcdEndpoints, ",")
		if len(endpoints) > 8 {
			return Controller{}, errors.New("etcd_endpoints permits at most eight endpoints")
		}
		seen := make(map[string]bool)
		for _, endpoint := range endpoints {
			endpoint = strings.TrimSpace(endpoint)
			host, port, err := net.SplitHostPort(endpoint)
			number, parseErr := strconv.ParseUint(port, 10, 16)
			if err != nil || parseErr != nil || number == 0 || host == "" || len(endpoint) > 320 || endpoint != net.JoinHostPort(host, port) {
				return Controller{}, errors.New("etcd_endpoints requires bounded plain host and port pairs without credentials")
			}
			if address := net.ParseIP(host); address != nil {
				if !address.IsGlobalUnicast() && !address.IsLoopback() {
					return Controller{}, errors.New("etcd_endpoints requires unicast addresses")
				}
				host = address.String()
			} else {
				host = strings.ToLower(strings.TrimSuffix(host, "."))
				if len(host) > 253 {
					return Controller{}, errors.New("etcd_endpoints requires bounded DNS names")
				}
				for label := range strings.SplitSeq(host, ".") {
					if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' || strings.ContainsFunc(label, func(r rune) bool {
						return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-')
					}) {
						return Controller{}, errors.New("etcd_endpoints requires valid DNS names or IP addresses")
					}
				}
			}
			canonical := net.JoinHostPort(host, strconv.FormatUint(number, 10))
			if seen[canonical] {
				return Controller{}, errors.New("etcd_endpoints must be distinct")
			}
			seen[canonical] = true
		}
		if config.EtcdAllowPlaintext {
			if config.EtcdCAFile != "" || config.EtcdTLSCertFile != "" || config.EtcdTLSKeyFile != "" || config.EtcdUsername != "" || config.EtcdPasswordFile != "" {
				return Controller{}, errors.New("etcd_allow_plaintext cannot include TLS or authentication settings")
			}
		} else {
			if config.EtcdCAFile == "" || config.EtcdUsername == "" || config.EtcdPasswordFile == "" {
				return Controller{}, errors.New("coordination requires etcd_ca_file, etcd_username, and etcd_password_file; development plaintext requires etcd_allow_plaintext")
			}
			if (config.EtcdTLSCertFile == "") != (config.EtcdTLSKeyFile == "") {
				return Controller{}, errors.New("etcd_tls_cert_file and etcd_tls_key_file must be supplied together")
			}
			if len(config.EtcdUsername) > 128 || strings.ContainsAny(config.EtcdUsername, ":/@\\") || strings.ContainsFunc(config.EtcdUsername, func(r rune) bool { return r < '!' || r > '~' }) {
				return Controller{}, errors.New("etcd_username must be a bounded account name without credentials")
			}
		}
		// The prefix only matters to an election, so an unused value never blocks startup.
		if !strings.HasPrefix(config.EtcdPrefix, "/") || strings.HasSuffix(config.EtcdPrefix, "/") || strings.Contains(config.EtcdPrefix, "//") {
			return Controller{}, errors.New("etcd_prefix must be an absolute key prefix without a trailing slash")
		}
	}
	if config.EtcdEndpoints == "" && (config.EtcdAllowPlaintext || config.EtcdCAFile != "" || config.EtcdTLSCertFile != "" || config.EtcdTLSKeyFile != "" || config.EtcdUsername != "" || config.EtcdPasswordFile != "") {
		return Controller{}, errors.New("coordination security settings require etcd_endpoints")
	}
	if config.PublicOrigin != "" {
		origin, err := url.Parse(config.PublicOrigin)
		if err != nil || (origin.Scheme != "https" && origin.Scheme != "http") || origin.Host == "" || origin.Path != "" || origin.RawQuery != "" {
			return Controller{}, errors.New("public_origin must be a scheme and host with no path")
		}
		// A browser sends a canonical origin: lowercase, with the default port elided.
		// Store that form, because the comparison is exact.
		scheme, host := strings.ToLower(origin.Scheme), strings.ToLower(origin.Host)
		host = strings.TrimSuffix(host, map[string]string{"https": ":443", "http": ":80"}[scheme])
		config.PublicOrigin = scheme + "://" + host
	}
	if config.AdminTokenFile == "" {
		return Controller{}, errors.New("admin_token_file is required")
	}
	if config.TLSCertFile == "" || config.TLSKeyFile == "" {
		return Controller{}, errors.New("tls_cert_file and tls_key_file are required")
	}
	if len(config.ActorID) == 0 || len(config.ActorID) > 256 || strings.ContainsFunc(config.ActorID, func(r rune) bool {
		return r < '!' || r > '~'
	}) {
		return Controller{}, errors.New("actor_id must contain 1 to 256 visible ASCII bytes")
	}
	switch config.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return Controller{}, errors.New("log_level must be debug, info, warn, or error")
	}
	return config, nil
}
