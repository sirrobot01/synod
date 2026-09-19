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
	AgentListen      string `json:"agent_listen"`
	AgentTLSCertFile string `json:"agent_tls_cert_file"`
	AgentTLSKeyFile  string `json:"agent_tls_key_file"`
	AgentCAFile      string `json:"agent_ca_file"`
	InstallationID   string `json:"installation_id"`
	Listen           string `json:"listen"`
	DatabaseURL      string `json:"database_url"`
	AdminTokenFile   string `json:"admin_token_file"`
	ActorID          string `json:"actor_id"`
	TLSCertFile      string `json:"tls_cert_file"`
	TLSKeyFile       string `json:"tls_key_file"`
	CACertFile       string `json:"ca_cert_file"`
	CAKeyFile        string `json:"ca_key_file"`
	EtcdEndpoints    string `json:"etcd_endpoints"`
	PublicOrigin     string `json:"public_origin"`
	EtcdPrefix       string `json:"etcd_prefix"`
	LogLevel         string `json:"log_level"`
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
	flags.StringVar(&override.EtcdEndpoints, "etcd-endpoints", "", "comma-separated etcd endpoints that elect one cluster owner")
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
		if err := decoder.Decode(&config); err != nil {
			return Controller{}, errors.New("controller configuration must be a valid JSON object with known fields")
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
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
		{"SYNOD_PUBLIC_ORIGIN", &config.PublicOrigin},
		{"SYNOD_ETCD_PREFIX", &config.EtcdPrefix},
		{"SYNOD_LOG_LEVEL", &config.LogLevel},
	} {
		if value, present := os.LookupEnv(field.name); present {
			*field.value = value
		}
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
		case "ca-key-file":
			config.CAKeyFile = override.CAKeyFile
		case "etcd-endpoints":
			config.EtcdEndpoints = override.EtcdEndpoints
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
		return Controller{}, errors.New("listen must contain a host and port")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 0 || portNumber > 65535 {
		return Controller{}, errors.New("listen port must be between 0 and 65535")
	}
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
			return Controller{}, errors.New("agent_listen must contain a host and port")
		}
		port, err := strconv.Atoi(agentPort)
		if err != nil || port < 0 || port > 65535 {
			return Controller{}, errors.New("agent_listen port must be between 0 and 65535")
		}
	}
	if config.EtcdEndpoints != "" {
		if config.AgentListen == "" {
			return Controller{}, errors.New("etcd_endpoints requires agent_listen, because a worker dispatches over the agent transport")
		}
		for _, endpoint := range strings.Split(config.EtcdEndpoints, ",") {
			if _, port, err := net.SplitHostPort(strings.TrimSpace(endpoint)); err != nil {
				return Controller{}, errors.New("each etcd endpoint must contain a host and port")
			} else if number, err := strconv.Atoi(port); err != nil || number < 1 || number > 65535 {
				return Controller{}, errors.New("each etcd endpoint port must be between 1 and 65535")
			}
		}
		// The prefix only matters to an election, so an unused value never blocks startup.
		if !strings.HasPrefix(config.EtcdPrefix, "/") || strings.HasSuffix(config.EtcdPrefix, "/") || strings.Contains(config.EtcdPrefix, "//") {
			return Controller{}, errors.New("etcd_prefix must be an absolute key prefix without a trailing slash")
		}
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
