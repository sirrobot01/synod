package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/spf13/pflag"
)

// Controller holds the resolved controller settings.
type Controller struct {
	AgentListen      string `json:"agent_listen"`
	AgentTLSCertFile string `json:"agent_tls_cert_file"`
	AgentTLSKeyFile  string `json:"agent_tls_key_file"`
	AgentCAFile      string `json:"agent_ca_file"`
	InstallationID   string `json:"installation_id"`
	Listen           string `json:"listen"`
	DatabasePath     string `json:"database_path"`
	AdminTokenFile   string `json:"admin_token_file"`
	ActorID          string `json:"actor_id"`
	TLSCertFile      string `json:"tls_cert_file"`
	TLSKeyFile       string `json:"tls_key_file"`
	CACertFile       string `json:"ca_cert_file"`
	CAKeyFile        string `json:"ca_key_file"`
	// APICACertFile and APICAKeyFile let the controller renew its API certificate.
	APICACertFile string `json:"api_ca_cert_file"`
	APICAKeyFile  string `json:"api_ca_key_file"`
	PublicOrigin  string `json:"public_origin"`
	LogLevel      string `json:"log_level"`
	// SecretKeyFile holds the base64 of one 32-byte wrapping key, and SecretKeyID
	// names it. Destination credentials are sealed with it.
	SecretKeyFile string `json:"secret_key_file"`
	SecretKeyID   string `json:"secret_key_id"`
}

// BindController registers the controller flags on flags. The returned function
// resolves the settings after flags are parsed. A flag overrides an environment
// variable, which overrides the JSON file.
func BindController(flags *pflag.FlagSet) func() (Controller, error) {
	defaults := Controller{Listen: "127.0.0.1:8443", ActorID: "bootstrap-admin", LogLevel: "info"}
	override := defaults
	path := flags.String("config", os.Getenv("SYNOD_CONFIG"), "JSON configuration file")
	flags.StringVar(&override.AgentListen, "agent-listen", "", "optional authenticated agent gRPC listen address")
	flags.StringVar(&override.AgentTLSCertFile, "agent-tls-cert-file", "", "controller identity certificate for agent transport")
	flags.StringVar(&override.AgentTLSKeyFile, "agent-tls-key-file", "", "protected controller identity private key")
	flags.StringVar(&override.AgentCAFile, "agent-ca-file", "", "installation CA trust bundle for agent transport")
	flags.StringVar(&override.InstallationID, "installation-id", "", "stable installation UUID")
	flags.StringVar(&override.Listen, "listen", defaults.Listen, "HTTPS listen address")
	flags.StringVar(&override.DatabasePath, "database-path", "", "metadata SQLite database file")
	flags.StringVar(&override.AdminTokenFile, "admin-token-file", "", "protected administrator token file")
	flags.StringVar(&override.ActorID, "actor-id", defaults.ActorID, "administrator audit identity")
	flags.StringVar(&override.TLSCertFile, "tls-cert-file", "", "API TLS certificate file")
	flags.StringVar(&override.TLSKeyFile, "tls-key-file", "", "API TLS private key file")
	flags.StringVar(&override.CACertFile, "ca-cert-file", "", "installation enrollment CA certificate file")
	flags.StringVar(&override.CAKeyFile, "ca-key-file", "", "protected enrollment CA private key file")
	flags.StringVar(&override.SecretKeyFile, "secret-key-file", "", "protected file holding the base64 of one 32-byte credential wrapping key")
	flags.StringVar(&override.SecretKeyID, "secret-key-id", "", "UUID naming the active credential wrapping key")
	flags.StringVar(&override.PublicOrigin, "public-origin", "", "browser origin of this API, such as https://synod.example.com")
	flags.StringVar(&override.LogLevel, "log-level", defaults.LogLevel, "debug, info, warn, or error")
	return func() (Controller, error) {
		return loadController(flags, *path, defaults, override)
	}
}

func loadController(flags *pflag.FlagSet, path string, config, override Controller) (Controller, error) {
	if path != "" {
		file, err := os.Open(path)
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
		{"SYNOD_DATABASE_PATH", &config.DatabasePath},
		{"SYNOD_ADMIN_TOKEN_FILE", &config.AdminTokenFile},
		{"SYNOD_ACTOR_ID", &config.ActorID},
		{"SYNOD_TLS_CERT_FILE", &config.TLSCertFile},
		{"SYNOD_TLS_KEY_FILE", &config.TLSKeyFile},
		{"SYNOD_CA_CERT_FILE", &config.CACertFile},
		{"SYNOD_CA_KEY_FILE", &config.CAKeyFile},
		{"SYNOD_API_CA_CERT_FILE", &config.APICACertFile},
		{"SYNOD_API_CA_KEY_FILE", &config.APICAKeyFile},
		{"SYNOD_PUBLIC_ORIGIN", &config.PublicOrigin},
		{"SYNOD_LOG_LEVEL", &config.LogLevel},
		{"SYNOD_SECRET_KEY_FILE", &config.SecretKeyFile},
		{"SYNOD_SECRET_KEY_ID", &config.SecretKeyID},
	} {
		if value, present := os.LookupEnv(field.name); present {
			*field.value = value
		}
	}
	flags.Visit(func(value *pflag.Flag) {
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
		case "database-path":
			config.DatabasePath = override.DatabasePath
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
		case "secret-key-file":
			config.SecretKeyFile = override.SecretKeyFile
		case "secret-key-id":
			config.SecretKeyID = override.SecretKeyID
		case "ca-key-file":
			config.CAKeyFile = override.CAKeyFile
		case "public-origin":
			config.PublicOrigin = override.PublicOrigin
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
	if !filepath.IsAbs(config.DatabasePath) {
		return Controller{}, errors.New("database_path must be an absolute file path")
	}
	if id, err := uuid.Parse(config.InstallationID); err != nil || id == uuid.Nil {
		return Controller{}, errors.New("installation_id must be a nonzero UUID")
	}
	if (config.CACertFile == "") != (config.CAKeyFile == "") {
		return Controller{}, errors.New("ca_cert_file and ca_key_file must be supplied together")
	}
	if (config.APICACertFile == "") != (config.APICAKeyFile == "") {
		return Controller{}, errors.New("api_ca_cert_file and api_ca_key_file must be supplied together")
	}
	if (config.SecretKeyFile == "") != (config.SecretKeyID == "") {
		return Controller{}, errors.New("secret_key_file and secret_key_id must be supplied together")
	}
	if config.SecretKeyID != "" {
		if id, err := uuid.Parse(config.SecretKeyID); err != nil || id == uuid.Nil {
			return Controller{}, errors.New("secret_key_id must be a nonzero UUID")
		}
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
