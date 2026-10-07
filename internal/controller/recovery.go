package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/repository"
)

// readRecovery opens the surviving repository before initialization writes any files.
func readRecovery(ctx context.Context, options InitOptions) (repository.Catalog, uuid.UUID, error) {
	var connection struct {
		Location    repository.Location    `json:"location"`
		Credentials repository.Credentials `json:"credentials"`
	}
	info, err := os.Stat(options.FromRepository)
	if err != nil {
		return repository.Catalog{}, uuid.Nil, fmt.Errorf("read --from-repository: %w", err)
	}
	if info.IsDir() {
		path, err := filepath.Abs(options.FromRepository)
		if err != nil {
			return repository.Catalog{}, uuid.Nil, err
		}
		connection.Location = repository.Location{Kind: "local", Path: path}
	} else {
		raw, err := readStartupFile(options.FromRepository, 64<<10, true)
		if err != nil {
			return repository.Catalog{}, uuid.Nil, fmt.Errorf("read private repository connection file: %w", err)
		}
		defer clear(raw)
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&connection) != nil || decoder.Decode(new(any)) != io.EOF {
			return repository.Catalog{}, uuid.Nil, errors.New("invalid repository connection JSON; use location and credentials fields")
		}
	}
	if options.PasswordFile != "" {
		password, err := readStartupFile(options.PasswordFile, 4096, true)
		if err != nil {
			return repository.Catalog{}, uuid.Nil, fmt.Errorf("read private repository password file: %w", err)
		}
		connection.Credentials.Password = strings.TrimRight(string(password), "\r\n")
		clear(password)
	}
	if connection.Location.Kind == "device" {
		return repository.Catalog{}, uuid.Nil, errors.New("recover a device repository from its directory; use --from-repository PATH --password-file FILE on that device")
	}
	if repository.Validate(connection.Location, connection.Credentials) != nil {
		return repository.Catalog{}, uuid.Nil, errors.New("invalid repository settings; a directory requires --password-file, and a remote repository requires a private connection JSON file")
	}
	repo, err := repository.Open(ctx, connection.Location, connection.Credentials)
	if err != nil {
		return repository.Catalog{}, uuid.Nil, err
	}
	catalog, err := repo.ReadCatalog(ctx)
	if err == nil {
		err = repo.CheckInstallation(ctx, catalog.InstallationID)
	}
	if err = errors.Join(err, repo.Close(context.WithoutCancel(ctx))); err != nil {
		return repository.Catalog{}, uuid.Nil, err
	}
	if options.InstallationID != "" && options.InstallationID != catalog.InstallationID.String() {
		return repository.Catalog{}, uuid.Nil, errors.New("--installation-id conflicts with the repository catalog; omit it")
	}
	var candidates []int
	var choices []string
	for i, saved := range catalog.Destinations {
		choices = append(choices, fmt.Sprintf("%s (%s)", saved.Name, saved.ID))
		matches := reflect.DeepEqual(saved.Location, connection.Location) || (connection.Location.Kind == "local" && saved.Location.Kind == "device" && saved.Location.Path == connection.Location.Path)
		if options.RepositoryDestination != "" {
			matches = options.RepositoryDestination == saved.Name || options.RepositoryDestination == saved.ID.String()
		} else if len(catalog.Destinations) == 1 {
			matches = true
		}
		if matches {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) != 1 {
		return repository.Catalog{}, uuid.Nil, fmt.Errorf("select the surviving repository with --repository-destination NAME_OR_UUID; catalog destinations: %s", strings.Join(choices, ", "))
	}
	d := &catalog.Destinations[candidates[0]]
	joinNode := uuid.Nil
	if connection.Location.Kind == "local" {
		if d.NodeID == uuid.Nil {
			d.NodeID = uuid.New()
			catalog.Devices = append(catalog.Devices, repository.Device{ID: d.NodeID, Name: "recovery-" + d.NodeID.String(), Architecture: runtime.GOARCH})
		}
		joinNode = d.NodeID
		if d.Location.Kind == "device" {
			d.Location.Path = connection.Location.Path
		} else {
			d.Location = connection.Location
		}
	} else {
		d.NodeID = uuid.Nil
		d.Location = connection.Location
	}
	d.Credentials = connection.Credentials
	d.Initialized = true
	return catalog, joinNode, nil
}
