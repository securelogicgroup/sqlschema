// +build integration

package sqlschema

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	_ "github.com/lib/pq"
)

// Sanity check for postgres
func TestPostgres(t *testing.T) {
	c, err := startPGContainer()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.cleanup(); err != nil {
			t.Fatal(err)
		}
	})

	db, err := sql.Open("postgres", "postgres://postgres:postgres@"+c.Addr+"/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	// Poll until db is ready
	err = db.Ping()
	for i := 0; err != nil && i < 100; i++ {
		time.Sleep(time.Millisecond * 50)
		err = db.Ping()
	}

	f := http.Dir("./test/valid_updates")
	if err := Apply(db, f); err != nil {
		t.Fatal(err)
	}

	defer db.Close()
	rows, err := db.Query(`SELECT * FROM a;`)
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
}

type pgContainer struct {
	ID      string
	Addr    string
	cleanup func() error
	cli     *client.Client
}

// Close removes the container and generated files.
func (c *pgContainer) Close() error {
	return c.cleanup()
}

func (c *pgContainer) Logs() (io.ReadCloser, error) {
	return c.cli.ContainerLogs(context.Background(), c.ID, types.ContainerLogsOptions{
		ShowStderr: true,
		ShowStdout: true,
	})
}

// startccf starts a single node CCF network with generated initial
// member in docker container.
func startPGContainer() (*pgContainer, error) {
	cli, err := client.NewEnvClient()
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}
	cleanup := func() error {
		return cli.Close()
	}

	// Create container
	container, err := cli.ContainerCreate(
		context.Background(),
		&container.Config{
			Image: "postgres:13-alpine",
			Env:   []string{"POSTGRES_PASSWORD=postgres"},
		},
		&container.HostConfig{
			PublishAllPorts: true,
			AutoRemove:      true,
		},
		&network.NetworkingConfig{},
		"",
	)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("create container: %w", err)
	}

	err = cli.ContainerStart(context.Background(), container.ID, types.ContainerStartOptions{})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("start container: %w", err)
	}

	cleanup = compose(cleanup, func() error {
		return cli.ContainerKill(context.Background(), container.ID, "KILL")
	})

	inspect, err := cli.ContainerInspect(context.Background(), container.ID)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("inspect: %w", err)
	}

	return &pgContainer{
		ID:      container.ID,
		Addr:    inspect.NetworkSettings.IPAddress,
		cleanup: cleanup,
		cli:     cli,
	}, nil
}

func compose(f func() error, g func() error) func() error {
	return func() error {
		a := g()
		b := f()
		return joinErrs(a, b)
	}
}

func joinErrs(a, b error) error {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if m, ok := a.(multiError); ok {
		return m.Add(b)
	}
	if m, ok := b.(multiError); ok {
		return m.Add(a)
	}
	return nil
}

type multiError []error

func (m multiError) Error() string {
	if len(m) == 0 {
		return "no error information"
	}
	if len(m) == 1 {
		return m[0].Error()
	}
	var msg, sep string
	for i, v := range m {
		msg += fmt.Sprintf("%s%d: %s", sep, i+1, v)
		sep = "\n"
	}
	return fmt.Sprintf("multiple (%d) errors:\n%s", len(m), msg)
}

func (m multiError) Add(err error) multiError {
	if e, ok := err.(multiError); ok {
		return append(m, e...)
	}
	return append(m, err)
}
