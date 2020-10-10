package testhelpers

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"os/exec"
	"strconv"
	"strings"
	"time"
	"github.com/testcontainers/testcontainers-go"
)

type PostgresConfig struct {
	// Used to configure the database
	Username string
	Password string
	Database string

	// Used to generate the database name
	Service string
	Realm string

	// The port is allocated randomly by docker, then looked up after launch
	port int

	// Cache the db connection
	db *sqlx.DB
	instance testcontainers.Container
}

func (cfg *PostgresConfig) GetContainerName() string {
	return fmt.Sprintf("postgres-%s-%s", cfg.Service, cfg.Realm)
}
func (cfg *PostgresConfig) GetUserVar() string {
	return fmt.Sprintf("POSTGRES_USER=%s", cfg.Username)
}
func (cfg *PostgresConfig) GetPasswordVar() string {
	return fmt.Sprintf("POSTGRES_PASSWORD=%s", cfg.Username)
}
func (cfg *PostgresConfig) GetDbVar() string {
	return fmt.Sprintf("POSTGRES_DB=%s", cfg.Username)
}

func (cfg *PostgresConfig) GetPort() int {
	if cfg.port > 0 {
		return cfg.port
	}

	// Look up the exposed port number
	cmd := exec.Command("docker", "port", cfg.GetContainerName(), "5432/tcp")
	var out2 bytes.Buffer
	cmd.Stdout = &out2
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	hostPortTokens := strings.Split(out2.String(), ":")
	port, err := strconv.Atoi(hostPortTokens[1])
	if err != nil {
		log.Fatal("Failed to parse host port for test db")
	}
	cfg.port = port
	return cfg.port
}

func (cfg *PostgresConfig) getDSN() string {
	return fmt.Sprintf("%s://%s:%s@%s:%d/%s?sslmode=disable",
		"postgres",
		cfg.Username,
		cfg.Password,
		"localhost",
		cfg.GetPort(),
		cfg.Database)
}
func (cfg *PostgresConfig) GetDbConn() (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", cfg.getDSN())
	retries := 20
	for i:=0; i < retries; i++ {
		log.WithError(err).Warn("Error connecting to postgres")
		time.Sleep(250 * time.Millisecond)
		db, err = sqlx.Connect("postgres", cfg.getDSN())
		if err != nil {
			break
		}
	}

	if err != nil {
		return nil, err
	}

	// Set low connection counts for a local test database
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)

	cfg.db = db
	return db, nil
}

func (cfg *PostgresConfig) LaunchDockerInstance(ctx context.Context) (*sqlx.DB, error) {
	req := testcontainers.ContainerRequest{
		Image: "timms/postgres-logging:10.3",
		ExposedPorts: []string{"5432/tcp"},
	}
	dbC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started: true,
	})
	if err != nil {
		return nil, err
	}

	cfg.instance = dbC

	// Wait until we can get a database connection
	db, err := sqlx.Connect("postgres", cfg.getDSN())
	connectStart := time.Now()
	maxWaitTime := 30 * time.Second
	for err != nil {
		if time.Since(connectStart) > maxWaitTime {
			return nil, err
		}
		time.Sleep(2*time.Second)
		db, err = sqlx.Connect("postgres", cfg.getDSN())
	}

	return db, err
}

func (cfg *PostgresConfig) Cleanup(ctx context.Context) error {
	if cfg.instance != nil {
		err := cfg.instance.Terminate(ctx)
		if err != nil {
			return err
		}
		cfg.instance = nil
	}
	return nil
}
