package sdk_cassandra

import (
	"github.com/gocql/gocql"
)

// CassandraKeyspaceObject has the CassandraKeyspace of type *gocql.Session.
// Here, *gocql.Session has the Keyspace parameter configured, hence it is keyspace level.
type CassandraKeyspaceObject struct {
	CassandraKeyspace *gocql.Session `json:"-"`
}
