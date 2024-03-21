package main

/***
import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/AryaanB9/sirius_aryaan/internal/tasks"

	"database/sql"


	_ "github.com/go-sql-driver/mysql"
	"github.com/gocql/gocql"

	"github.com/barkha06/sirius/internal/tasks"
	"github.com/barkha06/sirius/internal/template"

)

const (
	CouchbaseDb       = "couchbase"
	MongoDb           = "mongodb"
	CouchbaseColumnar = "columnar"
	DynamoDb          = "dynamodb"
	CassandraDb       = "cassandra"
	SqlDB             = "mysql"
)

func createDBOp(task *tasks.GenericLoadingTask) (string, bool) {
	status := false
	resultString := ""
	switch task.DBType {
	case CassandraDb:
		if task.ConnStr == "" || task.Password == "" || task.Username == "" {
			resultString = "Connection String or Auth Params Empty"
			break
		}

		if task.Extra.Keyspace == "" {
			resultString = "Keyspace name not provided"
			break
		} else if extra.Keyspace != "" && extra.Table == "" {
			// Creating a new Keyspace
			if task.Extra.CassandraClass == "" || task.Extra.ReplicationFactor == 0 {
				resultString += "Cassandra Class or Replication Factor not provided for creating Keyspace"
				break
			}

			cassClusterConfig := gocql.NewCluster(task.ConnStr)
			cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}

			cassandraSession, errCreateSession := cassClusterConfig.CreateSession()
			if errCreateSession != nil {
				log.Println("Unable to connect to Cassandra! err:", errCreateSession.Error())
				resultString += "Unable to connect to Cassandra! err:" + errCreateSession.Error()
				break
			}
			defer cassandraSession.Close()

			createKeyspaceQuery := fmt.Sprintf(`
							CREATE KEYSPACE IF NOT EXISTS %s
							WITH replication = {
								'class': '%s',
								'replication_factor': %v
							};`, extra.Keyspace, extra.CassandraClass, extra.ReplicationFactor)
			errCreateKeyspace := cassandraSession.Query(createKeyspaceQuery).Exec()
			if errCreateKeyspace != nil {
				log.Println("unable to create keyspace", errCreateKeyspace)
				resultString += errCreateKeyspace.Error()
				break
			}
			resultString += fmt.Sprintf("Keyspace '%s' created successfully.", extra.Keyspace)
			status = true
			break

		} else if task.Extra.Keyspace != "" && task.Extra.Table != "" {
			// Creating a new Table. Need to have Template.
			// And, we have to check if Keyspace is created or not.
			if task.OperationConfig.TemplateName == "" {
				resultString += "Template name is not provided. Cannot proceed to create a Table in Cassandra."
				break
			}

			// First creating a client on Cluster and checking if the Keyspace exists or not
			cassClusterConfig := gocql.NewCluster(task.ConnStr)
			cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}
			cassandraSession, errCreateSession := cassClusterConfig.CreateSession()
			if errCreateSession != nil {
				log.Println("Unable to connect to Cassandra! err:", errCreateSession.Error())
				resultString += "Unable to connect to Cassandra! err:" + errCreateSession.Error()
				break
			}

			var count int
			checkKeyspaceQuery := fmt.Sprintf("SELECT count(*) FROM system_schema.keyspaces WHERE keyspace_name = '%s'", task.Extra.Keyspace)
			if err := cassandraSession.Query(checkKeyspaceQuery).Scan(&count); err != nil {
				log.Println("unable run the query to check keyspace existence, err:", err.Error())
				resultString += "unable run the query to check keyspace existence, err:" + err.Error()
				break
			}

			// If keyspace does not exist
			if count <= 0 {
				// Creating the keyspace as it does not exist
				if task.Extra.CassandraClass == "" || task.Extra.ReplicationFactor == 0 {
					resultString += "Cassandra Class or Replication Factor not provided for creating Keyspace"
					break
				}

				createKeyspaceQuery := fmt.Sprintf(`
							CREATE KEYSPACE IF NOT EXISTS %s 
							WITH replication = {
								'class': '%s', 
								'replication_factor': %v
							};`, task.Extra.Keyspace, task.Extra.CassandraClass, task.Extra.ReplicationFactor)
				errCreateKeyspace := cassandraSession.Query(createKeyspaceQuery).Exec()
				if errCreateKeyspace != nil {
					log.Println("unable to create keyspace", errCreateKeyspace)
					resultString += errCreateKeyspace.Error()
					break
				}
			}
			cassandraSession.Close()

			cassClusterConfig = gocql.NewCluster(task.ConnStr)
			cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}
			cassClusterConfig.Keyspace = task.Extra.Keyspace
			cassandraSession, errCreateSession = cassClusterConfig.CreateSession()
			if errCreateSession != nil {
				log.Println("Unable to connect to Cassandra! err:", errCreateSession.Error())
				resultString += "Unable to connect to Cassandra! err:" + errCreateSession.Error()
				break
			}
			defer cassandraSession.Close()

			cassQueries, err := template.GetCassandraSchema(task.OperationConfig.TemplateName, task.Extra.Table)
			if err != nil {
				log.Println(err)
				resultString += err.Error()
				break
			}
			for _, cassQuery := range cassQueries {
				err = cassandraSession.Query(cassQuery).Exec()
				if err != nil {
					log.Println("in Cassandra CreateDbOp(), unable to create type or table err:", err)
					resultString += "in Cassandra CreateDbOp(), unable to create type or table err: " + err.Error()
					return resultString, status
				}
			}

			resultString += fmt.Sprintf("Table ' %s ' created successfully in Keyspace ' %s '.", task.Extra.Table, task.Extra.Keyspace)
			status = true
		}
	}
	return resultString, status
}

func deleteDBOp(task *tasks.GenericLoadingTask) (string, bool) {
	status := false
	resultString := ""
	case CassandraDb:
		if task.ConnStr == "" || task.Password == "" || task.Username == "" {
			resultString = "Connection String or Auth Params Empty"
			break
		}
		if extra.Keyspace == "" {
			resultString = "Keyspace name not provided"
			break
		} else if extra.Keyspace != "" && extra.Table == "" {
			cassClusterConfig := gocql.NewCluster(task.ConnStr)
			cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}

			cassandraSession, errCreateSession := cassClusterConfig.CreateSession()
			if errCreateSession != nil {
				log.Println("Unable to connect to Cassandra! err:", errCreateSession.Error())
				resultString += "Unable to connect to Cassandra! err:" + errCreateSession.Error()
				break
			}
			defer cassandraSession.Close()

			dropKeyspaceQuery := fmt.Sprintf("DROP KEYSPACE %s", extra.Keyspace)
			errDropKeyspace := cassandraSession.Query(dropKeyspaceQuery).Exec()
			if errDropKeyspace != nil {
				log.Println("unable to delete keyspace", errDropKeyspace)
				resultString += errDropKeyspace.Error()
				break
			}

			resultString += fmt.Sprintf("Keyspace '%s' deleted successfully.", extra.Keyspace)
			status = true
			break
		} else if extra.Keyspace != "" && extra.Table != "" {
			cassClusterConfig := gocql.NewCluster(task.ConnStr)
			cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}
			cassClusterConfig.Keyspace = extra.Keyspace
			cassandraSession, errCreateSession := cassClusterConfig.CreateSession()
			if errCreateSession != nil {
				log.Println("Unable to connect to Cassandra! err:", errCreateSession.Error())
				resultString += "Unable to connect to Cassandra! err:" + errCreateSession.Error()
				break
			}
			defer cassandraSession.Close()

			dropTableQuery := fmt.Sprintf("DROP TABLE %s", extra.Table)
			errDropTable := cassandraSession.Query(dropTableQuery).Exec()
			if errDropTable != nil {
				log.Println("unable to delete table", errDropTable)
				resultString += errDropTable.Error()
				break
			}

			resultString += fmt.Sprintf("Table ' %s ' deleted successfully from Keyspace ' %s '.", extra.Table, extra.Keyspace)
			status = true
		}
	}
	return resultString, status


func ListDBOp(task *tasks.GenericLoadingTask) (any, bool) {
	status := false
	resultString := ""
	dblist := make(map[string][]string)
	switch task.DBType {
	case CassandraDb:
		if task.ConnStr == "" || task.Password == "" || task.Username == "" {
			resultString = "Connection String or Auth Params Empty"
			break
		}
		if extra.Keyspace == "" {
			cassClusterConfig := gocql.NewCluster(task.ConnStr)
			cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}
			cassandraSession, errCreateSession := cassClusterConfig.CreateSession()
			if errCreateSession != nil {
				log.Println("Unable to connect to Cassandra! err:", errCreateSession.Error())
				resultString += "Unable to connect to Cassandra! err:" + errCreateSession.Error()
				break
			}
			defer cassandraSession.Close()

			var keyspaceName string
			keyspaces := make([]string, 0)

			//listKeyspaceQuery := "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name != 'system' AND keyspace_name != 'system_traces' AND keyspace_name != 'system_auth' AND keyspace_name != 'system_distributed'"
			listKeyspaceQuery := "SELECT keyspace_name FROM system_schema.keyspaces"
			iterKeyspaces := cassandraSession.Query(listKeyspaceQuery).Iter()

			for iterKeyspaces.Scan(&keyspaceName) {
				keyspaces = append(keyspaces, keyspaceName)
			}
			if err := iterKeyspaces.Close(); err != nil {
				log.Println("error while iterating keyspaces names. err:", err)
				resultString += err.Error()
				break
			}
			resultString += "Given Cassandra cluster contains the following keyspaces"
			for _, keyspaceN := range keyspaces {
				dblist[extra.Keyspace] = append(dblist[extra.Keyspace], keyspaceN)
			}
			status = true
			break
		}
		if extra.Table == "" {
			cassClusterConfig := gocql.NewCluster(task.ConnStr)
			cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}
			cassClusterConfig.Keyspace = extra.Keyspace
			cassandraSession, errCreateSession := cassClusterConfig.CreateSession()
			if errCreateSession != nil {
				log.Println("Unable to connect to Cassandra! err:", errCreateSession.Error())
				resultString += "Unable to connect to Cassandra! err:" + errCreateSession.Error()
				break
			}
			defer cassandraSession.Close()

			var tableName string
			tables := make([]string, 0)

			listTableQuery := "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"
			iterTables := cassandraSession.Query(listTableQuery, extra.Keyspace).Iter()

			for iterTables.Scan(&tableName) {
				tables = append(tables, tableName)
			}
			if err := iterTables.Close(); err != nil {
				log.Println("error while iterating table names. err:", err)
				resultString += err.Error()
				break
			}
			resultString += extra.Keyspace + " keyspace contains the following tables"
			for _, tableN := range tables {
				dblist[extra.Keyspace] = append(dblist[extra.Keyspace], tableN)
			}
			status = true
		}

	}
	if !status && dblist == nil {
		if resultString == "" {
			return "no databases found", status
		}
		return resultString, status
	} else {
		return dblist, status
	}

}
func CountOp(task *tasks.GenericLoadingTask) (string, int64, bool) {
	status := false
	resultString := ""
	var count int64 = -1
	switch task.DBType {

	case CassandraDb:
		if task.ConnStr == "" || task.Password == "" || task.Username == "" {
			resultString = "Connection String or Auth Params Empty"
			break
		}
		if extra.Keyspace == "" {
			resultString = "Keyspace name not provided"
			break
		}
		if extra.Table == "" {
			resultString = "Table name not provided"
			break
		}
		cassClusterConfig := gocql.NewCluster(task.ConnStr)
		cassClusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: task.Username, Password: task.Password}
		cassClusterConfig.Keyspace = extra.Keyspace
		cassandraSession, errCreateSession := cassClusterConfig.CreateSession()
		if errCreateSession != nil {
			log.Println("Unable to connect to Cassandra! err:", errCreateSession.Error())
			resultString += "Unable to connect to Cassandra! err:" + errCreateSession.Error()
			break
		}
		defer cassandraSession.Close()

		countQuery := "SELECT COUNT(*) FROM " + extra.Table
		var rowCount int64
		if errCount := cassandraSession.Query(countQuery).Scan(&rowCount); errCount != nil {
			log.Println("Error while getting COUNT", errCount)
			resultString += "Error while getting COUNT"
			status = false
			break
		}
		resultString += "Count Operation Successful. "
		resultString += "Count = " + string(rowCount)
		status = true
		count = rowCount
	}
	return resultString, count, status

}
***/
