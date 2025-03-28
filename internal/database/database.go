package database

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb" // MS SQL Server
	_ "github.com/go-sql-driver/mysql"   // MySQL
	"github.com/godror/godror"
	_ "github.com/godror/godror"    // Oracle
	_ "github.com/lib/pq"           // PostgreSQL
	_ "github.com/mattn/go-sqlite3" // SQLite

	"sqlrepl/internal/protocol"
)

// Database Driver Constants
const (
	DriverUnknown = iota
	DriverOracle
	DriverMySQL
	DriverPostgreSQL
	DriverSQLite
	DriverSqlServer
)

// dbDriverNames maps driver constants to their string names
var dbDriverNames = map[int]string{
	DriverUnknown:    "unknown",
	DriverOracle:     "godror",
	DriverMySQL:      "mysql",
	DriverPostgreSQL: "postgres",
	DriverSQLite:     "sqlite",
	DriverSqlServer:  "sqlserver",
}

// dbDriverTypes maps lowercase driver names to their driver constants
var dbDriverTypes = map[string]int{
	"oracle":    DriverOracle,
	"mysql":     DriverMySQL,
	"postgres":  DriverPostgreSQL,
	"sqlite":    DriverSQLite,
	"sqlserver": DriverSqlServer,
}

// ValidateDBType validates the database type and returns the corresponding driver constant.
// If the dbType is invalid, it returns DriverUnknown and an error.
func ValidateDBType(dbType string) (int, error) {
	driver, ok := dbDriverTypes[strings.ToLower(dbType)]
	if !ok {
		return DriverUnknown, fmt.Errorf("invalid database type: %s", dbType)
	}
	return driver, nil
}

// DBTypeString returns the string representation of a database type.
func DBTypeString(dbType int) string {
	name, ok := dbDriverNames[dbType]
	if !ok {
		return "unknown"
	}
	return name
}

type Connection struct {
	db      *sql.DB
	dbType  int
	context context.Context
}

// Connect opens the database connection.
func (conn *Connection) Connect(dbType string, dbConnString string) (err error) {
	var driver int
	var db *sql.DB

	driver, err = ValidateDBType(dbType)
	if err != nil {
		return
	}

	db, err = sql.Open(dbDriverNames[driver], dbConnString)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Set connection pooling parameters
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	if err = db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	log.Println("Successfully connected to the database")

	conn.db = db
	conn.dbType = driver
	conn.context = context.TODO()

	switch driver {
	case DriverOracle:
		db.Exec("SET SQLBLANKLINES ON")
		godror.EnableDbmsOutput(conn.context, conn.db)
	}

	return
}

// ExecuteQuery executes a SQL query.
func (conn *Connection) ExecuteQuery(query string) *protocol.QueryResult {
	result := &protocol.QueryResult{}

	// TODO: make this timeout duration configurable
	context, cancelFunc := context.WithTimeout(conn.context, time.Second*20)
	defer cancelFunc()

	conn.preQuery(&query)
	rows, err := conn.db.QueryContext(context, query)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.Columns = columns

	for rows.Next() {
		values := make([]any, len(columns))
		scanArgs := make([]any, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			result.Error = err.Error()
			return result
		}

		rowValues := make([]string, len(columns))
		for i, val := range values {
			if val == nil {
				rowValues[i] = "<nil>"
			} else {
				rowValues[i] = fmt.Sprintf("%v", val)
			}
		}

		protoRow := &protocol.Row{
			Values: rowValues,
		}
		result.Rows = append(result.Rows, protoRow)
	}

	if err = rows.Err(); err != nil {
		result.Error = err.Error()
		return result
	}

	conn.postQuery(result)
	return result
}

// Close closes the database connection.
func (conn *Connection) Close() error {
	if err := conn.db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}
	log.Println("Successfully closed the database connection")
	return nil
}

// some drivers need tweaks to the query, like ensuring that there's
// a semicolon at the end and such. This function houses that logic.
// Directly modifies `query`
func (conn *Connection) preQuery(query *string) {
	switch conn.dbType {
	case DriverOracle:
		q := *query
		if len(q) > 3 && strings.ToUpper(q[len(q)-3:]) == "END" {
			// a block must end with a semicolon
			*query = fmt.Sprintf("%s;", q)
		} else if len(q) > 1 && q[len(q)-1] == ';' {
			// oracle does not like you to add your own semicolons at the
			// end of a statement
			*query = q[:len(q)-1]
		}
	}
}

// some drivers need to do some extra steps after a query, such as processing
// output from print statements
func (conn *Connection) postQuery(result *protocol.QueryResult) {
	switch conn.dbType {
	case DriverOracle:
		var builder strings.Builder
		var writer io.Writer = &builder
		err := godror.ReadDbmsOutput(conn.context, writer, conn.db)
		if err != nil {
			log.Fatalf("Unable to read DBMS_OUTPUT: %v", err)
		}
		result.Message = builder.String()
	}
}
