package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"sqlrepl/internal/client"
	"sqlrepl/internal/database"
	"sqlrepl/internal/protocol"
)

const (
	defaultListenAddress = 8080
)

var (
	// Flags
	dbType        = flag.String("t", "", "Database type (oracle, mysql, postgres, sqlite3)")
	dbConnString  = flag.String("c", "", "Database connection string")
	listenAddress = flag.Int("p", defaultListenAddress, "Address to listen on in server mode")
)

func main() {
	flag.Parse()
	args := flag.Args()

	// Check for positional arguments for interactive mode
	if len(args) == 2 {
		runInteractive(args[0], args[1])
		return
	}

	// Use flags if provided
	if *dbType != "" && *dbConnString != "" {
		runInteractive(*dbType, *dbConnString)
		return
	}

	// Run in server mode if no flags are provided
	if len(args) == 0 {
		runServer(*listenAddress)
		return
	}

	// Otherwise, print usage
	fmt.Println("Usage:")
	fmt.Println("  sqlrepl <dbtype> <connstring>  (Interactive mode)")
	fmt.Println("  sqlrepl -p <port>               (Server mode)")
	flag.PrintDefaults()
	os.Exit(1)
}

func runInteractive(dbType, dbConnString string) {
	dbconn := database.Connection{}
	err := dbconn.Connect(dbType, dbConnString)
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	defer dbconn.Close()

	fmt.Println("Connected. Enter SQL queries (or 'exit' to quit):")
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break // Exit on Ctrl+D
		}
		query := scanner.Text()
		if query == "exit" {
			break
		}

		result := dbconn.ExecuteQuery(query)

		if result == nil {
			log.Printf("Result returned from executeQuery was nil: %v", err)
			return
		}

		printQueryResult(result) // Helper function to format and print result
	}

	if err := scanner.Err(); err != nil {
		log.Println("Error reading input:", err)
	}
}

func runServer(listenAddress int) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", listenAddress))
	if err != nil {
		log.Fatalf("Error listening: %v", err)
	}
	defer listener.Close()

	fmt.Printf("SQL REPL server listening on %d\n", listenAddress)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		log.Printf("Accepted connection from %s\n", conn.RemoteAddr().String())
		go client.Handle(conn) // Delegate to client handler (modified)
	}
}

func printQueryResult(result *protocol.QueryResult) {
	if result.Error != "" {
		fmt.Println("Error:", result.Error)
		return
	}

	if len(result.Columns) > 0 {
		for _, col := range result.Columns {
			fmt.Printf("%s\t", col)
		}
		fmt.Println()
	}

	for _, row := range result.Rows {
		for i := range result.Columns {
			fmt.Printf("%v\t", row.Values[i])
		}
		fmt.Println()
	}

	if result.Message != "" {
		fmt.Println(result.Message)
	}
}
