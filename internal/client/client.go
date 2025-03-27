package client

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"

	"sqlrepl/internal/database"
	"sqlrepl/internal/protocol"

	"google.golang.org/protobuf/proto"
)

// TODO: the database connection parameters should really be encrypted, maybe
// the whole request really; not a huge deal when running locally, but it's a super
// big deal if connecting to this server remotely

// Handle manages a single client connection in server mode.
func Handle(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Read the database connection parameters as JSON
	paramsJSON, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Error reading connection parameters: %v", err)
		return
	}
	paramsJSON = paramsJSON[:len(paramsJSON)-1] // Trim newline

	var params protocol.DBParams
	err = json.Unmarshal([]byte(paramsJSON), &params)
	if err != nil {
		log.Printf("Error unmarshaling connection parameters: %v", err)
		sendError(conn, "Invalid connection parameters")
		return
	}

	// Connect to the database
	dbconn := database.Connection{}
	err = dbconn.Connect(params.Dbtype, params.Connstring)
	if err != nil {
		log.Printf("Error connecting to database: %v", err)
		sendError(conn, "Failed to connect to database")
		return
	}
	defer dbconn.Close()

	// Handle subsequent queries
	for {
		query, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("Client disconnected")
				return
			}
			log.Printf("Error reading from client: %v", err)
			return
		}

		if len(query) > 0 && query[0] == '\x1D' { // group/batch delimiter
			// First send the response length so that the client knows how many
			// bytes to read
			bytes := []byte("\x1D")
			lengthBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(lengthBytes, uint32(len(bytes)))

			_, err = conn.Write(lengthBytes)
			if err != nil {
				log.Printf("Error sending length to client: %v", err)
				return
			}

			// write out group-delimiter characted to notify the client that
			// we're finished writing responses for the current batch of
			// queries
			conn.Write(bytes)
			continue
		}

		query = query[:len(query)-1] // Trim newline
		result := dbconn.ExecuteQuery(query)

		protoResult := protocol.QueryResult{
			Columns: result.Columns,
			Message: result.Message,
			Error:   result.Error,
		}

		for _, row := range result.Rows {
			protoRow := &protocol.Row{
				Values: make([]string, len(result.Columns)),
			}
			for i := range result.Columns {
				protoRow.Values[i] = fmt.Sprintf("%v", row.Values[i])
			}
			protoResult.Rows = append(protoResult.Rows, protoRow)
		}

		// Marshal the protocol buffer
		responseBytes, err := proto.Marshal(&protoResult)
		if err != nil {
			log.Printf("Error marshaling protocol buffer: %v", err)
			return
		}

		// First send the response length so that the client knows how many
		// bytes to read
		lengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lengthBytes, uint32(len(responseBytes)))

		log.Printf("Sending protobuf data (length: %d, bytes: %x)", len(responseBytes), lengthBytes)

		_, err = conn.Write(lengthBytes)
		if err != nil {
			log.Printf("Error sending length to client: %v", err)
			return
		}

		_, err = conn.Write(responseBytes)
		if err != nil {
			log.Printf("Error sending response to client: %v", err)
			return
		}
	}
}

// sendError sends a protocol buffer-encoded error message to the client.
func sendError(conn net.Conn, message string) {
	errorResult := protocol.QueryResult{Error: message}
	errorBytes, _ := proto.Marshal(&errorResult)
	conn.Write(errorBytes)
	conn.Write([]byte("\n"))
}
