package transports

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	

	// {{if .Config.Debug}}
	"log"
	// {{end}}

	pb "github.com/bishopfox/sliver/protobuf/sliverpb"
	"google.golang.org/protobuf/proto"

)
//
// Length-prefixed envelope helpers (must match server)
// Format: [uint32 length][protobuf data]
//

func tcpWriteEnvelope(conn net.Conn, envelope *pb.Envelope) error {
	data, err := proto.Marshal(envelope)
	if err != nil {
		return err
	}

	length := uint32(len(data))
	if err := binary.Write(conn, binary.LittleEndian, length); err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}

func tcpReadEnvelope(conn net.Conn) (*pb.Envelope, error) {
	var length uint32
	if err := binary.Read(conn, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	if length == 0 || length > 16*1024*1024 {
		return nil, errors.New("invalid envelope length")
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, err
	}

	envelope := &pb.Envelope{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return nil, err
	}
	return envelope, nil
}


