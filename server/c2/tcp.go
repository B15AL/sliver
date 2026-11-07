package c2

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	
	"github.com/bishopfox/sliver/protobuf/sliverpb"
	"github.com/bishopfox/sliver/server/core"
	serverHandlers "github.com/bishopfox/sliver/server/handlers"
	"github.com/bishopfox/sliver/server/log"
	"google.golang.org/protobuf/proto"
)

var (
	simpletcpLog = log.NamedLogger("c2", "tcp")
)

// StartTCPListener - Start a simple raw TCP listener for implants
func StartSimpleTCPListener(bindIface string, port uint16) (net.Listener, error) {
	simpletcpLog.Infof("Starting raw TCP listener on %s:%d", bindIface, port)

	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", bindIface, port))
	if err != nil {
		simpletcpLog.Errorf("Failed to start TCP listener: %v", err)
		return nil, err
	}

	go acceptTCPConnections(ln)
	return ln, nil
}

func acceptTCPConnections(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errType, ok := err.(*net.OpError); ok && errType.Op == "accept" {
				break // Listener closed
			}
			simpletcpLog.Errorf("Accept failed: %v", err)
			continue
		}
		go handleTCPConnection(conn)
	}
}

func handleTCPConnection(conn net.Conn) {
	defer conn.Close()

	simpletcpLog.Infof("Accepted TCP connection from %s", conn.RemoteAddr())

	implantConn := core.NewImplantConnection("tcp", conn.RemoteAddr().String())
	defer implantConn.Close()

	// Start the main session handler
	handleTCPSession(conn, implantConn)
}

func handleTCPSession(conn net.Conn, implantConn *core.ImplantConnection) {
	done := make(chan struct{})
	var once sync.Once
	closeDone := func() {
		once.Do(func() {
			close(done)
			conn.Close()
		})
	}

	// Closing either side should stop everything
	go func() {
		select {
		case <-implantConn.Done():
			closeDone()
		case <-done:
		}
	}()

	//
	// Receiver loop (Server ← Implant)
	//
	go func() {
		defer closeDone()
		handlers := serverHandlers.GetHandlers()

		for {
			envelope, err := tcpReadEnvelope(conn)
			if err != nil {
				simpletcpLog.Errorf("Read error: %v", err)
				return
			}

			implantConn.UpdateLastMessage()

			// Response to a previous request
			if envelope.ID != 0 {
				implantConn.DeliverResponse(envelope)
				continue
			}

			// New message from implant
			if handler, ok := handlers[envelope.Type]; ok {
				go func(env *sliverpb.Envelope) {
					resp := handler(implantConn, env.Data)
					if resp != nil {
						implantConn.SendEnvelope(resp, core.DefaultImplantSendTimeout)
					}
				}(envelope)
			}
		}
	}()

	//
	// Sender loop (Server → Implant)
	//
	go func() {
		defer closeDone()
		for {
			select {
			case <-done:
				return
			case envelope := <-implantConn.Send:
				if envelope == nil {
					continue
				}
				if err := tcpWriteEnvelope(conn, envelope); err != nil {
					simpletcpLog.Errorf("Write error: %v", err)
					return
				}
			}
		}
	}()

	<-done
}

//
// Simple length-prefixed envelope framing
// Format: [uint32 length][protobuf data]
//

func tcpWriteEnvelope(conn net.Conn, envelope *sliverpb.Envelope) error {
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

func tcpReadEnvelope(conn net.Conn) (*sliverpb.Envelope, error) {
	var length uint32
	if err := binary.Read(conn, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	if length == 0 || length > 16*1024*1024 { // 16 MB safety limit
		return nil, errors.New("invalid envelope length")
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, err
	}

	envelope := &sliverpb.Envelope{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return nil, err
	}
	return envelope, nil
}
