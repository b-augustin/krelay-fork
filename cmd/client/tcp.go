package main

import (
	"io"
	"log/slog"
	"net"
	"time"

	"k8s.io/apimachinery/pkg/util/httpstream"

	"github.com/knight42/krelay/pkg/constants"
	slogutil "github.com/knight42/krelay/pkg/slog"
	"github.com/knight42/krelay/pkg/xio"
	"github.com/knight42/krelay/pkg/xnet"
)

const maxRetries = 3
const retryDelay = 2 * time.Second

func handleTCPConn(clientConn net.Conn, serverConn httpstream.Connection, dstAddr xnet.Addr, dstPort uint16) {
	defer clientConn.Close()
	l := slog.With(slog.String(constants.LogFieldRequestID, xnet.NewRequestID()))

	var attempt int
	for attempt = 0; attempt < maxRetries; attempt++ {
		l.Info("Attempt:")
		if handleConnection(clientConn, serverConn, dstAddr, dstPort) {
			return
		}
		time.Sleep(retryDelay)
	}

	l.Error("Max retries reached, giving up")
}

func handleConnection(clientConn net.Conn, serverConn httpstream.Connection, dstAddr xnet.Addr, dstPort uint16) bool {
	requestID := xnet.NewRequestID()
	l := slog.With(slog.String(constants.LogFieldRequestID, requestID))
	defer l.Debug("handleTCPConn exit")
	l.Info("Handling tcp connection",
		slog.String(constants.LogFieldDestAddr, xnet.JoinHostPort(dstAddr.String(), dstPort)),
		slog.String(constants.LogFieldLocalAddr, clientConn.LocalAddr().String()),
		slog.String("clientAddr", clientConn.RemoteAddr().String()),
	)

	dataStream, errorChan, err := createStream(serverConn, requestID)
	if err != nil {
		l.Error("Fail to create stream", slogutil.Error(err))
		return false
	}

	hdr := xnet.Header{
		RequestID: requestID,
		Protocol:  xnet.ProtocolTCP,
		Port:      dstPort,
		Addr:      dstAddr,
	}
	_, err = xio.WriteFull(dataStream, hdr.Marshal())
	if err != nil {
		l.Error("Fail to write header", slogutil.Error(err))
		return false
	}

	var ack xnet.Acknowledgement
	err = ack.FromReader(dataStream)
	if err != nil {
		l.Error("Fail to receive ack", slogutil.Error(err))
		return false
	}
	if ack.Code != xnet.AckCodeOK {
		l.Error("Fail to connect", slogutil.Error(ack.Code))
		return false
	}

	localError := make(chan struct{})
	remoteDone := make(chan struct{})

	go func() {
		if _, err := io.Copy(clientConn, dataStream); err != nil && !xnet.IsClosedConnectionError(err) {
			l.Error("Fail to copy from remote stream to local connection", slogutil.Error(err))
		}
		close(remoteDone)
	}()

	go func() {
		defer dataStream.Close()
		if _, err := io.Copy(dataStream, clientConn); err != nil && !xnet.IsClosedConnectionError(err) {
			l.Error("Fail to copy from local connection to remote stream", slogutil.Error(err))
			close(localError)
		}
	}()

	select {
	case <-remoteDone:
	case <-localError:
		return false
	}

	err = <-errorChan
	if err != nil {
		l.Error("Unexpected error from stream", slogutil.Error(err))
		return false
	}

	return true
}
