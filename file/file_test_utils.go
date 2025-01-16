package file

import "bytes"

var local_db_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
var local_ms_url = "sqlserver://sa:YourStrong%40Passw0rd@localhost:1433?database=TestDB&TrustServerCertificate=true&encrypt=disable"

type WriteCloseBuffer struct {
	*bytes.Buffer
}

func (wcb *WriteCloseBuffer) Write(p []byte) (n int, err error) {
	return wcb.Buffer.Write(p)
}

func (wcb *WriteCloseBuffer) Close() error {
	return nil
}

// NewWriteCloseBuffer initializes and returns a new WriteCloseBuffer.
func NewWriteCloseBuffer(buf *bytes.Buffer) *WriteCloseBuffer {
	return &WriteCloseBuffer{
		Buffer: buf,
	}
}
