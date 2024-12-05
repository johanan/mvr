package data

const (
	RFC3339NanoNoTZ = "2006-01-02T15:04:05.999999999"
)

func ConvertSQLServerUUID(sqlBytes []byte) []byte {
	if len(sqlBytes) != 16 {
		return sqlBytes
	}

	converted := make([]byte, 16)

	// Reverse the byte order for the first three fields.
	// time_low (bytes 0-3)
	converted[0] = sqlBytes[3]
	converted[1] = sqlBytes[2]
	converted[2] = sqlBytes[1]
	converted[3] = sqlBytes[0]

	// time_mid (bytes 4-5)
	converted[4] = sqlBytes[5]
	converted[5] = sqlBytes[4]

	// time_hi_and_version (bytes 6-7)
	converted[6] = sqlBytes[7]
	converted[7] = sqlBytes[6]

	// The remaining bytes (8-15) are copied as-is.
	copy(converted[8:], sqlBytes[8:16])

	return converted
}
