package data

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
)

func ParsePEMPrivateKey(pemKey string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemKey))
	if block == nil {
		return nil, errors.New("failed to decode PEM block containing RSA private key")
	}

	var privateKey *rsa.PrivateKey

	if block.Type == "RSA PRIVATE KEY" {
		privateKey, _ = x509.ParsePKCS1PrivateKey(block.Bytes)
	} else if block.Type == "PRIVATE KEY" {
		privKey, _ := x509.ParsePKCS8PrivateKey(block.Bytes)
		privateKey = privKey.(*rsa.PrivateKey)
	} else {
		return nil, errors.New("unsupported key type")
	}

	return privateKey, nil
}

func GeneratePKCS8StringSupress(key *rsa.PrivateKey) string {
	// Copied straight from snowflake's go driver
	tmpBytes, _ := x509.MarshalPKCS8PrivateKey(key)
	privKeyPKCS8 := base64.URLEncoding.EncodeToString(tmpBytes)
	return privKeyPKCS8
}
