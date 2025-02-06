package data

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"

	"github.com/zeebo/assert"
)

func generatePEMPrivateKey() (string, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", err
	}

	privateKeyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: privateKeyBytes,
	})

	return string(privateKeyPEM), nil
}

func TestPEMCertificateParsing(t *testing.T) {
	pemKey, _ := generatePEMPrivateKey()
	key, err := ParsePEMPrivateKey(pemKey)
	urlSafeKey := GeneratePKCS8StringSupress(key)
	assert.NotNil(t, urlSafeKey)
	assert.NoError(t, err)
	block, _ := base64.URLEncoding.DecodeString(urlSafeKey)
	privKey, _ := x509.ParsePKCS8PrivateKey(block)
	assert.Equal(t, key, privKey)
}
