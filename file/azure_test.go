package file

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/johanan/mvr/core"
	"github.com/johanan/mvr/data"
	"github.com/johanan/mvr/database"
	"github.com/zeebo/assert"
)

var (
	account    = "devstoreaccount1"
	sharedKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	serviceURL = "http://127.0.0.1:10000/devstoreaccount1"
	sasToken   = ""
)

func init() {
	// create new client
	cred, _ := azblob.NewSharedKeyCredential(account, sharedKey)
	client, _ := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	_, _ = client.CreateContainer(context.Background(), "testcontainer", nil)
	sasQuery, _ := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPSandHTTP,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		ContainerName: "testcontainer",
		BlobName:      "",
		Permissions:   (&sas.ContainerPermissions{Read: true, Write: true, Delete: true, List: false}).String(),
	}.SignWithSharedKey(cred)
	sasToken = sasQuery.Encode()
}

func TestAzureBlob_Writer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sc := &data.StreamConfig{StreamName: "public.numbers", Format: "csv"}
	err := sc.Validate()
	assert.NoError(t, err)
	local_url, _ := url.Parse(local_db_url)

	pgr, _ := database.NewPGDataReader(local_url)
	pgDs, _ := pgr.CreateDataStream(ctx, local_url, sc)
	assert.NotNil(t, pgDs)
	defer pgr.Close()

	blobUrl, _ := url.Parse(fmt.Sprintf("http://:%s@127.0.0.1:10000/devstoreaccount1/%s", sasToken, "testcontainer/test/test.csv"))
	blobConfig, _ := ParseAzurite(blobUrl)
	blobWriter, _ := blobConfig.GetWriter(ctx)

	buf := NewBufferedWriter(blobWriter, blobWriter)
	writer := NewCSVDataWriter(pgDs, buf)

	err = core.Execute(ctx, 1, sc, pgDs, pgr, writer)
	assert.NoError(t, err)
	writer.Close()

	err = blobWriter.Close()
	assert.NoError(t, err)
}

func Test_NewAzureBlob(t *testing.T) {
	tests := []struct {
		name      string
		blobUrl   string
		sas       string
		container string
		blobName  string
		expectErr bool
	}{
		{
			name:      "Valid Azure Blob with path",
			blobUrl:   "https://devstoreaccount1.blob.core.windows.net/testcontainer/testpath/year/month/day/test.parquet",
			sas:       "",
			container: "/testcontainer/testpath/year/month/day",
			blobName:  "test.parquet",
			expectErr: false,
		},
		{
			name:      "Valid Azure Blob with custom azure scheme and path",
			blobUrl:   "azure://devstoreaccount1/testcontainer/testpath/year/month/day/test.parquet",
			sas:       "",
			container: "/testcontainer/testpath/year/month/day",
			blobName:  "test.parquet",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blobUrl, _ := url.Parse(tt.blobUrl)
			blob, err := ParseAzureBlobURL(blobUrl)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.container, blob.container)
				assert.Equal(t, tt.blobName, blob.blobName)
			}

		})
	}
}
