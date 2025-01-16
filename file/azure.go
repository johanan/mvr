package file

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type AzureBlobConfig struct {
	blobUrl       *url.URL
	withContainer string
	container     string
	blobName      string
	sasToken      string
}

type AzureBlob struct {
	*AzureBlobConfig
	client *azblob.Client
	writer io.WriteCloser
	wg     *sync.WaitGroup
}

func (a *AzureBlob) Write(p []byte) (n int, err error) {
	return a.writer.Write(p)
}

func (a *AzureBlob) Close() error {
	a.writer.Close()
	a.wg.Wait()
	return nil
}

func (a *AzureBlobConfig) GetWriter(ctx context.Context) (*AzureBlob, error) {
	var client *azblob.Client
	if a.sasToken == "" {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("AzureBlob: %v", err)
		}
		client, err = azblob.NewClient(a.withContainer, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("AzureBlob: %v", err)
		}
	}

	if a.sasToken != "" {
		urlWithSas := fmt.Sprintf("%s?%s", a.withContainer, a.sasToken)
		var err error
		client, err = azblob.NewClientWithNoCredential(urlWithSas, nil)
		if err != nil {
			return nil, fmt.Errorf("AzureBlob: %v", err)
		}
	}
	pr, pw := io.Pipe()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer pr.Close()
		_, err := client.UploadStream(ctx, "", a.blobName, pr, nil)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()

	return &AzureBlob{AzureBlobConfig: a, writer: pw, client: client, wg: &wg}, nil

}

func ParseAzurite(blobUrl *url.URL) (*AzureBlobConfig, error) {
	sasToken, _ := blobUrl.User.Password()
	blobUrl.User = nil

	segments := strings.SplitN(blobUrl.Path, "/", 4)
	if len(segments) <= 3 {
		return nil, fmt.Errorf("AzureBlob: invalid path needs container")
	}
	account := segments[1]
	container := segments[2]
	blobName := segments[3]
	withContainer := fmt.Sprintf("http://%s:%s/%s/%s", blobUrl.Hostname(), blobUrl.Port(), account, container)

	return &AzureBlobConfig{withContainer: withContainer, blobUrl: blobUrl, container: container, blobName: blobName, sasToken: sasToken}, nil
}

func ParseAzureBlobURL(blobUrl *url.URL) (*AzureBlobConfig, error) {
	sasToken, ok := blobUrl.User.Password()
	if ok {
		blobUrl.User = nil
	}

	if blobUrl.Scheme == "azure" {
		azureUrl := fmt.Sprintf("https://%s.blob.core.windows.net%s", blobUrl.Hostname(), blobUrl.Path)
		var err error
		blobUrl, err = url.Parse(azureUrl)
		if err != nil {
			return nil, fmt.Errorf("AzureBlob: invalid url")
		}
	}

	if blobUrl.Scheme != "https" {
		return nil, fmt.Errorf("AzureBlob: only https or azure is supported")
	}

	// ensure that is is blob.core.windows.net
	if !strings.Contains(strings.ToLower(blobUrl.Hostname()), "blob.core.windows.net") {
		return nil, fmt.Errorf("AzureBlob: only blob.core.windows.net is supported for https")
	}
	segments := strings.SplitN(blobUrl.Path, "/", 3)
	if len(segments) <= 2 {
		return nil, fmt.Errorf("AzureBlob: invalid path needs container and blob path")
	}
	container := segments[1]
	blobName := segments[2]
	withContainer := fmt.Sprintf("https://%s/%s", blobUrl.Hostname(), container)

	return &AzureBlobConfig{withContainer: withContainer, blobUrl: blobUrl, container: container, blobName: blobName, sasToken: sasToken}, nil
}
