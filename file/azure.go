package file

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type AzureBlobConfig struct {
	blobUrl   *url.URL
	base      string
	container string
	blobName  string
	sasToken  string
}

type AzureBlob struct {
	*AzureBlobConfig
	client *azblob.Client
	writer io.WriteCloser
	wg     *sync.WaitGroup
	errCh  chan error
	open   bool
}

func (a *AzureBlob) Write(p []byte) (n int, err error) {
	return a.writer.Write(p)
}

func (a *AzureBlob) Close() error {
	if a.open {
		a.open = false

		if err := a.writer.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %w", err)
		}
		a.wg.Wait()
		select {
		case err := <-a.errCh:
			if err != nil {
				return fmt.Errorf("upload failed: %w", err)
			}
		default:
		}
	}
	return nil

}

func (a *AzureBlobConfig) GetWriter(ctx context.Context) (*AzureBlob, error) {
	var client *azblob.Client
	if a.sasToken == "" {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("AzureBlob: %v", err)
		}
		client, err = azblob.NewClient(a.base, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("AzureBlob: %v", err)
		}
	}

	if a.sasToken != "" {
		urlWithSas := fmt.Sprintf("%s?%s", a.base, a.sasToken)
		var err error
		client, err = azblob.NewClientWithNoCredential(urlWithSas, nil)
		if err != nil {
			return nil, fmt.Errorf("AzureBlob: %v", err)
		}
	}
	pr, pw := io.Pipe()

	wg := sync.WaitGroup{}
	wg.Add(1)
	errCh := make(chan error, 1)

	go func() {
		defer wg.Done()
		defer pr.Close()
		_, err := client.UploadStream(ctx, a.container, a.blobName, pr, nil)
		if err != nil {
			pw.CloseWithError(err)
			errCh <- err
		}
	}()

	return &AzureBlob{AzureBlobConfig: a, writer: pw, client: client, wg: &wg, errCh: errCh, open: true}, nil

}

func ParseAzurite(blobUrl *url.URL) (*AzureBlobConfig, error) {
	sasToken, _ := blobUrl.User.Password()
	blobUrl.User = nil

	blobName := path.Base(blobUrl.Path)
	container := path.Dir(blobUrl.Path)
	base := fmt.Sprintf("http://%s:%s", blobUrl.Hostname(), blobUrl.Port())

	return &AzureBlobConfig{base: base, blobUrl: blobUrl, container: container, blobName: blobName, sasToken: sasToken}, nil
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
	blobName := path.Base(blobUrl.Path)
	container := path.Dir(blobUrl.Path)
	base := fmt.Sprintf("%s://%s", blobUrl.Scheme, blobUrl.Host)

	return &AzureBlobConfig{base: base, blobUrl: blobUrl, container: container, blobName: blobName, sasToken: sasToken}, nil
}
