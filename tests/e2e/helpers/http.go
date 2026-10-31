package helpers

import (
	"bytes"
	"io"
	"net/http"
	"time"
)

type HTTPClient struct {
	BaseURL string
	Client  *http.Client
}

func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{
		BaseURL: baseURL,
		Client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *HTTPClient) Get(path string) ([]byte, int, error) {
	resp, err := c.Client.Get(c.BaseURL + path)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	return body, resp.StatusCode, err
}

func (c *HTTPClient) Post(path string, contentType string, data []byte) ([]byte, int, error) {
	resp, err := c.Client.Post(c.BaseURL+path, contentType, bytes.NewBuffer(data))
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	return body, resp.StatusCode, err
}
