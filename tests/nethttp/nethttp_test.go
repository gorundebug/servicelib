/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package nethttp

import (
	"bytes"
	"context"
	"fmt"
	"github.com/gorundebug/servicelib/tests/mockservice"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	mockservice.Main(func() int {
		return m.Run()
	})
}

func sendRequest() error {
	url := "http://localhost:8080/data"
	data := []byte(`{"text":"OK"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("error creating request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %s", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response: %s", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status code is %d", resp.StatusCode)
	}

	return nil
}

func TestNetHTTPEndpointConsumer(t *testing.T) {
	service := mockservice.GetMockService()
	assert.Equal(t, nil, sendRequest())
	assert.NotNilf(t, service.RequestData, "request data is nil")
	if service.RequestData != nil {
		assert.Equal(t, "OK", service.RequestData.Text)
	}
}
