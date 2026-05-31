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
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/tests/mockservice"
	"github.com/stretchr/testify/assert"
)

var testEnv *mockservice.TestEnv

func TestMain(m *testing.M) {
	mockservice.Main("..", func(env *mockservice.TestEnv) int {
		testEnv = env
		return m.Run()
	})
}

func sendRequest() error {
	url := "http://127.0.0.1:9091/data"
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
	service := testEnv.Service
	assert.Equal(t, nil, sendRequest())
	rd := service.RequestData.Load()
	assert.NotNilf(t, rd, "request data is nil")
	if rd != nil {
		assert.Equal(t, "OK", rd.Text)
	}
}
