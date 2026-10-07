package notification

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/textproto"
	"sync/atomic"
	"time"

	"github.com/nicholas-fedor/shoutrrr/pkg/router"
	"github.com/nicholas-fedor/shoutrrr/pkg/types"
)

type httpClientFunc func(*http.Request) (*http.Response, error)

func (f httpClientFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

// ValidateService checks Shoutrrr's configuration without sending a notification.
func ValidateService(settings Settings) error {
	if err := settings.Validate(); err != nil {
		return err
	}
	if settings.URL == "" {
		return nil
	}
	if _, err := router.NewWithOptions(nil, types.SenderOptions{}, settings.URL); err != nil {
		return ErrInvalid
	}
	return nil
}

// Send never returns provider errors because they can contain the private URL.
func Send(ctx context.Context, settings Settings, message string) error {
	if settings.Validate() != nil || settings.URL == "" || len(message) == 0 || len(message) > 4096 {
		return ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone(), Timeout: 10 * time.Second, CheckRedirect: func(request *http.Request, via []*http.Request) error {
		if len(via) >= 3 || request.URL.Host != via[0].URL.Host || request.URL.Scheme != via[0].URL.Scheme {
			return http.ErrUseLastResponse
		}
		return nil
	}}
	defer client.CloseIdleConnections()
	var rejected atomic.Bool
	options := types.SenderOptions{
		Timeout: 10 * time.Second,
		HTTPClient: httpClientFunc(func(request *http.Request) (*http.Response, error) {
			response, err := client.Do(request.WithContext(ctx))
			if err == nil && (response.StatusCode < 200 || response.StatusCode >= 300) {
				response.Body.Close()
				if response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != 408 && response.StatusCode != 429 {
					rejected.Store(true)
				}
				return nil, ErrDelivery
			}
			if err == nil {
				response.Body = http.MaxBytesReader(nil, response.Body, 64<<10)
			}
			return response, err
		}),
		DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
	}
	sender, err := router.NewWithOptions(nil, options)
	if err != nil {
		return ErrInvalid
	}
	service, err := sender.Locate(settings.URL)
	if err != nil {
		return ErrInvalid
	}
	if sender, ok := service.(types.ContextSender); ok {
		err = sender.SendContext(ctx, message, &types.Params{})
	} else {
		err = service.Send(message, &types.Params{})
	}
	if err != nil {
		if status, ok := errors.AsType[*textproto.Error](err); ok && status.Code >= 500 && status.Code < 600 {
			return ErrRejected
		}
		if rejected.Load() {
			return ErrRejected
		}
		return ErrDelivery
	}
	return nil
}
