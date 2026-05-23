package metrics

import (
	"context"
	"errors"
)

// ReadinessSource checks whether the service is ready to handle requests.
type ReadinessSource interface {
	Ready(context.Context) error
}

// CombineReadiness returns a ReadinessSource that reports an error if any of
// the provided sources reports an error. Nil sources are ignored.
func CombineReadiness(sources ...ReadinessSource) ReadinessSource {
	var active []ReadinessSource
	for _, s := range sources {
		if s != nil {
			active = append(active, s)
		}
	}
	switch len(active) {
	case 0:
		return nil
	case 1:
		return active[0]
	default:
		return &multiReadiness{sources: active}
	}
}

type multiReadiness struct {
	sources []ReadinessSource
}

func (m *multiReadiness) Ready(ctx context.Context) error {
	var errs []error
	for _, s := range m.sources {
		if err := s.Ready(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
