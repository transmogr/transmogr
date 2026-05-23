package app

import (
	"context"
	"errors"
	"testing"
)

func TestAppRunJoinsConcurrentRunnerErrors(t *testing.T) {
	t.Parallel()

	errOne := errors.New("runner one failed")
	errTwo := errors.New("runner two failed")
	release := make(chan struct{})

	a := &App{
		runners: []Runner{
			runnerFunc(func(_ context.Context) error {
				<-release
				return errOne
			}),
			runnerFunc(func(_ context.Context) error {
				<-release
				return errTwo
			}),
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- a.Run(context.Background())
	}()

	close(release)

	err := <-done
	if err == nil {
		t.Fatal("expected joined runner error")
	}
	if !errors.Is(err, errOne) {
		t.Fatalf("expected error to include %q, got %v", errOne, err)
	}
	if !errors.Is(err, errTwo) {
		t.Fatalf("expected error to include %q, got %v", errTwo, err)
	}
}
