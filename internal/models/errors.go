package models

import "errors"

// ErrLeaseNotAcquired reports that a lease-scoped operation could not start because another worker owns the lease.
var ErrLeaseNotAcquired = errors.New("lease not acquired")

// ErrChangefeedCursorSourceMismatch is returned when the stored changefeed cursor source differs from the current one.
var ErrChangefeedCursorSourceMismatch = errors.New("changefeed cursor source mismatch")
