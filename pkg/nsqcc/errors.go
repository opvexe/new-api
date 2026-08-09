package nsqcc

import "errors"

var (
	ErrTimeout      = errors.New("action timed out")
	ErrTypeClosed   = errors.New("type was closed")
	ErrNotConnected = errors.New("not connected to target source or sink")
)
