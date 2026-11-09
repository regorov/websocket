package websocket

import (
	"errors"
	"fmt"
)

type Error struct {
	Status StatusCode
	Reason string
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Status, e.Reason)
}

// ErrConnClosed is returned when writing to a connection that is closing or closed.
var ErrConnClosed = errors.New("websocket: connection closed")
