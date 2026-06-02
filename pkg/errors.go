package seekable

import "errors"

// ErrClosed is returned by operations after a Reader, Writer, or Encoder has
// been closed or finalized.
var ErrClosed = errors.New("seekable: closed")
