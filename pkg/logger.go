package seekable

import (
	"log/slog"
)

var discardLogger = slog.New(slog.DiscardHandler)
