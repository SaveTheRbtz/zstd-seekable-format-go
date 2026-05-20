package seekable

import (
	"context"
	"log/slog"
)

type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool {
	return false
}

func (discardHandler) Handle(context.Context, slog.Record) error {
	return nil
}

func (discardHandler) WithAttrs([]slog.Attr) slog.Handler {
	return discardHandler{}
}

func (discardHandler) WithGroup(string) slog.Handler {
	return discardHandler{}
}

var discardLogger = slog.New(discardHandler{})
