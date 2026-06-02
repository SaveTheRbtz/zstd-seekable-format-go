package seekable

import "log/slog"

// LoggerOption configures the logger used by Reader, Writer, and Encoder internals.
// It can be passed anywhere a WriterOption or ReaderOption is accepted.
type LoggerOption interface {
	WriterOption
	ReaderOption
}

// WithLogger sets the logger used by Reader, Writer, and Encoder internals.
//
// Passing nil restores the default discard logger.
func WithLogger(l *slog.Logger) LoggerOption {
	if l == nil {
		l = discardLogger
	}
	return loggerOption{logger: l}
}

type loggerOption struct {
	logger *slog.Logger
}

func (o loggerOption) applyWriter(w *writerImpl) error {
	w.logger = o.logger
	return nil
}

func (o loggerOption) applyReader(r *readerImpl) error {
	r.logger = o.logger
	return nil
}
