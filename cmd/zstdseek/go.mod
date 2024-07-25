module github.com/SaveTheRbtz/zstd-seekable-format-go/cmd/zstdseek

go 1.22

replace github.com/SaveTheRbtz/zstd-seekable-format-go/pkg => ../../pkg

require (
	github.com/SaveTheRbtz/fastcdc-go v0.3.0
	github.com/SaveTheRbtz/zstd-seekable-format-go/pkg v0.0.0-20240724012851-f5b902bbf780
	github.com/klauspost/compress v1.17.9
	go.uber.org/zap v1.27.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/google/btree v1.1.2 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
)
