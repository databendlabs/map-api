all: test fmt clippy doc

test:
	cargo test

fmt:
	cargo fmt

clippy:
	cargo clippy

doc:
	cargo doc


