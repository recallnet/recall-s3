clean:
	cargo clean

lint: \
	check-fmt \
	check-clippy

check-fmt:
	cargo fmt --all --check

check-clippy:
	cargo clippy --no-deps -- -D clippy::all