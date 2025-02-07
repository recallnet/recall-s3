default:
	cargo build --locked --release --features="binary"
	./target/release/recall_s3 --version

install:
	cargo install --locked --path lib/recall_s3 --features="binary"

clean:
	cargo clean

lint: \
	check-fmt \
	check-clippy

check-fmt:
	cargo fmt --all --check

check-clippy:
	cargo clippy --no-deps -- -D clippy::all