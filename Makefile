.PHONY: test
test:
	cargo test -- \
  		--nocapture \
  		--color=always
