.PHONY: test
test:
	cargo test -- \
  		--nocapture \
  		--color=always


.PHONY: run 
run:
	cargo run
