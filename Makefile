test:	
	cargo test -- \
  		--nocapture \
  		--color=always

doc:
	cargo doc \
		--no-deps \
		--target-dir docs
