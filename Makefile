.PHONY: ut
ut:
	@go test ./...

.PHONY: tidy
tidy:
	@go mod tidy

.PHONY: clean
clean:
	@cd logs && rm wal.log

.PHONY: check
check:
	@$(MAKE) --no-print-directory tidy
	@$(MAKE) --no-print-directory ut