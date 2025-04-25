.PHONY: vet
vet:
	@go vet ./...

.PHONY: ut
ut:
	@go test ./...

.PHONY: tidy
tidy:
	@go mod tidy

.PHONY: clean
clean:
	@rm -f wal.test
	@cd logs && rm wal.log && rm -f mem.pprof

.PHONY: check
check:
	@$(MAKE) --no-print-directory tidy
	@$(MAKE) --no-print-directory vet
	@$(MAKE) --no-print-directory ut