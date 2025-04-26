DIR := logs

ifeq ($(wildcard $(DIR)),)
$(shell mkdir -p $(DIR))
endif


.PHONY: vet
vet:
	@go vet ./...

.PHONY: ut
ut:
	@go test ./...

.PHONY: tidy
tidy:
	@go mod tidy

.PHONY: check
check:
	@$(MAKE) --no-print-directory tidy
	@$(MAKE) --no-print-directory vet
	@$(MAKE) --no-print-directory ut

clean:
	@rm -f wal.test
	@cd logs && rm -rf *