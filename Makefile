# Makefile - Recursive build for all subfolders

.PHONY: all clean

all:
	@for dir in $$(find . -maxdepth 2 -type f -name 'Makefile' ! -path './Makefile' -exec dirname {} \;); do \
		echo "Building in $$dir..."; \
		$(MAKE) -C $$dir; \
	done

clean:
	@for dir in $$(find . -maxdepth 2 -type f -name 'Makefile' ! -path './Makefile' -exec dirname {} \;); do \
		echo "Cleaning in $$dir..."; \
		$(MAKE) -C $$dir clean; \
	done