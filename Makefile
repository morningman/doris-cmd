.PHONY: install test clean help

# Default target
help:
	@echo "doris-cmd Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  install    - Install doris-cmd and its dependencies"
	@echo "  test       - Run test script"
	@echo "  clean      - Clean temporary files and build files"
	@echo "  uninstall  - Uninstall doris-cmd"
	@echo ""
	@echo "Examples:"
	@echo "  make install"
	@echo "  make test"

# Install doris-cmd and its dependencies
install:
	@echo "Installing doris-cmd and its dependencies..."
	pip install -r requirements.txt
	pip install -e .
	@echo "Installation completed!"

# Run tests
test:
	@echo "Running doris-cmd test script..."
	@echo "Note: Please ensure doris-cmd is installed and Apache Doris service is running"
	@echo "To modify test connection info, please edit the test_doris_cmd.py file"
	python test_doris_cmd.py

# Run shell test script
test-shell:
	@echo "Running Shell test script..."
	./install_and_test.sh

# Clean temporary files and build files
clean:
	@echo "Cleaning temporary files and build files..."
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf __pycache__/
	rm -rf doris_cmd/__pycache__/
	rm -f test_query.sql
	@echo "Cleaning completed!"

# Uninstall doris-cmd
uninstall:
	@echo "Uninstalling doris-cmd..."
	pip uninstall -y doris-cmd
	@echo "Uninstallation completed!" 