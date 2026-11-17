# Builds the duckwake extension the way DuckDB's community extension CI does: its workflow checks out
# duckdb/extension-ci-tools into ./extension-ci-tools and runs `make configure`, `make release` and the tests
# here. For a local build without those tools, use scripts/build_extension.sh, which writes the same footer.
#
# The library is built with `cargo build -p duckwake_extension`, not the workspace's default members: building the
# extension (which links against the host's DuckDB) together with the CLI (which bundles DuckDB) would unify the
# two DuckDB builds. It uses DuckDB's stable C API pinned to v1.5.6, which DuckDB 2.0 loads.
.PHONY: all configure debug release test test_debug test_release clean clean_all \
	build_extension_library_debug build_extension_library_release

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=duckwake
USE_UNSTABLE_C_API=0
TARGET_DUCKDB_VERSION=v1.5.6

all: configure release

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile

# The cargo target for this platform: macOS builds name it, since the CI cross-compiles osx_amd64 on arm64.
ifeq ($(DUCKDB_PLATFORM),osx_amd64)
	CARGO_TARGET=--target x86_64-apple-darwin
	CARGO_OUT=target/x86_64-apple-darwin
else ifeq ($(DUCKDB_PLATFORM),osx_arm64)
	CARGO_TARGET=--target aarch64-apple-darwin
	CARGO_OUT=target/aarch64-apple-darwin
else
	CARGO_TARGET=
	CARGO_OUT=target
endif
# What cargo names the library of the duckwake_extension crate; the CI expects lib$(EXTENSION_NAME).
ifeq ($(shell uname -s),Darwin)
	CARGO_LIB=libduckwake_extension.dylib
else
	CARGO_LIB=libduckwake_extension.so
endif

build_extension_library_debug: check_configure
	cargo build -p duckwake_extension $(CARGO_TARGET)
	$(PYTHON_VENV_BIN) -c "from pathlib import Path;Path('$(EXTENSION_BUILD_PATH)/debug/extension/$(EXTENSION_NAME)').mkdir(parents=True, exist_ok=True)"
	$(PYTHON_VENV_BIN) -c "import shutil;shutil.copyfile('$(CARGO_OUT)/debug/$(CARGO_LIB)', '$(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_LIB_FILENAME)')"

build_extension_library_release: check_configure
	cargo build --release -p duckwake_extension $(CARGO_TARGET)
	$(PYTHON_VENV_BIN) -c "from pathlib import Path;Path('$(EXTENSION_BUILD_PATH)/release/extension/$(EXTENSION_NAME)').mkdir(parents=True, exist_ok=True)"
	$(PYTHON_VENV_BIN) -c "import shutil;shutil.copyfile('$(CARGO_OUT)/release/$(CARGO_LIB)', '$(EXTENSION_BUILD_PATH)/release/$(EXTENSION_LIB_FILENAME)')"

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_release
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build
	cargo clean -p duckwake_extension
clean_all: clean_configure clean
