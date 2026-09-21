BUILD ?= build
TYPE  ?= Release

.PHONY: all build test bench fmt clean

all: build

build:
	cmake -B $(BUILD) -DCMAKE_BUILD_TYPE=$(TYPE) -G Ninja \
	      -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
	cmake --build $(BUILD) -- -j$(shell nproc 2>/dev/null || sysctl -n hw.logicalcpu)
	@ln -sf $(BUILD)/compile_commands.json compile_commands.json 2>/dev/null || true

test: build
	cd $(BUILD) && ctest --output-on-failure -j4

bench: build
	./bench/run.sh BUILD=$(BUILD)

fmt:
	@command -v clang-format >/dev/null 2>&1 || \
	  { echo "clang-format not found — brew install clang-format"; exit 1; }
	find src include test bench tools \
	  -name '*.cpp' -o -name '*.hpp' | xargs clang-format -i

clean:
	rm -rf $(BUILD) compile_commands.json
