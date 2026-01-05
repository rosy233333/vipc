# Copied and modified from https://github.com/AsyncModules/vsched/blob/e19b572714a6931972f1428e42d43cc34bcf47f2/Makefile
ARCH ?= riscv64
MODE ?= release
TARGET_DIR ?= $(PWD)/target
UTEST_BIN ?= $(TARGET_DIR)/$(TARGET)/$(MODE)/test_vqueue
LOG ?= error

# Target
ifeq ($(ARCH), x86_64)
  TARGET := x86_64-unknown-linux-musl
else ifeq ($(ARCH), aarch64)
	TARGET := aarch64-unknown-linux-musl
else ifeq ($(ARCH), riscv64)
  TARGET := riscv64gc-unknown-linux-musl
else
  $(error "ARCH" must be one of "x86_64", "riscv64" or "aarch64")
endif

# Linker
ifeq ($(ARCH), x86_64)
  LINKER := x86_64-linux-musl-ld
else ifeq ($(ARCH), aarch64)
	LINKER := aarch64-linux-musl-ld
else ifeq ($(ARCH), riscv64)
  LINKER := riscv64-linux-musl-ld
else
  $(error "ARCH" must be one of "x86_64", "riscv64" or "aarch64")
endif

build_args-release := --release

clean:
	rm -rf $(TARGET_DIR)

run:
	RUST_BACKTRACE=1 RUSTFLAGS='-C target-feature=+crt-static' cargo --config target.$(TARGET).linker=\"$(LINKER)\" build --target $(TARGET) --target-dir $(TARGET_DIR) $(build_args-$(MODE))
	RUST_LOG=$(LOG) qemu-$(ARCH) -D qemu.log -d in_asm,int,mmu,pcall,cpu_reset,page,guest_errors $(UTEST_BIN)
