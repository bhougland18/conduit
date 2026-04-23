{
  description = "Conduit development shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    crane.url = "github:ipetkov/crane";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      flake-parts,
      nixpkgs,
      crane,
      fenix,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" ];

      imports = [ ./nix/devshell ];

      perSystem =
        { system, ... }:
        let
          pkgs = import nixpkgs {
            inherit system;
            config = {
              allowUnfree = true;
              android_sdk.accept_license = true;
            };
          };
          fenixToolchain = fenix.packages.${system}.stable.withComponents [
            "cargo"
            "clippy"
            "rust-src"
            "rustc"
            "rustfmt"
          ];
          dylintNightlyBase = fenix.packages.${system}.toolchainOf {
            channel = "nightly";
            date = "2025-09-18";
            sha256 = "sha256-JuyNmA7iixvGBDN+0DpivQofDODFrd2qh+kE4B3X3I8=";
          };
          dylintNightlyToolchain = dylintNightlyBase.withComponents [
            "cargo"
            "clippy"
            "llvm-tools-preview"
            "rust-src"
            "rustc"
            "rustc-dev"
            "rustfmt"
          ];
          fenixRustSrc = "${fenix.packages.${system}.stable.rust-src}/lib/rustlib/src/rust/library";
          craneLib = crane.mkLib pkgs;
          rustupShim = pkgs.writeShellScriptBin "rustup" ''
            find_toolchain_file() {
              local dir
              dir="$PWD"

              while [ "$dir" != "/" ]; do
                if [ -f "$dir/rust-toolchain.toml" ]; then
                  printf '%s\n' "$dir/rust-toolchain.toml"
                  return 0
                fi

                if [ -f "$dir/rust-toolchain" ]; then
                  printf '%s\n' "$dir/rust-toolchain"
                  return 0
                fi

                dir="$(dirname "$dir")"
              done

              return 1
            }

            read_channel() {
              local toolchain_file
              toolchain_file="$1"

              if grep -q '^\[toolchain\]' "$toolchain_file"; then
                sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$toolchain_file" | head -n 1
                return 0
              fi

              head -n 1 "$toolchain_file"
            }

            toolchain_file="$(find_toolchain_file || true)"

            if [ -n "$toolchain_file" ]; then
              toolchain="$(read_channel "$toolchain_file")"
            else
              toolchain=""
            fi

            if [ -z "$toolchain" ]; then
              toolchain="stable"
            fi

            if [ "$1" = "show" ] && [ "$2" = "active-toolchain" ]; then
              printf '%s (default)\n' "$toolchain"
              exit 0
            fi

            if [ "$1" = "which" ] && [ "$2" = "rustc" ]; then
              command -v rustc
              exit 0
            fi

            echo "This dev shell provides a minimal rustup shim for Dylint consumption." >&2
            echo "Supported commands: rustup show active-toolchain, rustup which rustc" >&2
            exit 1
          '';
          dylintLinkNightly = pkgs.writeShellScriptBin "dylint-link-nightly" ''
            export RUSTUP_TOOLCHAIN="''${RUSTUP_TOOLCHAIN:-nightly-2025-09-18}"
            exec dylint-link "$@"
          '';
          cargoDylintNightly = pkgs.writeShellScriptBin "cargo-dylint-nightly" ''
            export PATH="${dylintNightlyToolchain}/bin:${dylintLinkNightly}/bin:$PATH"
            export RUSTUP_TOOLCHAIN="nightly-2025-09-18"
            export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="dylint-link-nightly"
            export DYLINT_DRIVER_PATH="/home/ben/code/highland-labs-lints/.cache/dylint-drivers"
            export DYLINT_DRIVER_BUILD_ROOT="/home/ben/code/highland-labs-lints/.cache/dylint-driver-build"
            exec cargo dylint "$@"
          '';
        in
        {
          _module.args = {
            inherit pkgs fenixToolchain fenixRustSrc craneLib;
          };

          dendritic.devShell = {
            description = "Conduit ACFS development shell";
            env.RUSTUP_TOOLCHAIN = "nightly-2025-09-18";
            packages = [
              cargoDylintNightly
              dylintLinkNightly
              rustupShim
            ];

            features = {
              acfs.enable = true;
              direnv.enable = true;
              jujutsu.enable = true;
              quarto.enable = true;
              rust.enable = true;
              rust_devtools.enable = true;
              rust_lint_dylint.enable = true;

              android.enable = false;
              cargo_polylith.enable = false;
              crane.enable = false;
              flutter.enable = false;
              rinf.enable = false;
              stac.enable = false;
            };
          };
        };
    };
}
