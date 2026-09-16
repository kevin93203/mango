#!/bin/sh
# Install the Mango CLI, daemon, and shim from the latest GitHub release.
#
# Usage:
#   curl -LsSf https://raw.githubusercontent.com/kevin93203/mango/main/install.sh | sh
#
# Optional environment variables:
#   MANGO_VERSION       Release tag, for example v0.1.0 (default: latest)
#   MANGO_REPO          GitHub repository (default: kevin93203/mango)
#   MANGO_INSTALL_DIR   Installation directory (default: ~/.local/bin)
#   MANGO_NO_MODIFY_PATH=1  Do not update a shell profile

set -eu

error() {
    printf 'mango installer: error: %s\n' "$*" >&2
    exit 1
}

info() {
    printf 'mango installer: %s\n' "$*"
}

command -v curl >/dev/null 2>&1 || error "curl is required"
command -v tar >/dev/null 2>&1 || error "tar is required"

os_name=$(uname -s 2>/dev/null || true)
case "$os_name" in
    Linux) os=linux ;;
    Darwin) os=macos ;;
    *) error "unsupported operating system: ${os_name:-unknown}; use a supported Windows, Linux, or macOS release" ;;
esac

machine=$(uname -m 2>/dev/null || true)
case "$machine" in
    x86_64|amd64) architecture=amd64 ;;
    arm64|aarch64) architecture=arm64 ;;
    *) error "unsupported architecture: ${machine:-unknown}" ;;
esac

repository=${MANGO_REPO:-kevin93203/mango}
version=${MANGO_VERSION:-latest}
case "$version" in
    latest|v*) ;;
    V*) version="v${version#V}" ;;
    *) version="v$version" ;;
esac

if [ "$version" = "latest" ]; then
    release_base="https://github.com/$repository/releases/latest/download"
else
    release_base="https://github.com/$repository/releases/download/$version"
fi

archive_base="mango-$os-$architecture"
archive="$archive_base.tar.gz"
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/mango-install.XXXXXX") || error "could not create a temporary directory"
cleanup() {
    rm -rf "$tmp_dir"
}
trap cleanup EXIT

archive_path="$tmp_dir/$archive"
checksum_path="$tmp_dir/$archive_base.sha256"
package_dir="$tmp_dir/package"
mkdir -p "$package_dir"

info "downloading $archive (${version})"
curl --fail --location --silent --show-error --retry 3 \
    "$release_base/$archive" --output "$archive_path"
curl --fail --location --silent --show-error --retry 3 \
    "$release_base/$archive_base.sha256" --output "$checksum_path"

expected_hash=$(awk 'NF {print $1; exit}' "$checksum_path" | tr '[:upper:]' '[:lower:]')
case "$expected_hash" in
    ''|*[!0123456789abcdef]*) error "release checksum is invalid" ;;
esac
[ "${#expected_hash}" -eq 64 ] || error "release checksum is invalid"

if command -v sha256sum >/dev/null 2>&1; then
    actual_hash=$(sha256sum "$archive_path" | awk '{print $1}' | tr '[:upper:]' '[:lower:]')
elif command -v shasum >/dev/null 2>&1; then
    actual_hash=$(shasum -a 256 "$archive_path" | awk '{print $1}' | tr '[:upper:]' '[:lower:]')
else
    error "sha256sum or shasum is required to verify the release"
fi

[ "$actual_hash" = "$expected_hash" ] || error "release checksum mismatch"
info "release checksum verified"

tar -xzf "$archive_path" -C "$package_dir"
for binary in mango mangod mango-shim; do
    [ -f "$package_dir/$binary" ] || error "release package is missing $binary"
done
[ -f "$package_dir/manifest.json" ] || error "release package is missing manifest.json"

install_dir=${MANGO_INSTALL_DIR:-"$HOME/.local/bin"}
case "$install_dir" in
    "~"/*) install_dir="$HOME/${install_dir#\~/}" ;;
esac
case "$install_dir" in
    /*) ;;
    *) install_dir="$(pwd)/$install_dir" ;;
esac
mkdir -p "$install_dir" || error "could not create installation directory: $install_dir"
install_dir=$(cd "$install_dir" && pwd -P)

# Replace each executable atomically after the archive has been downloaded and
# verified, so a failed download never removes an existing installation.
for binary in mango mangod mango-shim; do
    staged="$install_dir/.$binary.$$"
    cp "$package_dir/$binary" "$staged"
    chmod 755 "$staged"
    mv -f "$staged" "$install_dir/$binary"
done

path_has_install_dir=0
case ":${PATH:-}:" in
    *":$install_dir:"*) path_has_install_dir=1 ;;
esac

profile_path_line() {
    # Single quotes keep characters such as '$' literal when a custom install
    # directory is written to a shell profile.
    quoted=$(printf '%s' "$install_dir" | sed "s/'/'\\\\''/g")
    printf "export PATH='%s':\$PATH" "$quoted"
}

add_profile_path() {
    profile=$1
    line=$2
    marker='# Mango installer'
    if [ -f "$profile" ] && grep -Fq "$marker" "$profile" 2>/dev/null; then
        return 0
    fi
    if ! mkdir -p "$(dirname "$profile")" 2>/dev/null || ! {
        printf '\n%s\n%s\n' "$marker" "$line" >> "$profile"
    } 2>/dev/null; then
        info "could not update $profile; add $install_dir to PATH manually"
        return 0
    fi
    info "added $install_dir to PATH in $profile"
}

if [ "$path_has_install_dir" -eq 0 ]; then
    no_modify_path=${MANGO_NO_MODIFY_PATH:-0}
    case "$no_modify_path" in
        1|true|TRUE|yes|YES|on|ON)
            info "PATH was not modified (MANGO_NO_MODIFY_PATH is set)"
            ;;
        *)
            shell_name=${SHELL##*/}
            line=$(profile_path_line)
            case "$shell_name" in
                zsh) add_profile_path "$HOME/.zshrc" "$line" ;;
                bash) add_profile_path "$HOME/.bashrc" "$line" ;;
                fish)
                    info "add $install_dir to PATH in ~/.config/fish/config.fish before using Mango"
                    ;;
                *) add_profile_path "$HOME/.profile" "$line" ;;
            esac
            info "restart your shell or run: export PATH='$install_dir':\$PATH"
            ;;
    esac
fi

info "installed Mango in $install_dir"
"$install_dir/mango" --version || true
info "run 'mango init' to create a configuration, or 'mango --help' to get started"
