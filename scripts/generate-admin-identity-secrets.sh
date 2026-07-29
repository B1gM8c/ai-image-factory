#!/bin/sh

set -eu

usage() {
  echo "usage: $0 /absolute/output/directory [key-id]" >&2
  exit 64
}

fail() {
  echo "error: $*" >&2
  exit 1
}

[ "$#" -ge 1 ] && [ "$#" -le 2 ] || usage

output_dir=$1
key_id=${2:-admin-es256-v1}

case "$output_dir" in
  /*) ;;
  *) fail "output directory must be absolute" ;;
esac

case "$key_id" in
  "" | *[!A-Za-z0-9._-]*) fail "key id may contain only letters, digits, dot, underscore, and hyphen" ;;
esac

command -v openssl >/dev/null 2>&1 || fail "openssl is required"
[ ! -L "$output_dir" ] || fail "output directory must not be a symbolic link"

umask 077
mkdir -p -- "$output_dir"
[ -d "$output_dir" ] || fail "output path is not a directory"

private_key="$output_dir/admin-jwt-es256-private.pem"
public_key="$output_dir/admin-jwt-es256-public.pem"
refresh_peppers="$output_dir/refresh-token-peppers"

for path in "$private_key" "$public_key" "$refresh_peppers"; do
  if [ -e "$path" ] || [ -L "$path" ]; then
    fail "refusing to overwrite $path"
  fi
done

cleanup() {
  rm -f -- "$private_key" "$public_key" "$refresh_peppers"
}
trap cleanup EXIT HUP INT TERM

# jsonwebtoken requires a named P-256 curve for ES256. OpenSSL's default
# parameter encoding is not portable across every supported runtime.
openssl genpkey \
  -algorithm EC \
  -pkeyopt ec_paramgen_curve:P-256 \
  -pkeyopt ec_param_enc:named_curve \
  -out "$private_key"
openssl pkey -in "$private_key" -pubout -out "$public_key"
printf '1:%s\n' "$(openssl rand -hex 32)" >"$refresh_peppers"

chmod 600 "$private_key" "$public_key" "$refresh_peppers"
openssl pkey -in "$private_key" -noout >/dev/null
openssl pkey -pubin -in "$public_key" -noout >/dev/null

trap - EXIT HUP INT TERM

echo "Admin identity material created:"
printf '  GATEWAY_JWT_ACTIVE_KID=%s\n' "$key_id"
printf '  GATEWAY_JWT_PRIVATE_KEY_PATH=%s\n' "$private_key"
printf '  GATEWAY_JWT_PUBLIC_KEYS=%s:%s\n' "$key_id" "$public_key"
printf '  GATEWAY_REFRESH_TOKEN_CURRENT_PEPPER_VERSION=1\n'
printf '  GATEWAY_REFRESH_TOKEN_PEPPERS_PATH=%s\n' "$refresh_peppers"
