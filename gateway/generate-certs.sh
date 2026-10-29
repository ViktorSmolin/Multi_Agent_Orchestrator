#!/bin/sh
set -eu

cert_dir="${CERT_DIR:-/certs}"
mkdir -p "$cert_dir"

if [ -s "$cert_dir/server.crt" ] && [ -s "$cert_dir/server.key" ]; then
  echo "TLS certificate already present"
  exit 0
fi

openssl req -x509 -nodes -newkey rsa:2048 -sha256 -days 825 \
  -keyout "$cert_dir/server.key" \
  -out "$cert_dir/server.crt" \
  -subj "/C=RU/ST=Local/L=Local/O=MAO/OU=Dev/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:gateway,IP:127.0.0.1"

chmod 600 "$cert_dir/server.key"
echo "generated self-signed TLS certificate"
