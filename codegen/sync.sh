#!/usr/bin/env bash
# Pull the latest upstream inputs and regenerate the SDK.
#
# Usage: codegen/sync.sh [python-sdk-ref]   (default: latest release tag)
#
# Fetches:
#   - the OpenAPI spec from https://api.onkernel.com/spec.json
#   - api.md from github.com/kernel/kernel-python-sdk at the given ref
# Then regenerates the code, formats it, and records the versions in
# upstream/VERSION.json. The crate version follows the Python SDK version.
set -euo pipefail

cd "$(dirname "$0")/.."

repo="kernel/kernel-python-sdk"
ref="${1:-}"
if [[ -z "$ref" ]]; then
  ref="$(gh api "repos/$repo/releases/latest" --jq .tag_name)"
fi
commit="$(gh api "repos/$repo/commits/$ref" --jq .sha)"
version="$(gh api "repos/$repo/contents/pyproject.toml?ref=$commit" --jq .content | base64 --decode \
  | sed -n 's/^version = "\(.*\)"/\1/p' | head -1)"

curl -fsSL https://api.onkernel.com/spec.json | python3 -c \
  'import json,sys; json.dump(json.load(sys.stdin), sys.stdout, indent=2, sort_keys=True); print()' \
  > upstream/openapi.json
gh api "repos/$repo/contents/api.md?ref=$commit" --jq .content | base64 --decode > upstream/api.python.md

spec_sha="$(python3 -c "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" upstream/openapi.json)"
cat > upstream/VERSION.json <<JSON
{
  "python_sdk_version": "$version",
  "python_sdk_commit": "$commit",
  "spec_sha256": "$spec_sha"
}
JSON

# Keep the crate version in step with the first-party SDKs.
python3 - "$version" <<'PY'
import re, sys
p = "Cargo.toml"
s = open(p).read()
s = re.sub(r'^version = ".*"$', f'version = "{sys.argv[1]}"', s, count=1, flags=re.M)
open(p, "w").write(s)
PY

python3 codegen/generate.py
cargo fmt

echo "synced to $repo@$ref ($version, ${commit:0:7})"
