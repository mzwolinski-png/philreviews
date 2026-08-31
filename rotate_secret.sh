#!/usr/bin/env bash
# One-time secret-rotation helper for PhilReviews.
# Updates a secret everywhere it lives: .env, all LaunchAgent plists that
# embed it, and (optionally) the Fly secret. The new value is read from a
# hidden prompt and passed via the environment (never argv/history/this repo).
#
# Usage:
#   ./rotate_secret.sh ANTHROPIC_API_KEY
#   ./rotate_secret.sh FLY_ACCESS_TOKEN
#   ./rotate_secret.sh GMAIL_APP_PASSWORD --fly   # also: fly secrets set
#
# After it runs, reload the affected LaunchAgents (it prints the commands).
set -euo pipefail

KEY="${1:?usage: rotate_secret.sh SECRET_NAME [--fly]}"
DO_FLY="${2:-}"
ROOT="$(cd "$(dirname "$0")" && pwd)"
LA="$HOME/Library/LaunchAgents"
ENV_FILE="$ROOT/.env"

printf 'Paste new value for %s (hidden): ' "$KEY"
read -rs VAL; echo
[ -z "$VAL" ] && { echo "empty value — aborting"; exit 1; }
export VAL KEY

changed=()

# 1) .env
if [ -f "$ENV_FILE" ] && grep -q "^$KEY=" "$ENV_FILE"; then
  python3 - "$ENV_FILE" <<'PY'
import os, sys
f = sys.argv[1]; key = os.environ['KEY']; val = os.environ['VAL']
lines = open(f, encoding='utf-8').read().splitlines()
open(f, 'w', encoding='utf-8').write(
    "\n".join((f"{key}={val}" if l.startswith(key + "=") else l) for l in lines) + "\n")
PY
  changed+=(".env")
fi

# 2) every plist that embeds this key (rewrites in place; mode preserved)
for p in "$LA"/com.philreviews.*.plist; do
  [ -e "$p" ] || continue
  PLIST="$p" python3 - <<'PY'
import os, plistlib
f = os.environ['PLIST']; key = os.environ['KEY']; val = os.environ['VAL']
with open(f, 'rb') as fh:
    d = plistlib.load(fh)
ev = d.get('EnvironmentVariables', {})
if key in ev:
    ev[key] = val
    with open(f, 'wb') as fh:
        plistlib.dump(d, fh)
PY
  if /usr/libexec/PlistBuddy -c "Print :EnvironmentVariables:$KEY" "$p" >/dev/null 2>&1; then
    changed+=("$(basename "$p")")
  fi
done

unset VAL

# 3) optional Fly secret
if [ "$DO_FLY" = "--fly" ]; then
  echo "Run this yourself to update Fly (triggers an app restart):"
  echo "  fly secrets set $KEY=<new-value> -a philreviews"
fi

echo
echo "Updated $KEY in: ${changed[*]:-<nothing matched>}"
echo "Now reload the affected LaunchAgents:"
for p in "$LA"/com.philreviews.*.plist; do
  [ -e "$p" ] || continue
  if /usr/libexec/PlistBuddy -c "Print :EnvironmentVariables:$KEY" "$p" >/dev/null 2>&1; then
    label="$(basename "$p" .plist)"
    echo "  launchctl unload \"$p\" && launchctl load \"$p\"   # $label"
  fi
done
