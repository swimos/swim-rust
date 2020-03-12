#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cp "$DIR/pre-commit" "$DIR/.git/hooks"
chmod +x "$DIR/.git/hooks/pre-commit"