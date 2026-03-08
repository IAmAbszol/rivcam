#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3.11}"
VENV_DIR="${VENV_DIR:-.venv311}"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "error: ${PYTHON_BIN} not found on PATH" >&2
  exit 1
fi

"${PYTHON_BIN}" -m venv "${VENV_DIR}"
# shellcheck disable=SC1090
source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[tests,dev]"
python -m pip install -r scripts/requirements.txt

echo "Bootstrap complete. Activate with: source ${VENV_DIR}/bin/activate"
echo "Smoke check: rivcam all recordings/OffRoading --template scripts/default_template.json"
