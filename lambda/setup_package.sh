#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
set -u
# Exit status of a pipeline is the exit status of the last command to exit with a non-zero status,
# or zero if no command exited with a non-zero status.
set -o pipefail

#### INPUTS
# $PYTHON: python script
# $VENV_PATH: path where venv should be created
# $PACKAGE_PATH: where all files that will end up on the zip should be placed
# $REQUIREMENTS_FILE: python's requirements.txt file
# $SOURCE: lambda code source folder
# $LIBPATH: path to faashceduler lib

# Define variables if not already defined
SCRIPT_DIR=$(dirname -- "$(readlink -f -- "${BASH_SOURCE[0]}")")

: ${PYTHON:="python"}
: ${VENV_PATH:="$(readlink -f -- "${SCRIPT_DIR}/pkg_venv")"}
: ${PACKAGE_PATH:="$(readlink -f -- "${SCRIPT_DIR}/../lambdabin")"}
: ${REQUIREMENTS_FILE:="$(readlink -f -- "${SCRIPT_DIR}/../requirements_lambda.txt")"}
: ${SOURCE:="$(readlink -f -- "${SCRIPT_DIR}/src")"}
: ${LIBPATH:="$(readlink -f -- "${SCRIPT_DIR}/..")"}

# Dependency check
required_commands=("zip" "${PYTHON}")

for cmd in "${required_commands[@]}"; do
  if ! command -v "$cmd" &> /dev/null; then
    echo "Error: Required command '$cmd' is not installed. Please install it to continue." >&2
    exit 1
  fi
done

echo "SUMMARY: This script will use the python binary \"${PYTHON}\" to create a virtual environment at \"${VENV_PATH}\". It will then install dependencies from \"${REQUIREMENTS_FILE}\". Finally, it will prepare the deployment package at \"${PACKAGE_PATH}\" using the source code from \"${SOURCE}\" and the faasscheduler library from \"${LIBPATH}\"."
echo ""

# Confirm actions
read -p "WARNING: this will CLEAR \"${PACKAGE_PATH}\" and \"${VENV_PATH}\", do you wish to continue? (y/N): " response
if [[ "$response" == "y" || "$response" == "Y" ]]; then
    echo "Continuing with the script..."
else
    echo "Operation cancelled by user. Exiting."
    exit 1
fi

# Stars building package
echo "INFO: preparing package"

# Cleanup previous code
echo "INFO: cleaning previous data"
rm -rf $PACKAGE_PATH

# Venv
echo "INFO: preparing venv '$VENV_PATH' with python '$PYTHON'"
env $PYTHON -m venv $VENV_PATH # TODO: use varible for python version and venv
source $VENV_PATH/bin/activate
echo "INFO: venv activated"

# Requirements
echo "INFO: installing dependencies on '$REQUIREMENTS_FILE'"
pip install -r $REQUIREMENTS_FILE
pip install $LIBPATH
# Remove already present dependencies
pip uninstall -y setuptools pip botocore 

echo "INFO: dependencies installed"

# Generate package
echo "INFO: copying venv and code from '$SOURCE' to '$PACKAGE_PATH'"
SITE_PACKAGES_PATH=$("$PYTHON" -c "import sysconfig; print(sysconfig.get_path('platlib'))")
mkdir -p $PACKAGE_PATH
cp -r $SITE_PACKAGES_PATH/. $PACKAGE_PATH
# Clean folders
pushd $PACKAGE_PATH
# This remove tests folders from dependencies to reduce zip size
find . -type d -name 'tests' -exec rm -rf {} + 
popd
# Copy remaining files
cp -r $SOURCE/. $PACKAGE_PATH

# Generate zip file
pushd $PACKAGE_PATH
zip -r lambda_function_payload.zip .
popd



# cleanup
echo "INFO: all done, cleaning up"
deactivate
rm -rf $VENV_PATH