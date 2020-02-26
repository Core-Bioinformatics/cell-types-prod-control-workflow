#!/usr/bin/env bash

PROD_WORKFLOW_ROOT="$PWD"
PROD_WORKFLOWS="$PWD/cell-types-prod-workflows"

# Clone or update the cell-types-eval-workflows repo containing submodules for individual pipelines
if [ ! -d 'cell-types-eval-workflows' ]; then
    git clone --recursive https://github.com/ebi-gene-expression-group/cell-types-prod-workflows $PROD_WORKFLOWS
fi

pushd $PROD_WORKFLOWS > /dev/null
git checkout origin/develop > /dev/null
git pull origin develop > /dev/null
git submodule update --recursive --remote > /dev/null
popd > /dev/null
