#!/usr/bin/env sh

set -e

exec docker run --rm \
  --env "DATABRICKS_BUNDLE_ENV=${DATABRICKS_BUNDLE_ENV}" \
  --env "DATABRICKS_HOST=${DATABRICKS_HOST}" \
  --env "DATABRICKS_AUTH_TYPE=github-oidc" \
  --env "DATABRICKS_CLIENT_ID=${DATABRICKS_CLIENT_ID}" \
  --env "ACTIONS_ID_TOKEN_REQUEST_URL=${ACTIONS_ID_TOKEN_REQUEST_URL}" \
  --env "ACTIONS_ID_TOKEN_REQUEST_TOKEN=${ACTIONS_ID_TOKEN_REQUEST_TOKEN}" \
  --volume ${GITHUB_ACTION_PATH}/entrypoint.sh:/opt/sbin/entrypoint \
  --volume ${GITHUB_WORKSPACE}/${DATABRICKS_BUNDLE_DIRECTORY}:/bundle \
  --entrypoint /opt/sbin/entrypoint \
  --workdir /bundle \
  ${DATABRICKS_BUNDLE_IMAGE_REF} bundle ${1}
