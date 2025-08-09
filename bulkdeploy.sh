#!/bin/bash
POWERSHELL_LAYER="LayerVersion=7"
PYSTHON_LAYER="LayerVersion=7"
PWSH_DEPLOY_CMD="sam deploy --parameter-overrides ${POWERSHELL_LAYER}"
PY_DEPLOY_CMD="sam deploy --parameter-overrides ${PYSTHON_LAYER}"

# powershell
(cd powershell/M365Common; ${PWSH_DEPLOY_CMD} &)  
(cd powershell/M365GetUser; ${PWSH_DEPLOY_CMD} &) 
(cd powershell/M365GetGroup; ${PWSH_DEPLOY_CMD} &)

# python
(cd python/M365Common; ${PY_DEPLOY_CMD} &)
(cd python/M365ConvUser; ${PY_DEPLOY_CMD} &) 
(cd python/M365ConvGroup; ${PY_DEPLOY_CMD} &)
(cd python/S3TOPG; ${PY_DEPLOY_CMD} &)