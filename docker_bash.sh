#!/bin/bash
BASE_DIR=$(pwd)
docker build --tag=db  .
docker container run \
		-v ${BASE_DIR}/src:/db/src \
		-v ${BASE_DIR}/infra_scripts:/db/infra_scripts \
		-v ${BASE_DIR}/test_outputs:/db/test_outputs \
		-v ${BASE_DIR}/tests:/db/tests \
		-v ${BASE_DIR}/data:/db/data \
		 --rm -it --privileged db bash
