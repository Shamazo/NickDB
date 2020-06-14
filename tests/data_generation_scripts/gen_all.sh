# sample usage:
#       ./gen_all.sh


OUTPUT_TEST_DIR=/db/tests/gen_tests
DOCKER_TEST_DIR="${2:-/db/tests/gen_tests}"
TBL_SIZE="${3:-1005}"
RAND_SEED="${4:-42}"
JOIN_DIM1_SIZE="${5:-10000}"
JOIN_DIM2_SIZE="${6:-1000}"
ZIPFIAN_PARAM="${7:-1.0}"
NUM_UNIQUE_ZIPF="${8:-1000}"

echo "FILES WILL BE WRITTEN TO: $OUTPUT_TEST_DIR"
echo "DSL SCRIPTS WILL LOAD FROM THIS DOCKER DIR: $DOCKER_TEST_DIR"
echo "DSL SCRIPTS WILL USE DATA SIZE: $TBL_SIZE"
echo "DSL SCRIPTS WILL USE RANDOM SEED: $RAND_SEED"

echo "DATA GENERATION STEP BEGIN ..."

python3 /db/tests/data_generation_scripts/basic_scans.py $TBL_SIZE $RAND_SEED ${OUTPUT_TEST_DIR} ${DOCKER_TEST_DIR}
python3 /db/tests/data_generation_scripts/multicore_scans.py $TBL_SIZE $RAND_SEED ${OUTPUT_TEST_DIR} ${DOCKER_TEST_DIR}
python3 /db/tests/data_generation_scripts/indexing.py $TBL_SIZE $RAND_SEED ${OUTPUT_TEST_DIR} ${DOCKER_TEST_DIR}
python3 /db/tests/data_generation_scripts/joins.py $TBL_SIZE $JOIN_DIM1_SIZE $JOIN_DIM2_SIZE $RAND_SEED $ZIPFIAN_PARAM $NUM_UNIQUE_ZIPF ${OUTPUT_TEST_DIR} ${DOCKER_TEST_DIR}
python3 /db/tests/data_generation_scripts/updates.py $TBL_SIZE $RAND_SEED ${OUTPUT_TEST_DIR} ${DOCKER_TEST_DIR}

echo "DATA GENERATION STEP FINISHED ..."
