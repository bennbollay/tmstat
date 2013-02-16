#!/bin/bash
OBJ_DIR=.

rm -fr ${OBJ_DIR}/test_data
mkdir -p ${OBJ_DIR}/test_data
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --merge-test="1x2: [ 1+1 1 0   0% ] [ 1+1 1 0 100% ]"
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --merge-test="2x3: [ 1+1 1 0   0% ] [ 1+1 1 0  50% ] [ 1+1 1 0 100% ]"
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --merge-test="4x1: [ 1+1 1 0  50% ]"
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --test=single
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --test=multi
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --test=merge
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --test=reread
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --test=rollup
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --test=bugs
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --test=parse-print
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --test=long-keys
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --test=unterminated-keys
${OBJ_DIR}/tmstat_test --base=${OBJ_DIR}/test_data --test=insn
sh test-eval.sh ${OBJ_DIR}
touch ${OBJ_DIR}/test_data/pass

