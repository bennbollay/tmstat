/*
 * Copyright (c) 2013, F5 Networks, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: 
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer. 
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution. 
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * The views and conclusions contained in the software and documentation are
 * those of the authors and should not be interpreted as representing official
 * policies, either expressed or implied, of F5 Networks, Inc.
 *
 * Statistics subsystem unit tests.
 */
#define _GNU_SOURCE
#include <assert.h>
#include <err.h>
#include <getopt.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <time.h>
#include <ctype.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

#include <errno.h>

#include "tmstat.h"

#define SIZE             1024
#define SMALL_SIZE        128

#define array_count(a)      (sizeof(a) / sizeof((a)[0]))

#define TMCTL_MIN(a, b)                \
    ({ typeof(a) _a = (a);             \
        typeof(b) _b = (b);            \
        _a < _b ? _a : _b; })
#define TMCTL_MAX(a, b)                \
    ({ typeof(a) _a = (a);             \
        typeof(b) _b = (b);            \
        _a > _b ? _a : _b; })

#define rdtsc() ({                      \
    register long long __X;             \
    asm volatile("rdtsc" : "=A" (__X)); \
    __X; })

extern THREAD char *tmstat_path;

/*
 * Usage text (please sort options list alphabetically).
 */
static const char usage_str[] = 
   "usage: %1$s [OPTIONS] <--merge-test=PARAMS | --test=TEST>...\n"
   "\n"
   "Statistics subsystem unit tests.\n"
   "\n"
   "Supported options:\n"
   "   -b, --base=PATH          Set segment directory base path.\n"
   "   -d, --directory=DIR      Subscribe to specific directory.\n"
   "   -f, --file=PATH          Inspect specific segment file.\n"
   "   -h, --help               Display this text.\n"
   "   -M, --max-errors=N       Display at most N error reports from tests.\n"
   "   -T, --merge-test=PARAMS  Perform a merge test specifying parameters.\n"
   "   -s, --seed=S             Use PRNG seed S.\n"
   "   -t, --test=TEST          Run subsystem test:\n"
   "              single        Single-segment allocation test.\n"
   "              multi         Multi-segment allocation test.\n"
   "              merge         Multi-segment merge test.\n"
   "              rndmrg        Merge test of random intensity.\n"
   "              mrgperf       Merge performance test.\n"
   "              parse-print   Test parsing and printing functions.\n"
   "              eval          Create segment for eval test.\n"
   "              reread        Test auto re-read of directory.\n"
   "              bugs          Exercise bug fixes.\n"
   "              cmod          (Sort of) concurrent read/write test.\n"
   "              rollup        Test rollup queries.\n"
   "              insn          Test by-n row creation.\n"
   "   -v, --verbose            Be verbose.\n"
   "\n"
   "For --merge-test, the argument should be like this example:\n"
   "\n"
   "    SxT: [K+V U R P%%] ...\n"
   "\n"
   "The first two numbers' meanings are:\n"
   "\n"
   "    S:  Number of segments.\n"
   "    T:  Number of tables in each segment.\n"
   "\n"
   "The numbers in the brackets apply to one table.  There must be as many\n"
   "bracketed groups of numbers as there are tables.  The meanings for the\n"
   "five bracketed numbers are:\n"
   "\n"
   "    K:  Number of key columns.\n"
   "    V:  Number of value columns (columns with a merge rule).\n"
   "    U:  Number of unique keys that may enter the table.\n"
   "    R:  1/0: Insert keys in random order?\n"
   "    P:  Percent chance of inserting each key in a segment.\n"
   "\n"
   "For example, to test with two segments and two tables where table 1 has\n"
   "one key column, three value columns, 100 keys, randomized insertion,\n"
   "and 100%% probability of insertion; and table 2 has two key columns,\n"
   "six value columns, 200 keys, ordered insertion, and 50%% probability\n"
   "of insertion, you would say the following.\n"
   "\n"
   "    %1$s --merge-test='2x2: [1+3 100 1 100%%] [2+6 200 0 50%%]'\n"
;

/*
 * Options (please sort options alphabetically).
 */
enum {
    OPT_BASE,
    OPT_DIR,
    OPT_FILE,
    OPT_HELP,
    OPT_MAX_ERRORS,
    OPT_MERGE_TEST,
    OPT_SEED,
    OPT_TEST,
    OPT_VERBOSE,
    /* This entry is always last. */
    OPT_COUNT,
};
static struct option long_options[] = {
    [OPT_BASE]          = { "base",         required_argument,  NULL, 'b' },
    [OPT_DIR]           = { "dir",          required_argument,  NULL, 'd' },
    [OPT_FILE]          = { "file",         required_argument,  NULL, 'f' },
    [OPT_HELP]          = { "help",         no_argument,        NULL, 'h' },
    [OPT_MAX_ERRORS]    = { "max-errors",   required_argument,  NULL, 'M' },
    [OPT_MERGE_TEST]    = { "merge-test",   required_argument,  NULL, 'T' },
    [OPT_SEED]          = { "seed",         required_argument,  NULL, 's' },
    [OPT_TEST]          = { "test",         required_argument,  NULL, 't' },
    [OPT_VERBOSE]       = { "verbose",      no_argument,        NULL, 'v' },
    [OPT_COUNT]         = { 0 },
};
static char options[] = "ab:d:e:f:hiM:m:T:t:vw:";

static unsigned seed;

/*
 * User preferences.
 */
static unsigned     max_errors = 100;       // Max number of errors to show.
static bool         verbose = false;        // Be verbose.

/*
 * Used by tests to specify the tables they want to the make_stuff
 * function.
 */
struct table_spec {
    const char *name;
    const struct TMCOL *cols;
    unsigned ncols;
    unsigned rowsz;
};

/*
 * Format value into a text representation.
 */
static char *
format_column(TMCOL col, void *p)
{
    int64_t     d = 0;
    uint64_t    u = 0;
    char       *s = NULL;

    switch (col->type) {
    case TMSTAT_T_HIDDEN:
        /* Pretend that hidden fields don't exist. */
        s = strdup("-");
        break;

    case TMSTAT_T_SIGNED:
        switch (col->size) {
        case 1:     d = *(int8_t *)p;       break;
        case 2:     d = *(int16_t *)p;      break;
        case 4:     d = *(int32_t *)p;      break;
        case 8:     d = *(int64_t *)p;      break;
        }
        asprintf(&s, "%lld", d);
        break;

    case TMSTAT_T_UNSIGNED:
        switch (col->size) {
        case 1:     u = *(uint8_t *)p;      break;
        case 2:     u = *(uint16_t *)p;     break;
        case 4:     u = *(uint32_t *)p;     break;
        case 8:     u = *(uint64_t *)p;     break;
        }
        asprintf(&s, "%llu", u);
        break;

    case TMSTAT_T_TEXT:
        s = malloc(col->size + 1);
        memcpy(s, p, col->size);
        s[col->size] = '\0';
        break;

    case TMSTAT_T_BIN:
        s = malloc((col->size * 10) + 1);
        for (unsigned i = 0; i < col->size; i++) {
            s[(i * 10) + 0] = (((uint8_t *)p)[i] & (1 << 7)) ? '1' : '0';
            s[(i * 10) + 1] = (((uint8_t *)p)[i] & (1 << 6)) ? '1' : '0';
            s[(i * 10) + 2] = (((uint8_t *)p)[i] & (1 << 5)) ? '1' : '0';
            s[(i * 10) + 3] = (((uint8_t *)p)[i] & (1 << 4)) ? '1' : '0';
            s[(i * 10) + 4] = ':';
            s[(i * 10) + 5] = (((uint8_t *)p)[i] & (1 << 3)) ? '1' : '0';
            s[(i * 10) + 6] = (((uint8_t *)p)[i] & (1 << 2)) ? '1' : '0';
            s[(i * 10) + 7] = (((uint8_t *)p)[i] & (1 << 1)) ? '1' : '0';
            s[(i * 10) + 8] = (((uint8_t *)p)[i] & (1 << 0)) ? '1' : '0';
            s[(i * 10) + 9] = ' ';
        }
        s[(col->size * 10) - 1] = '\0';
        break;

    case TMSTAT_T_DEC: {
        unsigned j = 0;

        s = malloc((col->size * 4) + 1);
        for (unsigned i = 0; i < col->size; i++) {
            j += sprintf(&s[j], "%d.", ((uint8_t *)p)[i]);
        }
        s[j - 1] = '\0';
        break;
    }

    case TMSTAT_T_HEX: {
        static char digit[16] = "0123456789abcdef";

        s = malloc(col->size * 3);
        for (unsigned i = 0; i < col->size; i++) {
           s[(i * 3) + 0] = digit[(((uint8_t *)p)[i] / 16) % 16];
           s[(i * 3) + 1] = digit[ ((uint8_t *)p)[i]       % 16];
           s[(i * 3) + 2] = ':';
        }
        s[(col->size * 3) - 1] = '\0';
        break;
    }

    default:
        /* Unknown format. */
        s = strdup("?");
        break;
    }

    return s;
}

static const char *
tmctl_rule_name(enum tmstat_rule r)
{
    switch (r) {
    case TMSTAT_R_KEY: return "key";
    case TMSTAT_R_OR:  return "or";
    case TMSTAT_R_SUM: return "sum";
    case TMSTAT_R_MIN: return "min";
    case TMSTAT_R_MAX: return "max";
    }
    return "???";
}

static const char *
tmctl_type_name(enum tmstat_type r)
{
    switch (r) {
    case TMSTAT_T_HIDDEN: return "hidden";
    case TMSTAT_T_SIGNED: return "signed";
    case TMSTAT_T_UNSIGNED: return "unsigned";
    case TMSTAT_T_TEXT: return "text";
    case TMSTAT_T_BIN: return "bin";
    case TMSTAT_T_DEC: return "dec";
    case TMSTAT_T_HEX: return "hex";
    }
    return "???";
}

static char *
format_columns(TMCOL col, unsigned col_count, void *base)
{
    unsigned    col_idx;
    char       *buffer;

    buffer = malloc(2);
    if (buffer == NULL) return NULL;
    buffer[0] = '[';
    buffer[1] = '\0';
    for (unsigned i = 0; i < 2 * col_count; i++) {
        char *s;
        unsigned len;
        col_idx = i % col_count;
        if ((col[col_idx].rule == TMSTAT_R_KEY) != (i < col_count)) {
            continue;
        }
        s = format_column(col + col_idx,
                          (char*)base + col[col_idx].offset);
        len = strlen(buffer) + strlen(col[col_idx].name) + strlen(s) + 100;
        char *tmp = malloc(len);
        if (tmp == NULL) {
            free(s);
            free(buffer);
            return NULL;
        }
        snprintf(tmp, len, "%s%s%s(%d:%s:%s:%d)=%s",
                 buffer,
                 strlen(buffer) > 1 ? " " : "",
                 col[col_idx].name,
                 col_idx,
                 tmctl_rule_name(col[col_idx].rule),
                 tmctl_type_name(col[col_idx].type),
                 col[col_idx].size,
                 s);
        free(s);
        free(buffer);
        buffer = tmp;
    }

    strcat(buffer, "]");
    return buffer;
}

/* For use in GDB. */
char *prow(TMROW);
char *
prow(TMROW row)
{
    void *p;
    TMCOL col;
    unsigned col_count;
    tmstat_row_field(row, NULL, &p);
    tmstat_row_description(row, &col, &col_count);
    return format_columns(col, col_count, p);
}

/*
 * Perform multi-segment merge performance test.
 * The point of this test to is to compare merge and search performance across
 * versions. All key types with different comparison algorithms will be used
 * and tables of various sizes will be compared.
 */
static int
test_merge_perf(void)
{
    struct row {
        uint32_t k1;
        uint64_t k2;
        int32_t k3;
        int64_t k4;
        char k5[32];
        int16_t dat;
    } *p;
    
    static struct TMCOL cols[] = {
        TMCOL_UINT(struct row, k1),
        TMCOL_UINT(struct row, k2),
        TMCOL_INT(struct row, k3),
        TMCOL_INT(struct row, k4),
        TMCOL_TEXT(struct row, k5),
        TMCOL_INT(struct row, dat, .rule=TMSTAT_R_SUM),
    };

    char            filename[PATH_MAX];
    char            label[32];
    TMSTAT          stat[SMALL_SIZE], sub_stat, merge_stat;
    TMTABLE         table;
    TMROW           *row[SMALL_SIZE], *sub_row;
    unsigned        rows;
    signed          ret;
    unsigned long long start, end, ctot = 0, mtot = 0, qtot = 0;
    unsigned  REPS, TREPS = 120;

    FILE* pdat = fopen("merge_perf.txt", "w");
    fprintf(pdat, "size, create, merge, query\n");

    printf("Merge performance test\n");
    unsigned ROWS, run;
    for (ROWS = 4, run = 1; ROWS < (1<<15);
         ROWS = ROWS << 1, ++run)
    {
        REPS = TREPS/run;
        printf(" %d: rows: %d (%d reps)", run, ROWS, REPS); fflush(stdout);
        for (unsigned reps = 0; reps < REPS; ++reps) {
            printf("."); fflush(stdout);
            // creation time
            start = rdtsc();
            for (unsigned i = 0; i < SMALL_SIZE; i++) {
                row[i] = malloc(ROWS*sizeof(TMROW));
                assert(row[i] != NULL);
                snprintf(label, sizeof(label), "ptest.%d", i);
                ret = tmstat_create(&stat[i], label);
                assert(ret == 0);
                ret = tmstat_table_register(stat[i], &table, "ptest",
                    cols, 6, sizeof(struct row));
                assert(ret == 0);
                ret = tmstat_publish(stat[i], "ptest");
                assert(ret == 0);
                for (unsigned j = 0; j < ROWS; j++) {
                    ret = tmstat_row_create(stat[i], table, &row[i][j]);
                    assert(ret == 0);
                    tmstat_row_field(row[i][j], NULL, &p);
                    p->k4 = p->k3 = p->k2 = p->k1 = (j*5)%ROWS;
                    snprintf(p->k5, 32, "key.%d", (j*5)%ROWS);
                }
            }
            end = rdtsc();
            ctot += end - start;
            
            ret = tmstat_subscribe(&sub_stat, "ptest");
            assert(ret == 0);
            ret = tmstat_query(
                sub_stat, "ptest", 0, NULL, NULL, &sub_row, &rows);
            assert(ret == 0);
            assert(rows == ROWS);
            for (unsigned i = 0; i < rows; i++) {
                tmstat_row_field(sub_row[i], NULL, &p);
                tmstat_row_drop(sub_row[i]);
            }
            free(sub_row);
            
            // merge time
            start = rdtsc();
            tmstat_merge(sub_stat, "ptest_merged", TMSTAT_MERGE_ALL);
            tmstat_destroy(sub_stat);
            for (unsigned i = 0; i < SMALL_SIZE; i++) {
                for (unsigned j = 0; j < ROWS; j++) {
                    tmstat_row_drop(row[i][j]);
                }
                tmstat_destroy(stat[i]);
            }
            end = rdtsc();
            mtot += end - start;

            snprintf(filename, sizeof(filename), "%s/private/ptest_merged",
                     tmstat_path);
            assert(tmstat_read(&merge_stat, filename) == 0);
            assert(tmstat_is_table_sorted(merge_stat, "ptest") == true);
            char* cols[5] = {"k1","k2","k3","k4","k5"};
            
            // query time
            start = rdtsc();
            for (unsigned i = 0; i < ROWS; ++i)
            {
                uint32_t k1v = i;
                uint64_t k2v = i;
                int32_t k3v = i;
                int64_t k4v = i;
                snprintf(label, sizeof(label), "key.%d", i);
                void* vals[5] = { (void*)&k1v, (void*)&k2v, (void*)&k3v,
                    (void*)&k4v,(void*)label };
                // query_fast will fail if an 'index' is not available
                ret = tmstat_query(merge_stat, "ptest", 5, cols,
                        vals, &sub_row, &rows);
                assert(ret == 0);
                assert(rows == 1);
                tmstat_row_drop(*sub_row);
            }
            end = rdtsc();
            qtot += end - start;
            tmstat_destroy(merge_stat);
        }
        printf("\n");
        printf(" create cycles: %3.1f\n", (float)ctot/REPS);
        printf(" merge cycles:  %3.1f\n", (float)mtot/REPS);
        printf(" query cycles:  %3.1f\n", (float)qtot/REPS);
        printf("done \n");
        fprintf(pdat, "%d, %3.1f, %3.1f, %3.1f\n", ROWS,
            (float)ctot/REPS, (float)mtot/REPS, (float)qtot/REPS);
    }
    fclose(pdat);
    return EXIT_SUCCESS;
}

/*
 * Perform multi-segment merge test.
 *
 * TO DO: Deprecate this function.  If this covers anything that some
 * invocation of merge_test cannot, merge_test should be updated.
 */
static int
test_merge(void)
{
    struct row {
        uint32_t    key, sum, junk, min, max, or, more_junk;
    } *p;
    static struct TMCOL cols[] = {
        /*
         * Note that these are intentionally not in the same order as
         * the fields in struct row and that struct row includes extra
         * junk that we want to ignore.
         */
        TMCOL_BIN(struct row, or, .rule = TMSTAT_R_OR),
        TMCOL_UINT(struct row, min, .rule = TMSTAT_R_MIN),
        TMCOL_UINT(struct row, key),
        TMCOL_UINT(struct row, sum, .rule = TMSTAT_R_SUM),
        TMCOL_UINT(struct row, max, .rule = TMSTAT_R_MAX),
    };
    unsigned        alloc_count = 0, free_count = 0;
    unsigned        search_count = 0, match_count = 0;
    char            label[32];
    TMSTAT          stat[SMALL_SIZE], sub_stat, merge_stat;
    TMTABLE         table;
    TMROW          *row, *sub_row;
    unsigned        rows;
    signed          ret;
    char            filename[PATH_MAX];

    row = malloc(sizeof(TMROW) * SMALL_SIZE * SIZE);
    assert(row != NULL);

    printf("Merge test: %d segments, %d tables, %d rows, %d bytes.\n",
        SMALL_SIZE,
        SMALL_SIZE,
        SMALL_SIZE * SIZE,
        SMALL_SIZE * SIZE * sizeof(struct row));
    printf(" 1:"); fflush(stdout);
    /* Create. */
    printf(" create;"); fflush(stdout);
    for (unsigned i = 0; i < SMALL_SIZE; i++) {
        snprintf(label, sizeof(label), "test.%d", i);
        ret = tmstat_create(&stat[i], label);
        assert(ret == 0);
        ret = tmstat_table_register(stat[i], &table, "test",
            cols, 5, sizeof(struct row));
        assert(ret == 0);
        ret = tmstat_publish(stat[i], "test");
        assert(ret == 0);
        for (unsigned j = 0; j < SIZE; j++) {
            ret = tmstat_row_create(stat[i], table, &row[i * SIZE + j]);
            assert(ret == 0);
            alloc_count++;
            tmstat_row_field(row[i * SIZE + j], NULL, &p);
            p->key = (j*5)%SIZE;
            p->sum = 1;
            p->min = (i << 8) + 1;
            p->max = (i << 8) + 1;
            p->or = 1 << (i % 32);
        }
    }
    /* Subscribe. */
    printf(" subscribe;"); fflush(stdout);
    ret = tmstat_subscribe(&sub_stat, "test");
    assert(ret == 0);
    /* Query. */
    printf(" query;"); fflush(stdout);
    ret = tmstat_query(sub_stat, "test", 0, NULL, NULL, &sub_row, &rows);
    assert(ret == 0);
    assert(rows == SIZE);
    search_count++;
    match_count += rows;
    for (unsigned i = 0; i < rows; i++) {
        tmstat_row_field(sub_row[i], NULL, &p);
        assert(p->sum == SMALL_SIZE);
        assert(p->min == 1);
        assert(p->max == ((SMALL_SIZE - 1) << 8) + 1);
        assert(p->or == 0xffffffff);
        tmstat_row_drop(sub_row[i]);
        free_count++;
    }
    free(sub_row);
    /* Merge out to new segment */
    printf(" output merged segment;");
    tmstat_merge(sub_stat, "test_merged", TMSTAT_MERGE_ALL);
    
    /* Destroy. */
    printf(" destroy;\n"); fflush(stdout);
    tmstat_destroy(sub_stat);
    for (unsigned i = 0; i < SMALL_SIZE; i++) {
        for (unsigned j = 0; j < SIZE; j++) {
            tmstat_row_drop(row[i * SIZE + j]);
            free_count++;
        }
        tmstat_destroy(stat[i]);
    }
    
    printf("    sorted query test on merged segment.\n");
    snprintf(filename, sizeof(filename), "%s/private/test_merged", tmstat_path);
    assert(tmstat_read(&merge_stat, filename) == 0);
    assert(tmstat_is_table_sorted(merge_stat, "test") == true);
    char* col = "key";
    for (unsigned i = 0; i < SIZE; ++i)
    {
        uint32_t kval = i;
        void* val = (void*)&kval;
        int ret;
        ret = tmstat_query(merge_stat, "test", 1, &col, &val, &sub_row, &rows);
        assert(ret == 0);
        assert(rows == 1);
        tmstat_row_drop(*sub_row);
        free(sub_row);
    }
    tmstat_destroy(merge_stat);
    printf("Done: %d allocs, %d frees, %d queries, %d matches.\n",
        alloc_count, free_count, search_count, match_count);
    free(row);
    return EXIT_SUCCESS;
}

/**
 * Merge two column values in the way we would expect the library to
 * do so.
 */
static void
tmctl_col_merge(TMCOL col, void *r_ptr, void *a_ptr)
{
    switch (col->rule) {
    case TMSTAT_R_OR:
        switch (col->size) {
        case 1:
            *(uint8_t *)a_ptr |= *(uint8_t *)r_ptr;
            break;
        case 2:
            *(uint16_t *)a_ptr |= *(uint16_t *)r_ptr;
            break;
        case 4:
            *(uint32_t *)a_ptr |= *(uint32_t *)r_ptr;
            break;
        case 8:
            *(uint64_t *)a_ptr |= *(uint64_t *)r_ptr;
            break;
        default:
            assert(0);
            break;
        }
        break;

    case TMSTAT_R_SUM:
        switch (col->size) {
        case 1:
            *(uint8_t *)a_ptr += *(uint8_t *)r_ptr;
            break;
        case 2:
            *(uint16_t *)a_ptr += *(uint16_t *)r_ptr;
            break;
        case 4:
            *(uint32_t *)a_ptr += *(uint32_t *)r_ptr;
            break;
        case 8:
            *(uint64_t *)a_ptr += *(uint64_t *)r_ptr;
            break;
        default:
            assert(0);
            break;
        }
        break;

    case TMSTAT_R_MIN:
        switch (col->type) {
        case TMSTAT_T_SIGNED:
            switch (col->size) {
            case 1:
                *(int8_t *)a_ptr =
                    TMCTL_MIN(*(int8_t *)a_ptr, *(int8_t *)r_ptr);
                break;
            case 2:
                *(int16_t *)a_ptr =
                    TMCTL_MIN(*(int16_t *)a_ptr, *(int16_t *)r_ptr);
                break;
            case 4:
                *(int32_t *)a_ptr =
                    TMCTL_MIN(*(int32_t *)a_ptr, *(int32_t *)r_ptr);
                break;
            case 8:
                *(int64_t *)a_ptr =
                    TMCTL_MIN(*(int64_t *)a_ptr, *(int64_t *)r_ptr);
                break;
            default:
                assert(0);
                break;
            }
            break;

        case TMSTAT_T_UNSIGNED:
            switch (col->size) {
            case 1:
                *(uint8_t *)a_ptr =
                    TMCTL_MIN(*(uint8_t *)a_ptr, *(uint8_t *)r_ptr);
                break;
            case 2:
                *(uint16_t *)a_ptr =
                    TMCTL_MIN(*(uint16_t *)a_ptr, *(uint16_t *)r_ptr);
                break;
            case 4:
                *(uint32_t *)a_ptr =
                    TMCTL_MIN(*(uint32_t *)a_ptr, *(uint32_t *)r_ptr);
                break;
            case 8:
                *(uint64_t *)a_ptr =
                    TMCTL_MIN(*(uint64_t *)a_ptr, *(uint64_t *)r_ptr);
                break;
            default:
                assert(0);
                break;
            }
            break;
        default:
            assert(0);
            break;
        }
        break;

    case TMSTAT_R_MAX:
        switch (col->type) {
        case TMSTAT_T_SIGNED:
            switch (col->size) {
            case 1:
                *(int8_t *)a_ptr =
                    TMCTL_MAX(*(int8_t *)a_ptr, *(int8_t *)r_ptr);
                break;
            case 2:
                *(int16_t *)a_ptr =
                    TMCTL_MAX(*(int16_t *)a_ptr, *(int16_t *)r_ptr);
                break;
            case 4:
                *(int32_t *)a_ptr =
                    TMCTL_MAX(*(int32_t *)a_ptr, *(int32_t *)r_ptr);
                break;
            case 8:
                *(int64_t *)a_ptr =
                    TMCTL_MAX(*(int64_t *)a_ptr, *(int64_t *)r_ptr);
                break;
            default:
                assert(0);
                break;
            }
            break;

        case TMSTAT_T_UNSIGNED:
            switch (col->size) {
            case 1:
                *(uint8_t *)a_ptr =
                    TMCTL_MAX(*(uint8_t *)a_ptr, *(uint8_t *)r_ptr);
                break;
            case 2:
                *(uint16_t *)a_ptr =
                    TMCTL_MAX(*(uint16_t *)a_ptr, *(uint16_t *)r_ptr);
                break;
            case 4:
                *(uint32_t *)a_ptr =
                    TMCTL_MAX(*(uint32_t *)a_ptr, *(uint32_t *)r_ptr);
                break;
            case 8:
                *(uint64_t *)a_ptr =
                    TMCTL_MAX(*(uint64_t *)a_ptr, *(uint64_t *)r_ptr);
                break;
            default:
                assert(0);
                break;
            }
            break;
        default:
            assert(0);
            break;
        }
        break;

    default:
        assert(0);
        break;
    }
}

/**
 * Compare either key or value columns of two rows.
 *
 * @param[in]   col         Column definitions
 * @param[in]   n_col       Number of columns
 * @param[in]   r1          Base pointer of lhs row
 * @param[in]   r2          Base pointer of rhs row
 * @param[in]   key         1: compare key columns, 0: compare value columns
 */
static int
tmctl_row_cmp(TMCOL col, unsigned n_col, char *r1, char *r2, int key)
{
    int m;
    key = !!key; /* boolify */
    for (unsigned i = 0; i < n_col; i++) {
        if ((col[i].rule == TMSTAT_R_KEY) == key) {
            m = memcmp(&r1[col[i].offset], &r2[col[i].offset], col[i].size);
            if (m != 0) {
                return m;
            }
        }
    }
    return 0;
}

static void
show_error(unsigned *n_err, unsigned err_max, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    (*n_err) += 1;
    if (*n_err <= err_max) {
        vprintf(fmt, ap);
    }
    if (*n_err == err_max) {
        printf("Further error reports will be suppressed.\n");
    }
    va_end(ap);
}

static void
verbosef(const char *fmt, ...)
{
    if (verbose) {
        va_list ap;
        va_start(ap, fmt);
        vprintf(fmt, ap);
        va_end(ap);
    }
}

/**
 * Merge test: lots of segments/rows/tables/columns.
 * All parameters are per-table except n_seg and n_tbl.
 *
 * @param[in]   n_seg       Number of segments to use
 * @param[in]   n_tbl       Number of tables to use
 * @param[in]   n_key       Number of key columns to use
 * @param[in]   n_val       Number of value columns to use
 * @param[in]   n_uniq      Number of distinct keys to use
 * @param[in]   rnd         1 says add rows always in same order; 0 otherwise
 * @param[in]   p_ins       % Probability of inserting a row on any iteration
 * @return EXIT_SUCCESS
 */
static int
test_merge_new(unsigned n_seg,
               unsigned n_tbl,
               const unsigned *n_key,
               const unsigned *n_val,
               const unsigned *n_uniq,
               const unsigned *rnd,
               const unsigned *p_ins)
{
    /* col[i][j]: jth column of ith table.  Not segment-specific. */
    TMCOL *col;

    /* rowsz[i]: Calculated row size for ith table. */
    unsigned *rowsz;

    /* stat[i]: ith stat segment. */
    TMSTAT *stat;

    /* tbl[i][j]: jth table of ith segment. */
    TMTABLE **tbl;

    /* row[i][j]: jth row accumulator for ith table. */
    void ***row;

    /* Subscription to the entire stats dir with all segments. */
    TMSTAT sub_stat;

    /* Count of errors encountered in comparing query results to our data. */
    unsigned error_count = 0;

    int r, ret;

    /*
     * Chatty chat chat.
     */
    printf("Merge test.  %d segments, %d tables per segment.\n"
           "Seed = %d.\n"
           "Table | # key col | # val col |  # keys | rnd? | p(ins)\n"
           "------+-----------+-----------+---------+------+-------\n",
           n_seg, n_tbl, seed);
    for (unsigned i = 0; i < n_tbl; i++) {
        printf("%5d |%10d |%10d |%8d |    %c |%6d%%\n",
               i, n_key[i], n_val[i], n_uniq[i],
               rnd[i] ? 'Y' : 'N', p_ins[i]);
    }

    /*
     * Dream up table definitions.
     */
    col = (TMCOL *)calloc(n_tbl, sizeof(TMCOL *));
    assert(col);
    rowsz = (unsigned*)calloc(n_tbl, sizeof(int));
    for (unsigned i = 0; i < n_tbl; i++) {
        unsigned key_done = 0;
        unsigned val_done = 0;
        const unsigned n_col = n_key[i] + n_val[i];
        col[i] = (TMCOL)calloc(n_col, sizeof(struct TMCOL));
        assert(col[i]);
        for (unsigned j = 0; j < n_col; j++) {
            /* Add empty space with low probability */
            r = random() % 100;
            for (unsigned c = 95; c < r; c++) rowsz[i] += 1 + (random() % 8);
            /* Name */
            col[i][j].name = (char*)malloc(32);
            assert(col[i][j].name);
            snprintf(col[i][j].name, 32, "col_%d_%d", i, j);
            /* Offset */
            col[i][j].offset = rowsz[i];
            /* Decide at random whether to make a key or value column,
             * unless we must make one or the other because we have
             * enough of one kind already. */
            r = random() % n_col;
            if ((r < n_key[i] && key_done < n_key[i]) || val_done == n_val[i]) {
                /* Key column */
                col[i][j].rule = TMSTAT_R_KEY;
                /* Key type */
                r = random() % 6;
                switch (r) {
                case 0:
                    col[i][j].type = TMSTAT_T_TEXT;
                    break;
                case 1:
                    col[i][j].type = TMSTAT_T_BIN;
                    break;
                case 2:
                    col[i][j].type = TMSTAT_T_DEC;
                    break;
                case 3:
                    col[i][j].type = TMSTAT_T_HEX;
                    break;
                case 4:
                    col[i][j].type = TMSTAT_T_SIGNED;
                    break;
                case 5:
                    col[i][j].type = TMSTAT_T_UNSIGNED;
                    break;
                }
                /* Key size: loop to ensure that first key column is
                 * big enough to store an integer or string
                 * representation thereof. */
                while (1) {
                    if (col[i][j].type == TMSTAT_T_SIGNED ||
                        col[i][j].type == TMSTAT_T_UNSIGNED) {
                        r = 1 << (random() % 4);
                    } else {
                        r = 1 + (random() % ((2 * TMSTAT_PATHED_NAMELEN) - 1));
                    }
                    if (key_done != 0 ||
                        (col[i][j].type == TMSTAT_T_TEXT && r > 20) ||
                        (col[i][j].type != TMSTAT_T_TEXT && r >= sizeof(int))) {
                        break;
                    }
                }
                col[i][j].size = r;
                rowsz[i] += col[i][j].size;
                key_done += 1;
            } else {
                /* Value column */
                r = random() % 9;
                switch (r) {
                case 0:
                case 1:
                case 2:
                case 3:
                    /* SUM fields are our bread and butter, so they're
                     * most common. */
                    col[i][j].rule = TMSTAT_R_SUM;
                    break;
                case 4:
                case 5:
                    col[i][j].rule = TMSTAT_R_MIN;
                    break;
                case 6:
                case 7:
                    col[i][j].rule = TMSTAT_R_MAX;
                    break;
                case 8:
                    /* OR fields are likely to saturate and are thus
                     * not very interesting, so they're less probable. */
                    col[i][j].rule = TMSTAT_R_OR;
                    break;
                }
                /* Value type */
                r = random() % 2;
                switch (r) {
                case 0:
                    col[i][j].type = TMSTAT_T_SIGNED;
                    break;
                case 1:
                    col[i][j].type = TMSTAT_T_UNSIGNED;
                    break;
                }
                /* Value size */
                r = random() % 4;
                col[i][j].size = (1 << r);
                rowsz[i] += col[i][j].size;
                val_done += 1;
            }
        }
        /* Add empty space at the end with low probability */
        r = random() % 100;
        for (unsigned c = 95; c < r; c++) rowsz[i] += 1 + (random() % 8);
    }

    /*
     * Create segments and tables.
     */
    stat = (TMSTAT*)calloc(n_seg, sizeof(TMSTAT));
    assert(stat);
    tbl = (TMTABLE**)calloc(n_seg, sizeof(TMTABLE*));
    assert(tbl);
    for (unsigned i = 0; i < n_seg; i++) {
        char stat_label[32];
        snprintf(stat_label, sizeof(stat_label), "test.%d", i);
        ret = tmstat_create(&stat[i], stat_label);
        assert(ret == 0);
        tbl[i] = (TMTABLE*)calloc(n_tbl, sizeof(TMTABLE));
        for (unsigned j = 0; j < n_tbl; j++) {
            char table_label[32];
            snprintf(table_label, sizeof(table_label), "table_%d", j);
            ret = tmstat_table_register(stat[i], &tbl[i][j], table_label,
                                        col[j], n_key[j] + n_val[j], rowsz[j]);
            assert(ret == 0);
        }
        ret = tmstat_publish(stat[i], "test");
        assert(ret == 0);
    }

    /*
     * Create row accumulators and fill in keys.  The first key column
     * will ensure uniqueness.  The rest of the key bytes will be
     * filled with random garbage.  An extra byte is allocated at the
     * end of the row.  This will store 1 if we ever insert a row with
     * this key into a table and 0 otherwise.
     */
    row = (void ***)calloc(n_tbl, sizeof(void **));
    assert(row);
    for (unsigned i = 0; i < n_tbl; i++) {
        unsigned n_accum = (n_key[i] > 0 ? n_uniq[i] : 1);
        row[i] = (void **)calloc(n_uniq[i], sizeof(void *));
        assert(row[i]);
        for (unsigned j = 0; j < n_accum; j++) {
            const unsigned n_col = n_key[i] + n_val[i];
            unsigned first_key_col = 1;
            row[i][j] = malloc(rowsz[i] + 1);
            assert(row[i][j]);
            memset(row[i][j], 0, rowsz[i] + 1);
            for (unsigned k = 0; k < n_col; k++) {
                char *key;
                if (col[i][k].rule != TMSTAT_R_KEY) {
                    continue;
                }
                key = (char*)row[i][j] + col[i][k].offset;
                if (col[i][k].type == TMSTAT_T_TEXT) {
                    unsigned off = 0;
                    if (first_key_col) {
                        assert(col[i][k].size > 20);
                        off = snprintf(key, col[i][k].size, "%020d", j);
                    }
                    for (; off < col[i][k].size; off++) {
                        /* Keep text readable. */
                        unsigned x = random() % 62;
                        char b;
                        if (x < 26) {
                            b = 'a' + x;
                        } else if (x < 52) {
                            b = 'A' + x - 26;
                        } else {
                            b = '0' + x - 52;
                        }
                        key[off] = b;
                    }
                } else {
                    unsigned off = 0;
                    if (first_key_col) {
                        assert(col[i][k].size >= sizeof(int));
                        *((int *)key) = j;
                        off = sizeof(int);
                    }
                    for (; off < col[i][k].size; off++) {
                        key[off] = random();
                    }
                }
                first_key_col = 0;
            }
        }
    }

    /*
     * Populate tables.
     */
    for (unsigned i = 0; i < n_tbl; i++) {
        int ret;
        const unsigned n_col = n_key[i] + n_val[i];
        /* ord defines the order in which we choose keys */
        unsigned *ord = calloc(n_uniq[i], sizeof(int));
        assert(ord);
        verbosef("Table %d rows:\n", i);
        if (n_key[i] == 0) {
            for (unsigned j = 0; j < n_uniq[i]; j++) {
                ord[j] = 0;
            }
        } else {
            for (unsigned j = 0; j < n_uniq[i]; j++) {
                ord[j] = j;
            }
        }
        for (unsigned j = 0; j < n_seg; j++) {
            if (rnd[i]) {
                /* Randomize key order. */
                for (unsigned k = 0; k < n_uniq[i]; k++) {
                    unsigned tmp;
                    r = random() % n_uniq[i];
                    tmp = ord[r];
                    ord[r] = ord[0];
                    ord[0] = tmp;
                }
            }
            /* Produce rows. */
            for (unsigned k = 0; k < n_uniq[i]; k++) {
                TMROW f;
                void *base;
                r = random() % 100;
                if (r >= p_ins[i]) {
                    /* We've been asked to randomly skip some keys and
                     * this one got unlucky; skip it. */
                    continue;
                }
                /* Create a row for this key */
                ret = tmstat_row_create(stat[j], tbl[j][i], &f);
                assert(ret == 0);
                ret = tmstat_row_field(f, NULL, &base);
                assert(ret == 0);
                /* Produce column values. */
                for (unsigned c = 0; c < n_col; c++) {
                    char *r_ptr = (char*)base + col[i][c].offset;
                    char *a_ptr = (char*)row[i][ord[k]] + col[i][c].offset;
                    if (col[i][c].rule == TMSTAT_R_KEY) {
                        /* Copy key */
                        memcpy(r_ptr, a_ptr, col[i][c].size);
                    } else {
                        /* Produce value */
                        assert(col[i][c].type == TMSTAT_T_SIGNED ||
                               col[i][c].type == TMSTAT_T_UNSIGNED);
                        r = random();
                        switch (col[i][c].size) {
                        case 1:
                            *(uint8_t *)r_ptr = (uint8_t)r;
                            break;
                        case 2:
                            *(uint16_t *)r_ptr = (uint16_t)r;
                            break;
                        case 4:
                            *(uint32_t *)r_ptr = (uint32_t)r;
                            break;
                        case 8:
                            *(uint64_t *)r_ptr = ((uint64_t)r << 32) | random();
                            break;
                        default:
                            assert(0);
                            break;
                        }
                        /* Merge new value into accumulator */
                        if (((char*)row[i][ord[k]])[rowsz[i]] == 0) {
                            /* This is the first time we have inserted
                             * a row with this key so there is nothing
                             * to merge with; just copy this value
                             * into the accumulator. */
                            memcpy(a_ptr, r_ptr, col[i][c].size);
                        } else {
                            tmctl_col_merge(&col[i][c], r_ptr, a_ptr);
                        }
                    }
                }
                if (verbose) {
                    char *s = format_columns(col[i], n_col, base);
                    printf("Segment %2d %s\n", j, s);
                    free(s);
                }
                /* Mark the key as having been inserted into a table. */
                ((char*)row[i][ord[k]])[rowsz[i]] = 1;
                /* Cause the row to remain in the table but free our handle. */
                tmstat_row_preserve(f);
                tmstat_row_drop(f);
            }
        }
        free(ord);
    }

    /*
     * Subscribe, query, and check the results.
     */
    ret = tmstat_subscribe(&sub_stat, "test");
    assert(ret == 0);
    for (unsigned i = 0; i < n_tbl; i++) {
        unsigned n_accum = (n_key[i] > 0 ? n_uniq[i] : 1);
        char *label = tmstat_table_name(tbl[0][i]);
        const unsigned n_col = n_key[i] + n_val[i];
        TMROW *result;
        unsigned count;
        ret = tmstat_query(sub_stat, label, 0, NULL, NULL, &result, &count);
        if (ret != 0) {
            printf("query for table %s failed with status %d.\n", label, ret);
            continue;
        }
        /* Check each accumulator against the results. */
        /* XXX: O(n^2)! */
        for (unsigned j = 0; j < n_accum; j++) {
            int found = 0;
            char *a_base = (char*)row[i][j];
            /* This key should appear in the results iff it was
             * inserted into a table in some segment and we expect to
             * see it in the result set at most once. */
            int expected = ((char*)row[i][j])[rowsz[i]];
            ((char*)row[i][j])[rowsz[i]] = 0;
            for (unsigned k = 0; k < count; k++) {
                char *r_base;
                tmstat_row_field(result[k], NULL, &r_base);
                if (tmctl_row_cmp(col[i], n_col, a_base, r_base, 1) == 0) {
                    if (!expected) {
                        char *s = format_columns(col[i], n_col, r_base);
                        show_error(&error_count, max_errors,
                                   "[%s] unexpected:\n%s\n",
                                   label, s);
                        free(s);
                        break;
                    }
                    found = 1;
                    if (tmctl_row_cmp(col[i], n_col, a_base, r_base, 0)) {
                        char *s1 = format_columns(col[i], n_col, a_base);
                        char *s2 = format_columns(col[i], n_col, r_base);
                        show_error(&error_count, max_errors,
                                   "[%s] mismatch:\n"
                                   "expected  %s\n"
                                   "actual    %s\n",
                                   label, s1, s2);
                        free(s1);
                        free(s2);
                        break;
                    }
                }
            }
            if (expected && !found) {
                char *s = format_columns(col[i], n_col, a_base);
                show_error(&error_count, max_errors,
                           "[%s] missing:\n%s\n",
                           label, s);
            }
        }
        /* Clean up */
        for (unsigned j = 0; j < count; j++) {
            tmstat_row_drop(result[j]);
        }
        free(result);
    }
    printf("%d errors.\n", error_count);

    /*
     * Party's over.  Clean up the mess.
     */
    tmstat_destroy(sub_stat);
    for (unsigned i = 0; i < n_tbl; i++) {
        for (unsigned j = 0; j < n_uniq[i]; j++) {
            free(row[i][j]);
        }
        free(row[i]);
        for (unsigned j = 0; j < n_key[i] + n_val[i]; j++) {
            free(col[i][j].name);
        }
        free(col[i]);
    }
    for (unsigned i = 0; i < n_seg; i++) {
        free(tbl[i]);
        tmstat_destroy(stat[i]);
    }
    free(row);
    free(tbl);
    free(stat);
    free(rowsz);
    free(col);

    return EXIT_SUCCESS;
}

/*
 * Invoke test_merge_new with randomized parameters.
 */
static int
test_merge_randomly(void)
{
    unsigned n_seg;
    unsigned n_tbl;
    unsigned *n_key;
    unsigned *n_val;
    unsigned *n_uniq;
    unsigned *rnd;
    unsigned *p_ins;

    n_seg = 2 + (random() % 10);
    n_tbl = 2 + (random() % 10);
    n_key = calloc(n_tbl, sizeof(unsigned));
    n_val = calloc(n_tbl, sizeof(unsigned));
    n_uniq = calloc(n_tbl, sizeof(unsigned));
    rnd = calloc(n_tbl, sizeof(unsigned));
    p_ins = calloc(n_tbl, sizeof(unsigned));
    for (unsigned i = 0; i < n_tbl; i++) {
        n_key[i] = random() % 5;
        n_val[i] = 1 + random() % 25;
        n_uniq[i] = 0;
        for (unsigned j = 0; j < 10; j++) {
            n_uniq[i] += random() % 1000;
        }
        rnd[i] = random() % 2;
        if (random() % 2) {
            p_ins[i] = 100;
        } else {
            p_ins[i] = random() % 100;
        }
    }
    return test_merge_new(n_seg, n_tbl, n_key, n_val, n_uniq, rnd, p_ins);
}

static int
parse_params(const char *str, unsigned **intsp, unsigned *n_intsp)
{
    const char *p = str;
    unsigned *ints = NULL;
    unsigned size = 0;
    unsigned capacity = 0;

    for (;;) {
        while (*p && !isdigit(*p)) ++p;
        if (*p == 0) break;
        if (size == capacity) {
            capacity = (capacity > 0) ? (capacity * 2) : 7;
            ints = realloc(ints, capacity * sizeof(unsigned));
        }
        ints[size++] = strtoul(p, (char **)&p, 10);
    }
    *n_intsp = size;
    *intsp = ints;
    return 0;
}

/*
 * Invoke test_merge_new with parameters specified in a string.
 *
 *     4x2: [1+3 100 1 100%] [2+6 100 0 50%]
 *
 * The first number is the number of segments; the second number is
 * the number of tables.  The numbers in the brackets define each
 * table: number of key columns, number of value columns, number of
 * unique keys, 1/0: insert keys in random order?, percent chance of
 * inserting each key.
 */
static int
test_merge_with_params(const char *params)
{
    unsigned n_seg;
    unsigned n_tbl;
    unsigned *n_key = NULL;
    unsigned *n_val = NULL;
    unsigned *n_uniq = NULL;
    unsigned *rnd = NULL;
    unsigned *p_ins = NULL;
    unsigned *ints = NULL;
    unsigned n_ints = 0;
    int ret;

    ret = parse_params(params, &ints, &n_ints);
    if (ret < 0) {
        return ret;
    }
    if ((n_ints < 2) || (((n_ints - 2) % 5) != 0) ||
        (((n_ints - 2) / 5) != ints[1])) {
        errx(2, "Merge test parameter format error."
             "  See usage for instructions.");
    }
    n_seg = ints[0];
    n_tbl = ints[1];
    n_key = malloc(n_tbl * sizeof(unsigned));
    n_val = malloc(n_tbl * sizeof(unsigned));
    n_uniq = malloc(n_tbl * sizeof(unsigned));
    rnd = malloc(n_tbl * sizeof(unsigned));
    p_ins = malloc(n_tbl * sizeof(unsigned));
    for (unsigned i = 0; i < n_tbl; ++i) {
        unsigned j = (i * 5) + 2;
        n_key[i] = ints[j + 0];
        n_val[i] = ints[j + 1];
        n_uniq[i] = ints[j + 2];
        rnd[i] = ints[j + 3];
        p_ins[i] = ints[j + 4];
        if (((rnd[i] != 0) && (rnd[i] != 1)) || (p_ins[i] > 100)) {
            ret = -1;
            goto out;
        }

    }
    ret = test_merge_new(n_seg, n_tbl, n_key, n_val, n_uniq, rnd, p_ins);
out:
    free(n_key);
    free(n_val);
    free(n_uniq);
    free(rnd);
    free(p_ins);
    free(ints);
    return ret;
}

/*
 * Perform multi-segment allocation test.
 */
static int
test_multi(void)
{
    char marker_text[16] = "text";
    char *marker = marker_text;
    typedef struct {  char text[16]; } R;
    static struct TMCOL cols[] = { TMCOL_TEXT(R, text) };
    struct H {
        TMSTAT          stat[SMALL_SIZE], sub_stat;
        TMROW           row[SMALL_SIZE][SMALL_SIZE][SMALL_SIZE];
        TMTABLE         table[SMALL_SIZE][SMALL_SIZE];
    } *h;
    char            label[32];
    signed          ret;
    unsigned        i, ii, j, jj, k;
    unsigned        alloc_count = 0, free_count = 0;
    unsigned        search_count = 0, match_count = 0;
    char           *s;

    h = malloc(sizeof(struct H));
    assert(h != NULL);
    for (jj = 0; jj < SMALL_SIZE; jj++) {
        snprintf(label, sizeof(label), "test.%d", jj);
        ret = tmstat_create(&h->stat[jj], label);
        assert(ret == 0);
        /* Register. */
        for (j = 0; j < SMALL_SIZE; j++) {
            snprintf(label, sizeof(label), "table_%d", j);
            ret = tmstat_table_register(h->stat[jj], &h->table[jj][j],
                label, cols, 1, 16);
            assert(ret == 0);
        }
        ret = tmstat_publish(h->stat[jj], "test");
        assert(ret == 0);
    }

    ret = tmstat_subscribe(&h->sub_stat, "test");
    assert(ret == 0);

    for (i = 0, ii = 0; i < SMALL_SIZE; i++) ii += i + 1;
    printf("Allocation test:"
           " 16 tests, %d segments, %d tables, %d rows, %d bytes.\n",
        SMALL_SIZE,
        SMALL_SIZE * SMALL_SIZE,
        SMALL_SIZE * ii,
        SMALL_SIZE * ii * 16);
    for (k = 0; k < 4*4; k++) {
        memset(h->row, 0, sizeof(h->row));
        printf("%2d:", k + 1); fflush(stdout);

        /*
         * Allocate.
         */
        switch (k / 4) {
        case 0:
            printf(" alloc forwards;"); fflush(stdout);
            for (jj = 0; jj < SMALL_SIZE; jj++) {
                for (j = 0; j < SMALL_SIZE; j++) {
                    for (i = 0; i < j + 1; i++) {
                        ret = tmstat_row_create(h->stat[jj], h->table[jj][j],
                            &h->row[jj][j][i]);
                        assert(ret == 0);
                        alloc_count++;
                        if (i % 4 == 0) {
                            /* Mark for query. */
                            tmstat_row_field(h->row[jj][j][i], NULL, &s);
                            strcpy(s, marker);
                        }
                    }
                }
            }
            break;

        case 1:
            printf(" alloc backwards;"); fflush(stdout);
            for (jj = 0; jj < SMALL_SIZE; jj++) {
                for (j = 0; j < SMALL_SIZE; j++) {
                    for (i = 0; i < j + 1; i++) {
                        ret = tmstat_row_create(h->stat[jj], h->table[jj][j],
                                &h->row[jj][j][j - i]);
                        assert(ret == 0);
                        alloc_count++;
                        if ((j - i) % 4 == 0) {
                            /* Mark for query. */
                            tmstat_row_field(h->row[jj][j][j - i], NULL, &s);
                            strcpy(s, marker);
                        }
                    }
                }
            }
            break;

        case 2:
            printf(" alloc even, then odd;"); fflush(stdout);
            for (jj = 0; jj < SMALL_SIZE; jj++) {
                for (j = 0; j < SMALL_SIZE; j++) {
                    for (i = 0; i < j + 1; i += 2) {
                        ret = tmstat_row_create(h->stat[jj], h->table[jj][j],
                                &h->row[jj][j][i]);
                        assert(ret == 0);
                        alloc_count++;
                        if (i % 4 == 0) {
                            /* Mark for query. */
                            tmstat_row_field(h->row[jj][j][i], NULL, &s);
                            strcpy(s, marker);
                        }
                    }
                    for (i = 1; i < j + 1; i += 2) {
                        ret = tmstat_row_create(h->stat[jj], h->table[jj][j],
                                &h->row[jj][j][i]);
                        assert(ret == 0);
                        alloc_count++;
                        if (i % 4 == 0) {
                            /* Mark for query. */
                            tmstat_row_field(h->row[jj][j][i], NULL, &s);
                            strcpy(s, marker);
                        }
                    }
                }
            }
            break;

        case 3:
            printf(" alloc randomly;"); fflush(stdout);
            for (jj = 0; jj < SMALL_SIZE; jj++) {
                for (j = 0; j < SMALL_SIZE; j++) {
                    for (i = 0; i < j + 1; i++) {
                        ii = rand() % (j + 1);
                        while (h->row[jj][j][ii] != NULL) {
                            ii = (ii + 1) % (j + 1);
                        }
                        ret = tmstat_row_create(h->stat[jj], h->table[jj][j],
                                &h->row[jj][j][ii]);
                        assert(ret == 0);
                        alloc_count++;
                        if (ii % 4 == 0) {
                            /* Mark for query. */
                            tmstat_row_field(h->row[jj][j][ii], NULL, &s);
                            strcpy(s, marker);
                        }
                    }
                }
            }
            break;
        }

        /*
         * Query everything.
         */
        printf(" query all;"); fflush(stdout);
        for (j = 0; j < SMALL_SIZE; j++) {
            snprintf(label, sizeof(label), "table_%d", j);
            ret = tmstat_query(h->sub_stat, label, 0, NULL, NULL, NULL, &i);
            assert((ret == 0) && (i == (j + 1) * SMALL_SIZE));
            match_count += i;
            search_count++;
        }

        /*
         * Query selected quarter.
         */
        printf(" query marked;"); fflush(stdout);
        for (j = 0; j < SMALL_SIZE; j++) {
            snprintf(label, sizeof(label), "table_%d", j);
            ret = tmstat_query(h->sub_stat, label, 1, &marker,
                (void**)(void*)&marker, NULL, &i);
            assert((ret == 0) && (i == ((j / 4) + 1) * SMALL_SIZE));
            match_count += i;
            search_count++;
        }

        /*
         * Free.
         */
        switch (k % 4) {
        case 0:
            printf(" free forwards.\n"); fflush(stdout);
            for (jj = 0; jj < SMALL_SIZE; jj++) {
                for (j = 0; j < SMALL_SIZE; j++) {
                    for (i = 0; i < j + 1; i++) {
                        tmstat_row_drop(h->row[jj][j][i]);
                        free_count++;
                    }
                }
            }
            break;

        case 1:
            printf(" free backwards.\n"); fflush(stdout);
            for (jj = 0; jj < SMALL_SIZE; jj++) {
                for (j = 0; j < SMALL_SIZE; j++) {
                    for (i = 0; i < j + 1; i++) {
                        tmstat_row_drop(h->row[jj][j][j - i]);
                        free_count++;
                    }
                }
            }
            break;

        case 2:
            printf(" free even, then odd.\n"); fflush(stdout);
            for (jj = 0; jj < SMALL_SIZE; jj++) {
                for (j = 0; j < SMALL_SIZE; j++) {
                    for (i = 0; i < j + 1; i += 2) {
                        tmstat_row_drop(h->row[jj][j][i]);
                        free_count++;
                    }
                    for (i = 1; i < j + 1; i += 2) {
                        tmstat_row_drop(h->row[jj][j][i]);
                        free_count++;
                    }
                }
            }
            break;

        case 3:
            printf(" free randomly.\n"); fflush(stdout);
            for (jj = 0; jj < SMALL_SIZE; jj++) {
                for (j = 0; j < SMALL_SIZE; j++) {
                    for (i = 0; i < j + 1; i++) {
                        ii = rand() % (j + 1);
                        while (h->row[jj][j][ii] == NULL) {
                            ii = (ii + 1) % (j + 1);
                        }
                        h->row[jj][j][ii] = tmstat_row_drop(h->row[jj][j][ii]);
                        free_count++;
                    }
                }
            }
            break;
        }
    }
    printf("Done: %d allocs, %d frees, %d queries, %d matches.\n",
        alloc_count, free_count, search_count, match_count);
    tmstat_destroy(h->sub_stat);
    for (jj = 0; jj < SMALL_SIZE; jj++) {
        tmstat_destroy(h->stat[jj]);
    }
    free(h);
    return EXIT_SUCCESS;
}

/*
 * Perform single-segment allocation test.
 */
static int
test_single(void)
{
    char marker_text[16] = "text";
    char *marker = marker_text;
    typedef struct {  char text[16]; } R;
    static struct TMCOL cols[] = { TMCOL_TEXT(R, text) };
    char            label[32];
    TMSTAT          stat;
    struct H {
        TMROW           row[SIZE][SIZE];
        TMTABLE         table[SIZE];
    } *h;
    signed          ret;
    unsigned        i, ii, j, k;
    unsigned        alloc_count = 0, free_count = 0;
    unsigned        search_count = 0, match_count = 0;
    char           *s;

    h = malloc(sizeof(struct H));
    assert(h != NULL);
    ret = tmstat_create(&stat, "test");
    assert(ret == 0);
    /* Register. */
    for (j = 0; j < SIZE; j++) {
        snprintf(label, sizeof(label), "table_%d", j);
        ret = tmstat_table_register(stat, &h->table[j], label, cols, 1, 16);
        assert(ret == 0);
    }

    for (i = 0, ii = 0; i < SIZE; i++) ii += i + 1;
    printf("Allocation test: 16 tests, %d tables, %d rows, %d bytes.\n",
        SIZE, ii, ii * 16);
    for (k = 0; k < 4*4; k++) {
        memset(h->row, 0, sizeof(h->row));
        printf("%2d:", k + 1); fflush(stdout);

        /*
         * Allocate.
         */
        switch (k / 4) {
        case 0:
            printf(" alloc forwards;"); fflush(stdout);
            for (j = 0; j < SIZE; j++) {
                for (i = 0; i < j + 1; i++) {
                    ret = tmstat_row_create(stat, h->table[j], &h->row[j][i]);
                    assert(ret == 0);
                    alloc_count++;
                    if (i % 4 == 0) {
                        /* Mark for query. */
                        tmstat_row_field(h->row[j][i], NULL, &s);
                        strcpy(s, marker);
                    }
                }
            }
            break;

        case 1:
            printf(" alloc backwards;"); fflush(stdout);
            for (j = 0; j < SIZE; j++) {
                for (i = 0; i < j + 1; i++) {
                    ret = tmstat_row_create(stat, h->table[j], &h->row[j][j - i]);
                    assert(ret == 0);
                    alloc_count++;
                    if ((j - i) % 4 == 0) {
                        /* Mark for query. */
                        tmstat_row_field(h->row[j][j - i], NULL, &s);
                        strcpy(s, marker);
                    }
                }
            }
            break;

        case 2:
            printf(" alloc even, then odd;"); fflush(stdout);
            for (j = 0; j < SIZE; j++) {
                for (i = 0; i < j + 1; i += 2) {
                    ret = tmstat_row_create(stat, h->table[j], &h->row[j][i]);
                    assert(ret == 0);
                    alloc_count++;
                    if (i % 4 == 0) {
                        /* Mark for query. */
                        tmstat_row_field(h->row[j][i], NULL, &s);
                        strcpy(s, marker);
                    }
                }
                for (i = 1; i < j + 1; i += 2) {
                    ret = tmstat_row_create(stat, h->table[j], &h->row[j][i]);
                    assert(ret == 0);
                    alloc_count++;
                    if (i % 4 == 0) {
                        /* Mark for query. */
                        tmstat_row_field(h->row[j][i], NULL, &s);
                        strcpy(s, marker);
                    }
                }
            }
            break;

        case 3:
            printf(" alloc randomly;"); fflush(stdout);
            for (j = 0; j < SIZE; j++) {
                for (i = 0; i < j + 1; i++) {
                    ii = rand() % (j + 1);
                    while (h->row[j][ii] != NULL) ii = (ii + 1) % (j + 1);
                    ret = tmstat_row_create(stat, h->table[j], &h->row[j][ii]);
                    assert(ret == 0);
                    alloc_count++;
                    if (ii % 4 == 0) {
                        /* Mark for query. */
                        tmstat_row_field(h->row[j][ii], NULL, &s);
                        strcpy(s, marker);
                    }
                }
            }
            break;
        }

        /*
         * Query everything.
         */
        printf(" query all;"); fflush(stdout);
        for (j = 0; j < SIZE; j++) {
            snprintf(label, sizeof(label), "table_%d", j);
            ret = tmstat_query(stat, label, 0, NULL, NULL, NULL, &i);
            assert((ret == 0) && (i == j + 1));
            match_count += i;
            search_count++;
        }

        /*
         * Query selected quarter.
         */
        printf(" query marked;"); fflush(stdout);
        for (j = 0; j < SIZE; j++) {
            snprintf(label, sizeof(label), "table_%d", j);
            ret = tmstat_query(stat, label, 1, &marker, (void**)(void*)&marker,
                NULL, &i);
            assert((ret == 0) && (i == (j / 4) + 1));
            match_count += i;
            search_count++;
        }

        /*
         * Free.
         */
        switch (k % 4) {
        case 0:
            printf(" free forwards.\n"); fflush(stdout);
            for (j = 0; j < SIZE; j++) {
                for (i = 0; i < j + 1; i++) {
                    tmstat_row_drop(h->row[j][i]);
                    free_count++;
                }
            }
            break;

        case 1:
            printf(" free backwards.\n"); fflush(stdout);
            for (j = 0; j < SIZE; j++) {
                for (i = 0; i < j + 1; i++) {
                    tmstat_row_drop(h->row[j][j - i]);
                    free_count++;
                }
            }
            break;

        case 2:
            printf(" free even, then odd.\n"); fflush(stdout);
            for (j = 0; j < SIZE; j++) {
                for (i = 0; i < j + 1; i += 2) {
                    tmstat_row_drop(h->row[j][i]);
                    free_count++;
                }
                for (i = 1; i < j + 1; i += 2) {
                    tmstat_row_drop(h->row[j][i]);
                    free_count++;
                }
            }
            break;

        case 3:
            printf(" free randomly.\n"); fflush(stdout);
            for (j = 0; j < SIZE; j++) {
                for (i = 0; i < j + 1; i++) {
                    ii = rand() % (j + 1);
                    while (h->row[j][ii] == NULL) ii = (ii + 1) % (j + 1);
                    h->row[j][ii] = tmstat_row_drop(h->row[j][ii]);
                    free_count++;
                }
            }
            break;
        }
    }
    printf("Done: %d allocs, %d frees, %d queries, %d matches.\n",
        alloc_count, free_count, search_count, match_count);
    tmstat_destroy(stat);
    free(h);
    return EXIT_SUCCESS;
}

/*
 * Create source segment for eval test script.
 */
static signed
test_eval(void)
{
    struct foo_row {
        char        text[32];
        signed      a;
        signed      b;
        signed      c;
    };
    struct bar_row {
        char        name[32];
        signed      d;
        signed      e;
        signed      f;
    };
    struct taz_row {
        unsigned    id;
        signed      g;
        signed      h;
        signed      i;
    };
    struct zot_row {
        int         n;
        uint8_t     bin[4];
        uint8_t     dec[4];
        uint8_t     hex[4];
    };
    static struct TMCOL foo_cols[] = {
        TMCOL_TEXT(struct foo_row, text),
        TMCOL_INT(struct foo_row, a),
        TMCOL_INT(struct foo_row, b),
        TMCOL_INT(struct foo_row, c),
    };
    static struct TMCOL bar_cols[] = {
        TMCOL_TEXT(struct bar_row, name),
        TMCOL_INT(struct bar_row, d),
        TMCOL_INT(struct bar_row, e),
        TMCOL_INT(struct bar_row, f),
    };
    static struct TMCOL taz_cols[] = {
        TMCOL_UINT(struct taz_row, id),
        TMCOL_INT(struct taz_row, g),
        TMCOL_INT(struct taz_row, h),
        TMCOL_INT(struct taz_row, i),
    };
    static struct TMCOL zot_cols[] = {
        TMCOL_BIN(struct zot_row, bin),
        TMCOL_DEC(struct zot_row, dec),
        TMCOL_HEX(struct zot_row, hex),
        TMCOL_INT(struct zot_row, n),
    };
    TMSTAT          stat;
    TMTABLE         foo_table, bar_table, taz_table, zot_table;
    TMROW           row;
    struct foo_row *foo;
    struct bar_row *bar;
    struct taz_row *taz;
    struct zot_row *zot;
    signed          ret;
    unsigned        i;

    ret = tmstat_create(&stat, "test");
    assert(ret == 0);

    /*
     * Register.
     */
    ret = tmstat_table_register(stat, &foo_table, "test/foo", foo_cols,
            array_count(foo_cols), sizeof(struct foo_row));
    assert(ret == 0);
    ret = tmstat_table_register(stat, &bar_table, "test/bar", bar_cols,
            array_count(bar_cols), sizeof(struct bar_row));
    assert(ret == 0);
    ret = tmstat_table_register(stat, &taz_table, "test/taz", taz_cols,
            array_count(taz_cols), sizeof(struct taz_row));
    assert(ret == 0);
    ret = tmstat_table_register(stat, &zot_table, "test/zot", zot_cols,
            array_count(zot_cols), sizeof(struct zot_row));
    assert(ret == 0);

    /*
     * Create single foo row.
     */
    ret = tmstat_row_create(stat, foo_table, &row);
    assert(ret == 0);
    tmstat_row_field(row, NULL, &foo);
    snprintf(foo->text, sizeof(foo->text), "%s", "Hello, World!");
    foo->a = 1;
    foo->b = 2;
    foo->c = 3;
    tmstat_row_preserve(row);
    tmstat_row_drop(row);

    /*
     * Create bar rows.
     */
    for (i = 0; i < 10; i++) {
        ret = tmstat_row_create(stat, bar_table, &row);
        assert(ret == 0);
        tmstat_row_field(row, NULL, &bar);
        snprintf(bar->name, sizeof(bar->name), "row %u", i);
        bar->d = i;
        bar->e = i * 10;
        bar->f = i * 100;
        tmstat_row_preserve(row);
        tmstat_row_drop(row);
    }

    /*
     * Create taz rows.
     */
    for (i = 0; i < 10; i++) {
        ret = tmstat_row_create(stat, taz_table, &row);
        assert(ret == 0);
        tmstat_row_field(row, NULL, &taz);
        taz->id = i;
        taz->g = i * 11;
        taz->h = i * 111;
        taz->i = i * 1111;
        tmstat_row_preserve(row);
        tmstat_row_drop(row);
    }

    /*
     * Create zot row.
     */
    ret = tmstat_row_create(stat, zot_table, &row);
    assert(ret == 0);
    tmstat_row_field(row, NULL, &zot);
    zot->n = 5;
    zot->bin[0] = 0x5A;
    zot->bin[1] = 0xA5;
    zot->bin[2] = 0x00;
    zot->bin[3] = 0xFF;
    zot->dec[0] = 90;
    zot->dec[1] = 165;
    zot->dec[2] = 0;
    zot->dec[3] = 255;
    zot->hex[0] = 0x5A;
    zot->hex[1] = 0xA5;
    zot->hex[2] = 0x00;
    zot->hex[3] = 0xFF;
    tmstat_row_preserve(row);
    tmstat_row_drop(row);

    tmstat_dealloc(stat);

    return (EXIT_SUCCESS);
}

static int
mktables(struct table_spec *specs, TMSTAT *statp, TMTABLE *tablesp, unsigned n)
{
    int ret;
    unsigned i;

    ret = tmstat_create(statp, "test");
    if (ret != 0) {
        *statp = NULL;
        return ret;
    }
    for (i = 0; i < n; ++i) {
        ret = tmstat_table_register(
            *statp, tablesp + i, (char *)specs[i].name,
            (struct TMCOL *)specs[i].cols, specs[i].ncols, specs[i].rowsz);
        if (ret != 0) {
            return ret;
        }
    }
    return 0;
}

static signed
test_memcpy_text_value_bug(void)
{
    struct row {
        char text[32];
    };
    static struct TMCOL cols[] = {
        TMCOL_TEXT(struct row, text, TMSTAT_R_KEY),
    };
    static struct table_spec specs[] = {
        { "table", cols, 1, sizeof(struct row) },
    };
    static const char *names[] = { "text" };
    static const char fn[] = "memcpy_text_value_bug";
    TMSTAT stat = NULL;
    TMTABLE table = NULL;
    TMROW row;
    TMROW *rows = NULL;
    unsigned n;
    int ret;
    char path[strlen(tmstat_path)+sizeof(TMSTAT_DIR_PRIVATE)+sizeof(fn)+3];
    void *values[1];
    char *pages = NULL;
    size_t pgsz;

    ret = mktables(specs, &stat, &table, 1);
    if (ret != 0) {
        goto end;
    }
    ret = tmstat_row_create(stat, table, &row);
    if (ret != 0) {
        warn("tmstat_row_create");
        goto end;
    }
    sprintf(path, "%s/%s/%s", tmstat_path, TMSTAT_DIR_PRIVATE, fn);
    ret = tmstat_merge(stat, path, TMSTAT_MERGE_PUBLIC);
    if (ret != 0) {
        warn("tmstat_merge");
        goto end;
    }
    tmstat_destroy(stat);
    stat = NULL;
    ret = tmstat_read(&stat, path);
    if (ret != 0) {
        goto end;
    }
    assert(tmstat_is_table_sorted(stat, "table"));
    pgsz = sysconf(_SC_PAGESIZE);
    pages = mmap(0, pgsz * 2, PROT_READ | PROT_WRITE,
                 MAP_ANONYMOUS | MAP_PRIVATE, 0, 0);
    if (pages == MAP_FAILED) {
        warn("mmap");
        ret = -1;
        goto end;
    }
    ret = munmap(pages + pgsz, pgsz);
    if (ret != 0) {
        warn("munmap");
        munmap(pages, 2 * pgsz); /* Hmm. */
        goto end;
    }
    pages[pgsz - 1] = 0;
    values[0] = pages + pgsz - 1;
    /* If the following call doesn't get a seg fault, we're good. */
    ret = tmstat_query(stat, "table", 1, (char **)names, values,
                       &rows, &n);
    if ((ret == 0) && (rows != NULL)) {
        free(rows);
    }
    munmap(pages, pgsz);

 end:
    if (stat) {
        tmstat_destroy(stat);
    }
    return ret;
}

/*
 * Exercise bugs that have been fixed.
 */
static signed
test_bugs(void)
{
    int ret = 0;

    ret = test_memcpy_text_value_bug();
    if (ret != 0) {
        goto end;
    }

 end:
    return ret;
}

static int
test_parse_print(void)
{
    uint8_t packed[4];
    char string[40];
    char *end;
    int ret;

    /* binary / print / 0 */
    ret = tmstat_print(packed, string, TMSTAT_T_BIN, 0);
    assert(ret == 0);
    assert(strcmp("", string) == 0);
    /* binary / parse / 0 */
    memset(packed, 0xAB, 4);
    ret = tmstat_parse("", &end, packed, TMSTAT_T_BIN, 0);
    assert(ret == 0);
    assert(*end == '\0');
    assert(packed[0] == 0xAB);
    assert(packed[1] == 0xAB);
    assert(packed[2] == 0xAB);
    assert(packed[3] == 0xAB);
    /* binary / print / 1 */
    ret = tmstat_print(packed, string, TMSTAT_T_BIN, 1);
    assert(ret == 0);
    assert(strcmp("1010:1011", string) == 0);
    /* binary / parse / 1 */
    memset(packed, 0x00, 4);
    ret = tmstat_parse("1010:1011", &end, packed, TMSTAT_T_BIN, 1);
    assert(ret == 0);
    assert(*end == '\0');
    assert(packed[0] == 0xAB);
    assert(packed[1] == 0x00);
    assert(packed[2] == 0x00);
    assert(packed[3] == 0x00);
    /* binary / parse / 4 */
    ret = tmstat_parse("1111:0000 0000:1111 1100:0011 0011:1100", &end,
                       packed, TMSTAT_T_BIN, 4);
    assert(ret == 0);
    assert(*end == '\0');
    assert(packed[0] == 0xF0);
    assert(packed[1] == 0x0F);
    assert(packed[2] == 0xC3);
    assert(packed[3] == 0x3C);
    /* binary / print / 4 */
    ret = tmstat_print(packed, string, TMSTAT_T_BIN, 4);
    assert(ret == 0);
    assert(strcmp("1111:0000 0000:1111 1100:0011 0011:1100", string) == 0);

    /* decimal / print / 0 */
    ret = tmstat_print(packed, string, TMSTAT_T_DEC, 0);
    assert(ret == 0);
    assert(strcmp("", string) == 0);
    /* decimal / parse / 0 */
    memset(packed, 111, 4);
    ret = tmstat_parse("", &end, packed, TMSTAT_T_DEC, 0);
    assert(ret == 0);
    assert(*end == '\0');
    assert(packed[0] == 111);
    assert(packed[1] == 111);
    assert(packed[2] == 111);
    assert(packed[3] == 111);
    /* decimal / print / 1 */
    ret = tmstat_print(packed, string, TMSTAT_T_DEC, 1);
    assert(ret == 0);
    assert(strcmp("111", string) == 0);
    /* decimal / parse / 1 */
    memset(packed, 0x00, 4);
    ret = tmstat_parse("1", &end, packed, TMSTAT_T_DEC, 1);
    assert(ret == 0);
    assert(*end == '\0');
    assert(packed[0] == 1);
    assert(packed[1] == 0);
    assert(packed[2] == 0);
    assert(packed[3] == 0);
    /* decimal / parse / 4 */
    ret = tmstat_parse("1.11.111.0", &end, packed, TMSTAT_T_DEC, 4);
    assert(ret == 0);
    assert(*end == '\0');
    assert(packed[0] == 1);
    assert(packed[1] == 11);
    assert(packed[2] == 111);
    assert(packed[3] == 0);
    /* decimal / print / 4 */
    ret = tmstat_print(packed, string, TMSTAT_T_DEC, 4);
    assert(ret == 0);
    assert(strcmp("1.11.111.0", string) == 0);

    /* hexadecimal / print / 0 */
    ret = tmstat_print(packed, string, TMSTAT_T_HEX, 0);
    assert(ret == 0);
    assert(strcmp("", string) == 0);
    /* hexadecimal / parse / 0 */
    memset(packed, 0x66, 4);
    ret = tmstat_parse("", &end, packed, TMSTAT_T_HEX, 0);
    assert(ret == 0);
    assert(*end == '\0');
    assert(packed[0] == 0x66);
    assert(packed[1] == 0x66);
    assert(packed[2] == 0x66);
    assert(packed[3] == 0x66);
    /* hexadecimal / print / 1 */
    ret = tmstat_print(packed, string, TMSTAT_T_HEX, 1);
    assert(ret == 0);
    assert(strcmp("66", string) == 0);
    /* hexadecimal / parse / 1 */
    memset(packed, 0x00, 4);
    ret = tmstat_parse("9a", &end, packed, TMSTAT_T_HEX, 1);
    assert(ret == 0);
    assert(*end == '\0');
    assert(packed[0] == 0x9A);
    assert(packed[1] == 0x00);
    assert(packed[2] == 0x00);
    assert(packed[3] == 0x00);
    /* hexadecimal / parse / 4 */
    ret = tmstat_parse("8A:f2:11:01", &end, packed, TMSTAT_T_HEX, 4);
    assert(ret == 0);
    assert(*end == '\0');
    assert(packed[0] == 0x8A);
    assert(packed[1] == 0xF2);
    assert(packed[2] == 0x11);
    assert(packed[3] == 0x01);
    /* hexadecimal / print / 4 */
    ret = tmstat_print(packed, string, TMSTAT_T_HEX, 4);
    assert(ret == 0);
    assert(strcmp("8A:F2:11:01", string) == 0);

    return 0;
}

/*
 * Test the automatic re-read functionality.
 */
static int
test_reread(void)
{
    typedef struct {
        char table1_key[50];
    } table1_row;
    static struct TMCOL table1_cols[] = {
        TMCOL_TEXT(table1_row, table1_key, .rule = TMSTAT_R_KEY),
    };
    typedef struct {
        unsigned table2_key;
        unsigned table2_val;
    } table2_row;
    static struct TMCOL table2_cols[] = {
        TMCOL_UINT(table2_row, table2_key, .rule = TMSTAT_R_KEY),
        TMCOL_UINT(table2_row, table2_val, .rule = TMSTAT_R_SUM),
    };
    typedef struct {
        unsigned table3a_val;
    } table3a_row;
    static struct TMCOL table3a_cols[] = {
        TMCOL_UINT(table3a_row, table3a_val, .rule = TMSTAT_R_SUM),
    };
    typedef struct {
        unsigned table3b_key;
    } table3b_row;
    static struct TMCOL table3b_cols[] = {
        TMCOL_UINT(table3b_row, table3b_key, .rule = TMSTAT_R_KEY),
    };
    typedef struct {
        signed table4_key1;
        signed table4_key2;
        signed table4_key3;
    } table4_row;
    static struct TMCOL table4_cols[] = {
        TMCOL_UINT(table4_row, table4_key1, .rule = TMSTAT_R_KEY),
        TMCOL_UINT(table4_row, table4_key2, .rule = TMSTAT_R_KEY),
        TMCOL_UINT(table4_row, table4_key3, .rule = TMSTAT_R_KEY),
    };
    typedef struct {
        unsigned char table5_key;
        unsigned table5_val;
    } table5_row;
    static struct TMCOL table5_cols[] = {
        TMCOL_UINT(table5_row, table5_key, .rule = TMSTAT_R_KEY),
        TMCOL_UINT(table5_row, table5_val, .rule = TMSTAT_R_OR),
    };

    int ret;
    TMSTAT stat1;
    TMSTAT stat2;
    TMSTAT stat3;
    TMSTAT statr;
    TMTABLE table1;
    TMTABLE table1_2;
    TMTABLE table2_2;
    TMTABLE table3;
    TMTABLE table4;
    TMTABLE table5;
    TMROW row;
    table1_row *t1r;
    table2_row *t2r;
    table3a_row *t3ar;
    table3b_row *t3br;
    table5_row *t5r;
    char path[PATH_MAX];
    TMROW *rows;
    unsigned n_rows;
    unsigned sum;
    struct TMCOL *cols;
    unsigned n_cols;
    DIR *dir;
    struct dirent *dirent;
    unsigned i;

    snprintf(path, sizeof(path), "%s/reread", tmstat_path);
    dir = opendir(path);
    if (dir != NULL) {
        while ((dirent = readdir(dir)) != NULL) {
            if (dirent->d_name[0] == '.') {
                /* Ignore everything starting with '.'. */
                continue;
            }
            snprintf(path, sizeof(path), "%s/reread/%s",
                     tmstat_path, dirent->d_name);
            unlink(path);
        }
        closedir(dir);
    } else if (errno == ENOENT) {
        /*
         * Try to ensure that the directory exists so that the following
         * subscribe won't fail.
         */
        mkdir(path, 0777);
    } else {
        err(1, "opendir(%s)", path);
    }

    /* Create a read handle for the directory.  We should see nothing. */
    ret = tmstat_subscribe(&statr, "reread");
    assert(ret == 0);
    ret = tmstat_query(statr, ".table", 0, NULL, NULL, &rows, &n_rows);
    assert(ret == 0);
    for (i = 0; i < n_rows; ++i) {
        char *name;
        ret = tmstat_row_field(rows[i], "name", &name);
        assert(ret == 0);
        assert(name[0] == '.');
        tmstat_row_drop(rows[i]);
    }
    free(rows);
    rows = NULL;

    /* Create segments and tables. */
    ret = tmstat_create(&stat1, "reread1");
    assert(ret == 0);
    ret = tmstat_table_register(stat1, &table1, "table1", table1_cols,
                                array_count(table1_cols), sizeof(table1_row));
    assert(ret == 0);
    ret = tmstat_table_register(stat1, &table1_2, "table2", table2_cols,
                                array_count(table2_cols), sizeof(table2_row));
    assert(ret == 0);
    ret = tmstat_create(&stat2, "reread2");
    assert(ret == 0);
    ret = tmstat_table_register(stat2, &table2_2, "table2", table2_cols,
                                array_count(table2_cols), sizeof(table2_row));
    assert(ret == 0);
    ret = tmstat_table_register(stat2, &table3, "table3", table3a_cols,
                                array_count(table3a_cols), sizeof(table3a_row));
    assert(ret == 0);
    ret = tmstat_create(&stat3, "reread3");
    assert(ret == 0);
    ret = tmstat_table_register(stat2, &table4, "table4", table4_cols,
                                array_count(table4_cols), sizeof(table4_row));
    assert(ret == 0);

    /* Fill tables with rows. */
    for (i = 0; i < SMALL_SIZE; ++i) {
        ret = tmstat_row_create(stat1, table1, &row);
        assert(ret == 0);
        tmstat_row_field(row, NULL, &t1r);
        snprintf(t1r->table1_key, sizeof(t1r->table1_key), "row%u", i);
        tmstat_row_preserve(row);
        tmstat_row_drop(row);
    }
    for (i = 0; i < SMALL_SIZE; ++i) {
        ret = tmstat_row_create(stat1, table1_2, &row);
        assert(ret == 0);
        tmstat_row_field(row, NULL, &t2r);
        t2r->table2_key = i;
        t2r->table2_val = 1;
        tmstat_row_preserve(row);
        tmstat_row_drop(row);
        ret = tmstat_row_create(stat2, table2_2, &row);
        assert(ret == 0);
        tmstat_row_field(row, NULL, &t2r);
        t2r->table2_key = i;
        t2r->table2_val = 2 * i;
        tmstat_row_preserve(row);
        tmstat_row_drop(row);
    }
    for (i = 0; i < SMALL_SIZE; ++i) {
        ret = tmstat_row_create(stat2, table3, &row);
        assert(ret == 0);
        ret = tmstat_row_field(row, NULL, &t3ar);
        assert(ret == 0);
        t3ar->table3a_val = i;
        tmstat_row_preserve(row);
        tmstat_row_drop(row);
    }

    /* Publish segments. */
    ret = tmstat_publish(stat1, "reread");
    assert(ret == 0);
    ret = tmstat_publish(stat2, "reread");
    assert(ret == 0);
    ret = tmstat_publish(stat3, "reread");
    assert(ret == 0);

    /* Reread data. */
    /*
     * XXX: We should test this with force (the second argument) equal
     * to false but we usually do our testing on ext3 files, whose
     * ctimes do not have sub-second resolution.  As a result, the
     * test will fail if we don't force update because tmstat thinks
     * that it has the most current files open.  Under tmpfs, which
     * does have sub-second ctimes, this should work.
     *
     * The better thing might be to use inotify instead of looking at
     * ctimes.
     */
    tmstat_refresh(statr, true);

    /* Verify that we have what we seek. */
    ret = tmstat_query(statr, "table1", 0, NULL, NULL, &rows, &n_rows);
    assert(ret == 0);
    sum = 0;
    for (i = 0; i < n_rows; ++i) {
        char *key;
        unsigned n;
        ret = tmstat_row_field(rows[i], "table1_key", &key);
        assert(ret == 0);
        ret = sscanf(key, "row%u", &n);
        assert(ret == 1);
        tmstat_row_drop(rows[i]);
        sum += n;
    }
    assert(sum == (SMALL_SIZE * (SMALL_SIZE - 1) / 2));
    free(rows);
    rows = NULL;
    ret = tmstat_query(statr, "table2", 0, NULL, NULL, &rows, &n_rows);
    assert(ret == 0);
    sum = 0;
    for (i = 0; i < n_rows; ++i) {
        unsigned key, val;
        key = tmstat_row_field_unsigned(rows[i], "table2_key");
        val = tmstat_row_field_unsigned(rows[i], "table2_val");
        assert(key < SMALL_SIZE);
        assert(val == 1 + 2 * key);
        tmstat_row_drop(rows[i]);
        sum += key;
    }
    assert(sum == (SMALL_SIZE * (SMALL_SIZE - 1) / 2));
    free(rows);
    rows = NULL;
    ret = tmstat_query(statr, "table3", 0, NULL, NULL, &rows, &n_rows);
    assert(ret == 0);
    assert(n_rows == 1);
    sum = tmstat_row_field_unsigned(rows[0], "table3a_val");
    assert(sum == (SMALL_SIZE * (SMALL_SIZE - 1) / 2));
    tmstat_row_drop(rows[0]);
    free(rows);
    rows = NULL;
    ret = tmstat_query(statr, "table4", 0, NULL, NULL, &rows, &n_rows);
    assert(ret == 0);
    assert(n_rows == 0);
    free(rows);
    rows = NULL;
    tmstat_table_info(statr, "table4", &cols, &n_cols);
    assert(n_cols == 3);
    ret = tmstat_query(statr, "table5", 0, NULL, NULL, &rows, &n_rows);
    assert(ret == 0);
    assert(n_rows == 0);
    free(rows);
    rows = NULL;
    tmstat_table_info(statr, "table5", &cols, &n_cols);
    assert(n_cols == 0);

    /* Remove segment 3 entirely to test this functionality. */
    tmstat_destroy(stat3);

    /* Don't leak. */
    tmstat_dealloc(stat2);

    /* Create different tables in segments with the same names. */
    /* We'll leave segment 1 in place to make sure that it works
     * to change some and leave some the same. */
    ret = tmstat_create(&stat2, "reread2");
    assert(ret == 0);
    ret = tmstat_table_register(stat2, &table2_2, "table2", table2_cols,
                                array_count(table2_cols), sizeof(table2_row));
    assert(ret == 0);
    ret = tmstat_table_register(stat2, &table3, "table3", table3b_cols,
                                array_count(table3b_cols), sizeof(table3b_row));
    assert(ret == 0);
    ret = tmstat_table_register(stat2, &table5, "table5", table5_cols,
                                array_count(table5_cols), sizeof(table5_row));
    assert(ret == 0);

    /* Fill tables with rows. */
    for (i = 0; i < SMALL_SIZE; ++i) {
        ret = tmstat_row_create(stat1, table1, &row);
        assert(ret == 0);
        tmstat_row_field(row, NULL, &t1r);
        snprintf(t1r->table1_key, sizeof(t1r->table1_key), "row%u",
                 i + SMALL_SIZE);
        tmstat_row_preserve(row);
        tmstat_row_drop(row);
    }
    for (i = 0; i < SMALL_SIZE; ++i) {
        ret = tmstat_row_create(stat2, table2_2, &row);
        assert(ret == 0);
        tmstat_row_field(row, NULL, &t2r);
        t2r->table2_key = i;
        t2r->table2_val = 3 * i;
        tmstat_row_preserve(row);
        tmstat_row_drop(row);
    }
    for (i = 0; i < SMALL_SIZE; ++i) {
        ret = tmstat_row_create(stat2, table3, &row);
        assert(ret == 0);
        ret = tmstat_row_field(row, NULL, &t3br);
        assert(ret == 0);
        t3br->table3b_key = i;
        tmstat_row_preserve(row);
        tmstat_row_drop(row);
    }
    for (i = 0; i < SMALL_SIZE; ++i) {
        for (unsigned j = 0; j < sizeof(unsigned) * 8; ++j) {
            ret = tmstat_row_create(stat2, table5, &row);
            assert(ret == 0);
            ret = tmstat_row_field(row, NULL, &t5r);
            assert(ret == 0);
            t5r->table5_key = i;
            t5r->table5_val = (1 << j);
            tmstat_row_preserve(row);
            tmstat_row_drop(row);
        }
    }

    /* Publish segment. */
    ret = tmstat_publish(stat2, "reread");
    assert(ret == 0);

    /* Reread data. */
    tmstat_refresh(statr, true);

    /* Verify that the changes have shown up. */
    ret = tmstat_query(statr, "table1", 0, NULL, NULL, &rows, &n_rows);
    assert(ret == 0);
    sum = 0;
    for (i = 0; i < n_rows; ++i) {
        char *key;
        unsigned n;
        ret = tmstat_row_field(rows[i], "table1_key", &key);
        assert(ret == 0);
        ret = sscanf(key, "row%u", &n);
        assert(ret == 1);
        tmstat_row_drop(rows[i]);
        sum += n;
    }
    assert(sum == ((SMALL_SIZE * 2) * ((SMALL_SIZE * 2) - 1) / 2));
    free(rows);
    rows = NULL;
    ret = tmstat_query(statr, "table2", 0, NULL, NULL, &rows, &n_rows);
    assert(ret == 0);
    sum = 0;
    for (i = 0; i < n_rows; ++i) {
        unsigned key, val;
        key = tmstat_row_field_unsigned(rows[i], "table2_key");
        val = tmstat_row_field_unsigned(rows[i], "table2_val");
        assert(key < SMALL_SIZE);
        assert(val == 1 + 3 * key);
        tmstat_row_drop(rows[i]);
        sum += key;
    }
    assert(sum == (SMALL_SIZE * (SMALL_SIZE - 1) / 2));
    free(rows);
    rows = NULL;
    ret = tmstat_query(statr, "table3", 0, NULL, NULL, &rows, &n_rows);
    assert(ret == 0);
    sum = 0;
    for (i = 0; i < n_rows; ++i) {
        unsigned key;
        key = tmstat_row_field_unsigned(rows[i], "table3b_key");
        assert(key < SMALL_SIZE);
        tmstat_row_drop(rows[i]);
        sum += key;
    }
    assert(sum == (SMALL_SIZE * (SMALL_SIZE - 1) / 2));
    free(rows);
    rows = NULL;
    tmstat_table_info(statr, "table4", &cols, &n_cols);
    assert(n_cols == 0);
    ret = tmstat_query(statr, "table5", 0, NULL, NULL, &rows, &n_rows);
    assert(ret == 0);
    sum = 0;
    for (i = 0; i < n_rows; ++i) {
        unsigned key, val;
        key = tmstat_row_field_unsigned(rows[i], "table5_key");
        val = tmstat_row_field_unsigned(rows[i], "table5_val");
        assert(key < SMALL_SIZE);
        assert(val == (unsigned)-1);
        tmstat_row_drop(rows[i]);
        sum += key;
    }
    assert(sum == (SMALL_SIZE * (SMALL_SIZE - 1) / 2));
    free(rows);
    rows = NULL;

    tmstat_destroy(stat1);
    tmstat_destroy(stat2);
    /* Destroyed stat3 earlier. */
    tmstat_dealloc(statr);

    return EXIT_SUCCESS;
}

struct foo_row {
    char        text[32];
    signed      a;
    signed      b;
    signed      c;
};
static struct TMCOL foo_cols[] = {
    TMCOL_TEXT(struct foo_row, text),
    TMCOL_INT(struct foo_row, a, .rule = TMSTAT_R_SUM),
    TMCOL_INT(struct foo_row, b, .rule = TMSTAT_R_MIN),
    TMCOL_INT(struct foo_row, c, .rule = TMSTAT_R_MAX),
};

static void
test_one(TMSTAT stat, unsigned N, unsigned n, unsigned c, unsigned z)
{
    int ret;
    TMROW row;
    struct foo_row *r;
    char *names[] = { "text" };
    char value[32];
    char **values = malloc(sizeof(char*));
    unsigned sum;

    values[0] = value;

    sum = c * (n * (n + 1)) / 2;
    sum += (z - 1) * c * (N * (N + 1)) / 2;

    strncpy(value, "ldfhasadfkjgha", sizeof(value));
    row = (TMROW)0xBAD;
    ret = tmstat_query_rollup(
        stat, "foo", 1, names, (void **)values, &row);
    assert(ret != 0);
    assert(row == NULL);

    ret = tmstat_query_rollup(stat, "foo", 0, NULL, NULL, &row);
    assert(ret == 0);
    assert(row != NULL);
    ret = tmstat_row_field(row, NULL, &r);
    assert(ret == 0);
    assert(r->a == sum);
    assert(r->b == 1);
    assert(r->c == (z > 0) ? N : n);
    tmstat_row_drop(row);

    for (unsigned i = 1; i <= n; ++i) {
        snprintf(value, sizeof(value), "row%u", i);
        ret = tmstat_query_rollup(
            stat, "foo", 1, names, (void **)values, &row);
        assert(ret == 0);
        assert(row != NULL);
        ret = tmstat_row_field(row, NULL, &r);
        assert(ret == 0);
        assert(r->a == i * c * z);
        assert(r->b == i);
        assert(r->c == i);
        tmstat_row_drop(row);
    }
    free(values);
}

#define INSN_TABLE_COUNT    4
#define INSN_ROW_COUNT      10240

static int
test_insn(void)
{
    struct row {
        uintptr_t   self;
        uintptr_t   t;
    };
    struct H {
        TMTABLE         table[INSN_TABLE_COUNT];
        TMROW           row[INSN_TABLE_COUNT][INSN_ROW_COUNT];
    };

    static struct TMCOL cols[] = {
        TMCOL_UINT(struct row, self, .rule=TMSTAT_R_KEY),
        TMCOL_UINT(struct row, t),
    };

    char            label[32];
    TMSTAT          stat;
    TMROW          *result;
    struct H       *h;
    struct row     *row;
    int             n[INSN_TABLE_COUNT];
    int             ret;
    int             t;
    int             r;
    int             i;
    int             c;
    unsigned        result_count;

    h = malloc(sizeof(struct H));
    assert(h != NULL);
    ret = tmstat_create(&stat, "test");
    assert(ret == 0);
    for (t = 0; t < INSN_TABLE_COUNT; t++) {
        snprintf(label, sizeof(label), "table_%d", t);
        ret = tmstat_table_register(stat, &h->table[t], label, cols,
                sizeof(cols) / sizeof(cols[0]), sizeof(struct row) +
                (random() % 256));
        assert(ret == 0);
        n[t] = 0;
    }
    c = 0;
    r = 0;
    while (c < INSN_TABLE_COUNT * INSN_ROW_COUNT) {
        do {
            t = random() % INSN_TABLE_COUNT;
        } while (n[t] == INSN_ROW_COUNT);
        switch (random() % 10) {
        default:
            if (n[t] > 0) {
                r = random() % n[t];
                tmstat_row_drop(h->row[t][r]);
                h->row[t][r] = h->row[t][n[t] - 1];
                n[t] -= 1;
                c -= 1;
            }
            r = 0;
            break;
        case 0:
            for (i = 0; (i < r) && (n[t] > 0); ++i) {
                tmstat_row_drop(h->row[t][n[t] - 1]);
                n[t] -= 1;
                c -= 1;
            }
            r = 0;
            break;
        case 1:
            r = 1;
            ret = tmstat_row_create(stat, h->table[t],
                    &h->row[t][n[t]]);
            break;
        case 2:
            r = TMCTL_MIN(random() % INSN_ROW_COUNT, INSN_ROW_COUNT - n[t]);
            ret = tmstat_row_create_n(stat, h->table[t],
                    &h->row[t][n[t]], r);
            break;
        }
        assert(ret == 0);
        c += r;
        for (i = n[t], n[t] += r; i < n[t]; ++i) {
            tmstat_row_field(h->row[t][i], NULL, &row);
            row->self = (uintptr_t)row;
            row->t = t;
        }
    }
    for (t = 0; t < INSN_TABLE_COUNT; ++t) {
        snprintf(label, sizeof(label), "table_%d", t);
        ret = tmstat_query(stat, label, 0, NULL, NULL, &result, &result_count);
        assert(ret == 0);
        assert(result_count == n[t]);
        for (i = 0; i < result_count; ++i) {
            tmstat_row_field(result[i], NULL, &row);
            assert(row->self == (uintptr_t)row);
            assert(row->t == t);
            tmstat_row_drop(result[i]);
        }
        free(result);
    }
    tmstat_destroy(stat);
    free(h);
    return EXIT_SUCCESS;
}

static int
test_rollup(void)
{
    const unsigned C = 2;
    const unsigned Z = 3;
    const unsigned N = SMALL_SIZE;

    int ret;
    TMSTAT stat_c[Z];
    TMSTAT stat_r;
    TMSTAT stat_s;
    TMTABLE table;
    TMROW row;
    struct foo_row *r;
    char path[PATH_MAX];
    char name[10];

    snprintf(path, sizeof(path), "%s/rollup", tmstat_path);
    mkdir(path, 0777);
    ret = tmstat_subscribe(&stat_s, "rollup");
    assert(ret == 0);
    for (unsigned z = 0; z < Z; ++z) {
        snprintf(name, sizeof(name), "rollup%u", z);
        ret = tmstat_create(&stat_c[z], name);
        assert(ret == 0);
        ret = tmstat_table_register(
            stat_c[z], &table, "foo", foo_cols,
            array_count(foo_cols), sizeof(struct foo_row));
        assert(ret == 0);
        ret = tmstat_publish(stat_c[z], "rollup");
        assert(ret == 0);
        snprintf(path, sizeof(path), "%s/rollup/%s", tmstat_path, name);
        for (unsigned i = 1; i <= N; ++i) {
            for (unsigned j = 0; j < C; ++j) {
                ret = tmstat_row_create(stat_c[z], table, &row);
                assert(ret == 0);
                tmstat_row_field(row, NULL, &r);
                snprintf(r->text, sizeof(r->text), "row%u", i);
                r->a = i;
                r->b = i;
                r->c = i;
                tmstat_row_preserve(row);
                tmstat_row_drop(row);
            }
            ret = tmstat_read(&stat_r, path);
            assert(ret == 0);
            tmstat_refresh(stat_s, true);
            test_one(stat_c[z], N, i, C, 1);
            test_one(stat_r, N, i, C, 1);
            test_one(stat_s, N, i, C, z + 1);
            tmstat_destroy(stat_r);
        }
    }
    for (unsigned z = 0; z < Z; ++z) {
        tmstat_destroy(stat_c[z]);
    }
    tmstat_destroy(stat_s);
    return EXIT_SUCCESS;
}

/*
 * Test our resilience in the face of modifications to a segment that
 * is being read.  We don't test real synchronicity: We alternately
 * write and read.
 *
 * We first create a segment.  We then enter a loop, registering new
 * tables and adding rows to existing tables.  After some small random
 * number of iterations of the loop, we subscribe to the segment.  At
 * the end of each iteration, we query the segment and verify that our
 * view of it is consistent with what it should be.
 *
 * This test doesn't clean up after itself on failure.
 */
static int
test_concurrent_modification(void)
{
    struct simple_row {
        char        key[32];
        unsigned    value;
    };
    static struct TMCOL simple_cols[] = {
        TMCOL_TEXT(struct simple_row, key),
        TMCOL_INT(struct simple_row, value, .rule = TMSTAT_R_SUM),
    };

    TMSTAT              writer;
    TMSTAT              reader = NULL;
    int                 ret;
    unsigned            i, j;
    signed              k, r;
    unsigned            n_tables = 0;
    TMTABLE            *tables = NULL;
    unsigned           *table_rows = NULL;
    unsigned            key = 0;
    TMROW               tmrow;
    struct simple_row  *row;
    char                buffer[256];
    TMROW              *dot_table_rows;
    unsigned            dot_table_n_rows;
    char               *name;
    unsigned            cols;
    unsigned            rowsz;
    TMROW              *rows;
    unsigned            n_rows;
    unsigned            table_idx;
    unsigned            hidden;

    ret = tmstat_create(&writer, "cmod");
    assert(ret == 0);
    ret = tmstat_publish(writer, TMSTAT_DIR_PUBLISH);
    assert(ret == 0);
    for (i = 0; i < 1000; ++i) {
        printf("%u\n", i);
        /* Maybe add a table. */
        r = random() % 10;
        if (r > 6) {
            n_tables += 1;
            tables = realloc(tables, sizeof(TMTABLE) * n_tables);
            table_rows = realloc(table_rows, sizeof(unsigned) * n_tables);
            snprintf(buffer, sizeof(buffer), "table%u", n_tables - 1);
            ret = tmstat_table_register(
                writer, &tables[n_tables - 1], buffer,
                simple_cols, 2, sizeof(struct simple_row));
            assert(ret == 0);
            table_rows[n_tables - 1] = 0;
        }
        /* Maybe add some rows to each table. */
        for (j = 0; j < n_tables; ++j) {
            r = (random() % 200) - 100; /* Add rows only 50% of the time. */
            for (k = 0; k < r; ++k) {
                ret = tmstat_row_create(writer, tables[j], &tmrow);
                assert(ret == 0);
                tmstat_row_field(tmrow, NULL, &row);
                snprintf(row->key, sizeof(row->key), "row%u", ++key);
                row->value = 1;
            }
            table_rows[j] += (r > 0) ? r : 0;
        }
        /* Maybe subscribe if not subscribed. */
        if ((reader == NULL) && ((random() % 5) != 0)) {
            snprintf(buffer, sizeof(buffer), "%s/%s/cmod",
                     tmstat_path, TMSTAT_DIR_PUBLISH);
            ret = tmstat_read(&reader, buffer);
            assert(ret == 0);
        }
        if (reader != NULL) {
            /* Read all the tables.  Start by finding them. */
            ret = tmstat_query(reader, ".table", 0, NULL, NULL,
                               &dot_table_rows, &dot_table_n_rows);
            assert(ret == 0);
            for (hidden = 0, j = 0; j < dot_table_n_rows; ++j) {
                ret = tmstat_row_field(dot_table_rows[j], "name", &name);
                assert(ret == 0);
                if (name[0] == '.') {
                    hidden += 1;
                    goto next_row;
                }
                ret = sscanf(name, "table%u", &table_idx);
                assert(ret == 1);
                cols = tmstat_row_field_unsigned(dot_table_rows[j], "cols");
                assert(cols == 2);
                rowsz = tmstat_row_field_unsigned(dot_table_rows[j], "rowsz");
                assert(rowsz == sizeof(struct simple_row));
                ret = tmstat_query(reader, name, 0, NULL, NULL,
                                   &rows, &n_rows);
                assert(ret == 0);
                if (n_rows != table_rows[table_idx]) {
                    printf("%3u: in table%u, wrote %u rows, found %u rows\n",
                           i, table_idx, table_rows[table_idx], n_rows);
                }
                for (k = 0; k < n_rows; ++k) {
                    unsigned x;
                    tmstat_row_field(rows[k], NULL, &row);
                    ret = sscanf(row->key, "row%u", &x);
                    if ((ret != 1) || (row->value != 1)) {
                        printf("%3u: bogus-looking row in table%u: "
                               "key=`%s' value=%u\n",
                               i, table_idx, row->key, row->value);
                    }
                    tmstat_row_drop(rows[k]);
                }
                free(rows);
            next_row:
                tmstat_row_drop(dot_table_rows[j]);
            }
            free(dot_table_rows);
            if (dot_table_n_rows - hidden != n_tables) {
                printf("%3u: wrote %d tables; found %d tables\n",
                       i, n_tables, dot_table_n_rows - hidden);
            }
        }
    }
    return EXIT_SUCCESS;
}

static int
test_long_keys(void)
{
#define NUM_KEYS 3
#define START_LENGTH TMSTAT_PATHED_NAMELEN - 1

    struct long_key_row {
        char     key[TMSTAT_PATHED_NAMELEN];
        unsigned value;
    };

    static struct TMCOL long_key_cols[] = {
        TMCOL_TEXT(struct long_key_row, key),
        TMCOL_INT (struct long_key_row, value, .rule = TMSTAT_R_SUM),
    };

    TMROW write_row;
    TMROW *rows;
    TMSTAT write_stat;
    TMSTAT query_stat;
    TMTABLE table;
    struct long_key_row *ptr;
    unsigned ret;
    unsigned row_count;
    unsigned key_index;
    unsigned key_length = START_LENGTH;
    unsigned i;
    
    /* Define e keys:
     * one with length TMSTAT_PATHED_NAMELEN - 1
     * one with length TMSTAT_PATHED_NAMELEN 
     * one with length TMSTAT_PATHED_NAMELEN + 1
     */
    char *keys[NUM_KEYS];
    for(key_index=0; key_index < NUM_KEYS; key_index++) {
        keys[key_index] = calloc(1,key_length);
        /* Leave room for NULL. */
        for(i=0; i < key_length; i++) {
            *(keys[key_index]+i) = 'a';
        }
        *(keys[key_index]+i) = '\0';
        key_length++;
    }

    /* Create segment. */
    ret = tmstat_create(&write_stat, "long_keys_segment");
    assert(ret==0);

    ret = tmstat_table_register(write_stat, &table, "long_keys_table", 
            long_key_cols, 2, sizeof(struct long_key_row));
    assert(ret==0);

    /* Add keys. */
    for(i=0; i<NUM_KEYS; i++) {
        printf("testing with key of length %d.\n",  strlen(keys[i]));
        ret = tmstat_row_create(write_stat, table, &write_row);
        assert(ret==0);
        tmstat_row_field(write_row, NULL, &ptr);
        strncpy(ptr->key, keys[i], sizeof(ptr->key));
        tmstat_row_preserve(write_row);
        tmstat_row_drop(write_row);
    }

    /* Publish. */
    ret = tmstat_publish(write_stat, "long_keys_segment");
    assert(ret==0);

    /* Subscribe. */
    ret = tmstat_subscribe(&query_stat, "long_keys_segment");

    char *names[] = { "key" };
    void *values[1];
    for(i=0; i<NUM_KEYS; i++) {
        /* Query with key, should only get 1 row. */
        values[0] = (void*)keys[i];
        ret = tmstat_query(query_stat, "long_keys_table", 1, names, values,
                &rows, &row_count);
        assert(ret==0);
        assert(row_count==1);
    }


    return EXIT_SUCCESS;
}

static int
test_unterminated_keys(void)
{
#define NUM_KEYS 3
#define KEY_LENGTH 4

    struct long_key_row {
        char     key[4];
        unsigned value;
    };

    static struct TMCOL long_key_cols[] = {
        TMCOL_TEXT(struct long_key_row, key),
        TMCOL_INT (struct long_key_row, value, .rule = TMSTAT_R_SUM),
    };

    TMROW write_row;
    TMROW *rows;
    TMSTAT write_stat;
    TMSTAT query_stat;
    TMTABLE table;
    struct long_key_row *ptr;
    unsigned ret;
    unsigned row_count;
    unsigned i;
    char keys[NUM_KEYS][4];

    strncpy(keys[0],"aaaa", sizeof(keys[0]));
    strncpy(keys[1],"aaab", sizeof(keys[1]));
    strncpy(keys[2],"aaba", sizeof(keys[2]));
    strncpy(keys[3],"aabb", sizeof(keys[3]));

    /* Create segment. */
    ret = tmstat_create(&write_stat, "unterminated_keys_segment");
    assert(ret==0);

    ret = tmstat_table_register(write_stat, &table, "unterminated_keys_table", 
            long_key_cols, 2, sizeof(struct long_key_row));
    assert(ret==0);

    /* Add keys. */
    for(i=0; i<NUM_KEYS; i++) {
        ret = tmstat_row_create(write_stat, table, &write_row);
        assert(ret==0);
        tmstat_row_field(write_row, NULL, &ptr);
        strncpy(ptr->key, keys[i], sizeof(ptr->key));
        tmstat_row_preserve(write_row);
        tmstat_row_drop(write_row);
    }

    /* Publish. */
    ret = tmstat_publish(write_stat, "unterminated_keys_segment");
    assert(ret==0);

    /* Subscribe. */
    ret = tmstat_subscribe(&query_stat, "unterminated_keys_segment");

    char *names[] = { "key" };
    void *values[1];
    for(i=0; i<NUM_KEYS; i++) {
        /* Query with key, should only get 1 row. */
        values[0] = (void*)keys[i];
        ret = tmstat_query(query_stat, "unterminated_keys_table", 1, names, values,
                &rows, &row_count);
        assert(ret==0);
        assert(row_count==1);
    }

    /* Query with keys that are longer than the keys in the rows */

    char * long_key = "aaaaa";

    values[0] = (void*)long_key;
    ret = tmstat_query(query_stat, "unterminated_keys_table", 1, names, values,
            &rows, &row_count);
    assert(ret==0);
    assert(row_count==1);

    return EXIT_SUCCESS;
}

static volatile int zero = 0;

static int
test_core(void)
{
#define CORE_SIZE 16
    char marker_text[16] = "text";
    char *marker = marker_text;
    typedef struct {  int num; char text[16]; } R;
    static struct TMCOL cols[] = { TMCOL_INT(R, num), TMCOL_TEXT(R, text) };
    struct H {
        TMSTAT          stat[CORE_SIZE], sub_stat;
        TMROW           row[CORE_SIZE][CORE_SIZE][CORE_SIZE];
        TMTABLE         table[CORE_SIZE][CORE_SIZE];
    } *h;
    char            label[32];
    signed          ret;
    unsigned        i, j, jj;
    unsigned        alloc_count = 0;
    R              *row;

    h = malloc(sizeof(struct H));
    assert(h != NULL);
    for (jj = 0; jj < CORE_SIZE; jj++) {
        snprintf(label, sizeof(label), "test.%d", jj);
        ret = tmstat_create(&h->stat[jj], label);
        assert(ret == 0);
        /* Register. */
        for (j = 0; j < CORE_SIZE; j++) {
            snprintf(label, sizeof(label), "table_%d", j);
            ret = tmstat_table_register(h->stat[jj], &h->table[jj][j],
                label, cols, 2, sizeof(R));
            assert(ret == 0);
        }
        ret = tmstat_publish(h->stat[jj], "test");
        assert(ret == 0);
    }

    ret = tmstat_subscribe(&h->sub_stat, "test");
    assert(ret == 0);

    memset(h->row, 0, sizeof(h->row));

    for (jj = 0; jj < CORE_SIZE; jj++) {
        for (j = 0; j < CORE_SIZE; j++) {
            for (i = 0; i < (j + 1); i++) {
                ret = tmstat_row_create(h->stat[jj], h->table[jj][j],
                    &h->row[jj][j][i]);
                assert(ret == 0);
                alloc_count++;
                tmstat_row_field(h->row[jj][j][i], NULL, &row);
                row->num = i;
                if (i % 4 == 0) {
                    /* Mark for query. */
                    strcpy(row->text, marker);
                }
            }
        }
    }

    /* Force SIGFPE. */
    zero /= zero;

    return EXIT_SUCCESS;
#undef CORE_SIZE
}


static void
usage(int rc, FILE *out, const char *program_name)
{
    fprintf(out, usage_str, program_name);
    exit(rc);
}

/*
 * Statistics subsystem insepection utility.
 */
int
main(int argc, char * const argv[])
{
    int ret = EXIT_SUCCESS;
    int c, option_index;
    char directory[PATH_MAX] = TMSTAT_DIR_SUBSCRIBE;
    bool tested_something = false;
    struct timeval tv;

    gettimeofday(&tv, NULL);
    seed = tv.tv_usec;
    srandom(seed);

    for (;;) {
        option_index = 0;
        c = getopt_long(argc, argv, options, long_options, &option_index);
        switch (c) {
        case -1:
            /* End of options. */
            goto end_options;

        case '?':
            /* Unknown option. */
            ret = EXIT_FAILURE;
            usage(EXIT_FAILURE, stderr, argv[0]);

        case 'b': {
            /* -b, --base=PATH: Set segment directory base path. */
            tmstat_path = optarg;
            break;
        }

        case 'd':
            /* -d, --dir=DIR: Subscribe to specific directory. */
            snprintf(directory, sizeof(directory), "%s", optarg);
            break;

        case 'h':
            /* -h, --help: Show usage. */
            usage(0, stdout, argv[0]);

        case 'M':
            /* M, --max-errors=N: Display at most N error reports. */
            max_errors = strtoul(optarg, NULL, 10);
            break;

        case 'T':
            /* T, --merge-test=PARAMS: Merge test with specified parameters. */
            tested_something = true;
            ret = test_merge_with_params(optarg);
            if (ret != 0) {
                goto out;
            }
            break;

        case 's':
            /* s, --seed=S: PRNG seed. */
            seed = strtoul(optarg, NULL, 10);
            srandom(seed);
            break;

        case 't':
            /* -t, --test: Perform test. */
            tested_something = true;
            if (strcmp(optarg, "eval") == 0) {
                ret = test_eval();
            } else if (strcmp(optarg, "bugs") == 0) {
                ret = test_bugs();
            } else if (strcmp(optarg, "cmod") == 0) {
                ret = test_concurrent_modification();
            } else if (strcmp(optarg, "core") == 0) {
                ret = test_core();
            } else if (strcmp(optarg, "merge") == 0) {
                ret = test_merge();
            } else if (strcmp(optarg, "multi") == 0) {
                ret = test_multi();
            } else if (strcmp(optarg, "mrgperf") == 0) {
                ret = test_merge_perf();
            } else if (strcmp(optarg, "parse-print") == 0) {
                ret = test_parse_print();
            } else if (strcmp(optarg, "reread") == 0) {
                ret = test_reread();
            } else if (strcmp(optarg, "rndmrg") == 0) {
                ret = test_merge_randomly();
            } else if (strcmp(optarg, "rollup") == 0) {
                ret = test_rollup();
            } else if (strcmp(optarg, "insn") == 0) {
                ret = test_insn();
            } else if (strcmp(optarg, "single") == 0) {
                ret = test_single();
            } else if (strcmp(optarg, "long-keys") == 0) {
                ret = test_long_keys();
            } else if (strcmp(optarg, "unterminated-keys") == 0) {
                ret = test_unterminated_keys();
            } else {
                ret = EXIT_FAILURE;
                warnx("unknown test: `%s'", optarg);
                fprintf(stderr, "Try `%s --help' for more information.\n",
                        argv[0]);
            }
            if (ret != 0) {
                goto out;
            }
            break;

        case 'v':
            verbose = true;
            break;
        }
    }
end_options:
    if (!tested_something) {
        warnx("which test do you want to run?");
        fprintf(stderr, "Try `%s --help' for more information.\n",
                argv[0]);
    }
out:
    return ret;
}
