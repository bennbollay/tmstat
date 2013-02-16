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
 * Statistics subsystem insepection utility.
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

void
display(TMSTAT tmstat, TMROW *row, unsigned row_count,
        TMCOL col, unsigned col_count, bool hide);

/*
 * Usage text (please sort options list alphabetically).
 */
static const char usage[] = 
   "usage: %s [OPTIONS] [TABLE [COL=VALUE]...]\n"
   "\n"
   "Inspect and manipulate statistics subsystem.\n"
   "\n"
   "Supported options:\n"
   "   -a, --all            Display all tables.\n"
   "   -b, --base=PATH      Set segment directory base path.\n"
   "   -c, --csv            Output in CSV format.\n"
   "   -d, --directory=DIR  Subscribe to specific directory.\n"
   "   -e, --eval EXPR      Evaluate expression.\n"
   "   -f, --file=PATH      Inspect specific segment file.\n"
   "   -h, --help           Display this text.\n"
   "   -i, --internal       Include internal tables.\n"
   "   -m, --merge=PATH     Merge subscribed segments into one segment file.\n"
   "   -r, --rollup         Merge all selected rows, ignoring keys.\n"
   "   -x, --extract=DIR    Extract segments into directory.\n"
   "   -w, --wrap=COLS      Wrap output at COLS columns.\n"
   "\n"
;

/*
 * Options (please sort options alphabetically).
 */
enum {
    OPT_ALL,
    OPT_BASE,
    OPT_CSV,
    OPT_DIR,
    OPT_EXTRACT,
    OPT_EVAL,
    OPT_FILE,
    OPT_HELP,
    OPT_INTERNAL,
    OPT_MERGE,
    OPT_ROLLUP,
    OPT_VERBOSE,
    OPT_WRAP,
    /* This entry is always last. */
    OPT_COUNT,
};
static struct option long_options[] = {
    [OPT_ALL]           = { "all",          no_argument,        NULL, 'a' },
    [OPT_BASE]          = { "base",         required_argument,  NULL, 'b' },
    [OPT_CSV]           = { "csv",          no_argument,        NULL, 'c' },
    [OPT_DIR]           = { "dir",          required_argument,  NULL, 'd' },
    [OPT_EVAL]          = { "eval",         required_argument,  NULL, 'e' },
    [OPT_EXTRACT]       = { "extract",      required_argument,  NULL, 'x' },
    [OPT_FILE]          = { "file",         required_argument,  NULL, 'f' },
    [OPT_HELP]          = { "help",         no_argument,        NULL, 'h' },
    [OPT_INTERNAL]      = { "internal",     no_argument,        NULL, 'i' },
    [OPT_MERGE]         = { "merge",        required_argument,  NULL, 'm' },
    [OPT_ROLLUP]        = { "rollup",       no_argument,        NULL, 'r' },
    [OPT_WRAP]          = { "wrap",         required_argument,  NULL, 'w' },
    [OPT_COUNT]         = { 0 },
};
static char options[] = "ab:cd:e:f:Hhijm:rx:w:";

/*
 * User preferences.
 */
static unsigned     wrap = 80;              /* Display wrap width. */
static bool         display_all = false;    /* Display all tables. */
static bool         internal = false;       /* Include internal tables. */
static bool         csv = false;            /* Display in csv format. */
static char        *merge_path = NULL;      /* Path to merge into. */
static bool         rollup = false;         /* Merge all selected rows? */

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
    case TMSTAT_T_DEC:
    case TMSTAT_T_HEX:
        s = malloc(tmstat_strlen(col->type, col->size));
        tmstat_print(p, s, col->type, col->size);
        break;

    default:
        /* Unknown format. */
        s = strdup("?");
        break;
    }

    return s;
}

static char *
format_value(TMROW row, const char *col_name)
{
    void       *p;
    char       *s;
    TMCOL       col;
    unsigned    col_idx, col_count;

    tmstat_row_info(row, &col, &col_count);
    for (col_idx = 0; col_idx < col_count; col_idx++) {
        if (strcmp(col[col_idx].name, col_name) == 0) {
            tmstat_row_field(row, (char *)col_name, &p);
            goto col_found;
        }
    }
    /* This row does not contain this field. */
    s = strdup("-");
    goto out;
col_found:
    assert(p != NULL);
    s = format_column(col + col_idx, p);
out:
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
 * Convert string pattern into column value.
 *
 * convert_pattern(col_value[i], &table_col[i], col_pattern[i]);
 */
static void
convert_pattern(void *value, TMCOL tmcol, char *pattern)
{
    memset(value, 0, tmcol->size);
    switch (tmcol->type) {

        /*
         * Signed integer.
         */
    case TMSTAT_T_SIGNED: {
        long long ll = atoll(pattern);

        switch (tmcol->size) {
        case 1:     *(int8_t *)value    = (int8_t)ll;          break;
        case 2:     *(int16_t *)value   = (int16_t)ll;         break;
        case 4:     *(int32_t *)value   = (int32_t)ll;         break;
        case 8:     *(int64_t *)value   = (int64_t)ll;         break;
        }
        break;
    }

        /*
         * Unsigned integer.
         */
    case TMSTAT_T_UNSIGNED: {
        long long ull = strtoull(pattern, NULL, 10);

        switch (tmcol->size) {
        case 1:     *(uint8_t *)value   = (uint8_t)ull;         break;
        case 2:     *(uint16_t *)value  = (uint16_t)ull;        break;
        case 4:     *(uint32_t *)value  = (uint32_t)ull;        break;
        case 8:     *(uint64_t *)value  = (uint64_t)ull;        break;
        }
        break;
    }

        /*
         * String.
         */
    case TMSTAT_T_TEXT: {
        snprintf(value, tmcol->size, "%s", pattern);
        break;
    }

        /*
         * Binary sequence.
         */
    case TMSTAT_T_BIN:
        /*
         * Dotted decimal sequence.
         */
    case TMSTAT_T_DEC:
        /*
         * Colon-seperated hexadecimal sequence.
         */
    case TMSTAT_T_HEX: {
        int ret = tmstat_parse(pattern, NULL, value, tmcol->type, tmcol->size);
        if (ret != 0) {
            errx(EXIT_FAILURE, "invalid key string: `%s'", pattern);
        }
        break;
    }

    default:
        errx(EXIT_FAILURE, "unknown column type %d", tmcol->type);
    }
}

/*
 * Display result set.
 */
void
display(TMSTAT tmstat, TMROW *row, unsigned row_count,
        TMCOL col, unsigned col_count, bool hide)
{
    unsigned    w, len, col_start, col_end, col_idx, row_idx;
    char       *s;
    unsigned   *width;
    char      **text;
    char      **fmt;

    width = malloc(sizeof(unsigned) * col_count);
    if (width == NULL) {
        err(1, NULL);
    }
    text = malloc(sizeof(char *) * row_count * col_count);
    if (text == NULL) {
        err(1, NULL);
    }
    fmt = malloc(sizeof(char *) * col_count);
    if (fmt == NULL) {
        err(1, NULL);
    }

    /*
     * Generate result strings.
     */
    memset(width, 0, sizeof(unsigned) * col_count);
    for (row_idx = 0; row_idx < row_count; row_idx++) {
        for (col_idx = 0; col_idx < col_count; col_idx++) {
            s = format_value(row[row_idx], col[col_idx].name);
            text[row_idx * col_count + col_idx] = s;
            len = strlen(s);
            if (len > width[col_idx]) {
                /* Track widest column. */
                width[col_idx] = len;
            }
        }
    }

    /*
     * Calculate final widths and justification.
     */
    for (col_idx = 0; col_idx < col_count; col_idx++) {
        /* Calculate final width. */
        len = strlen(col[col_idx].name);
        if (len > width[col_idx]) {
            width[col_idx] = len;
        }
        /* Calculate justification. */
        switch (col[col_idx].type) {
        case TMSTAT_T_SIGNED:
        case TMSTAT_T_UNSIGNED:
            fmt[col_idx] = "%*s";
            break;
        default:
            fmt[col_idx] = "%-*s";
            break;
        }
    }

    /*
     * Print, one wrap width at a time.
     */
    for (col_start = 0; col_start < col_count; col_start = col_end) {
        /* Search for the last column we can display. */
        for (w = 0, col_end = col_start;
             (col_end < col_count) && (csv || (w + width[col_end] < wrap));
             w += width[col_end++] + 1) {
        }
        if (col_end == col_start) {
            /* Display at least one column. */
            col_end++;
        }
        /* Print header names. */
        for (col_idx = col_start; col_idx < col_end; col_idx++) {
            if (csv) {
                printf("%s", col[col_idx].name);
            } else {
                printf(fmt[col_idx], width[col_idx], col[col_idx].name);
            }
            putchar((col_idx < col_end - 1) ? (csv ? ',' : ' ') : '\n');
        }
        /* Print header dividers. */
        if (!csv) {
            for (col_idx = col_start; col_idx < col_end; col_idx++) {
                for (unsigned j = 0; j < width[col_idx]; j++) putchar('-');
                putchar((col_idx < col_end - 1) ? ' ' : '\n');
            }
        }
        /* Print rows. */
        for (row_idx = 0; row_idx < row_count; row_idx++) {
            if (hide &&
                ((text[row_idx * col_count][0] == '.') ||
                 (!internal &&
                         (strchr(text[row_idx * col_count], '/') != NULL)))) {
                /* Skip 'hidden' entries. */
                continue;
            }
            for (col_idx = col_start; col_idx < col_end; col_idx++) {
                if (csv) {
                    printf("%s", text[row_idx * col_count + col_idx]);
                } else {
                    printf(fmt[col_idx], width[col_idx],
                            text[row_idx * col_count + col_idx]);
                }
                putchar((col_idx < col_end - 1) ? (csv ? ',' : ' ') : '\n');
            }
        }
        putchar('\n');
    }

    /*
     * Clean up.
     */
    for (unsigned row_idx = 0; row_idx < row_count; row_idx++) {
        for (unsigned col_idx = 0; col_idx < col_count; col_idx++) {
            free(text[row_idx * col_count + col_idx]);
        }
    }
    free(width);
    free(text);
    free(fmt);
}

/*
 * Perform query:
 *
 *      TABLE_NAME [COLUMN_NAME=COLUMN_VALUE]...
 */
static void
query(TMSTAT tmstat, int argc, char * const argv[], bool hide)
{
    char           *table_name = argv[0];
    unsigned        col_count = argc - 1;
    signed          ret;
    TMCOL           table_col;
    unsigned        table_col_count;
    TMROW          *rows;
    TMROW           row;
    unsigned        match_count;
    char          **col_name;
    char          **col_pattern;
    void          **col_value;
    char           *eq;
    int             len;

    col_name = malloc(sizeof(char *) * col_count);
    if (col_name == NULL) {
        err(EXIT_FAILURE, "malloc");
    }
    col_pattern = malloc(sizeof(char *) * col_count);
    if (col_pattern == NULL) {
        err(EXIT_FAILURE, "malloc");
    }
    col_value = malloc(sizeof(char *) * col_count);
    if (col_value == NULL) {
        err(EXIT_FAILURE, "malloc");
    }
    /* Parse query string. */
    for (unsigned i = 0; i < col_count; i++) {
        col_name[i] = malloc(strlen(argv[i + 1]));
        col_pattern[i] = malloc(strlen(argv[i + 1]));
        eq = strchr(argv[i + 1], '=');
        if (eq == NULL) {
            errx(EXIT_FAILURE, "%s: Invalid match pattern.", argv[i + 1]);
        }
        len = eq - argv[i + 1];
        memcpy(col_name[i], argv[i + 1], len);
        col_name[i][len] = '\0';
        len = strlen(argv[i + 1]) - len + 1;
        memcpy(col_pattern[i], eq + 1, len);
        col_pattern[i][len] = '\0';
    }
    /* Construct match values. */
    tmstat_table_info(tmstat, table_name, &table_col, &table_col_count);
    if (table_col_count == 0) {
        errx(EXIT_FAILURE, "%s: No such table.", table_name);
    }
    for (unsigned i = 0; i < col_count; i++) {
        for (unsigned j = 0; j < table_col_count; j++) {
            if (strcmp(col_name[i], table_col[j].name) == 0) {
                if (table_col[j].rule != TMSTAT_R_KEY) {
                    errx(EXIT_FAILURE, "column %s: Not a key.", col_name[i]);
                }
                col_value[i] = malloc(table_col[j].size);
                if (col_value[i] == NULL) {
                    err(EXIT_FAILURE, "malloc");
                }
                convert_pattern(col_value[i], &table_col[j], col_pattern[i]);
                goto next_col;
            }
        }
        errx(EXIT_FAILURE, "%s: No such column.", col_name[i]);
next_col: ;
    }
    /* Perform query. */
    if (rollup) {
        ret = tmstat_query_rollup(tmstat, table_name, col_count, col_name,
            col_value, &row);
        if ( (ret != 0) || (NULL == row) ) {
            err(EXIT_FAILURE, "tmstat_query_rollup");
        }
        rows = calloc(sizeof(TMROW), 1);
        if (rows == NULL) {
            err(EXIT_FAILURE, "out of memory");
        }
        rows[0] = row;
        match_count = 1;
    } else {
        ret = tmstat_query(tmstat, table_name, col_count, col_name, col_value,
            &rows, &match_count);
        if (ret != 0) {
            err(EXIT_FAILURE, "tmstat_query");
        }
    }
    /* Display. */
    /*
     * We must call tmstat_table_info again because tmstat_query may
     * have re-read the stats, in which case the old table_col value
     * will be stale (that is, will point to freed memory).
     */
    tmstat_table_info(tmstat, table_name, &table_col, &table_col_count);
    display(tmstat, rows, match_count, table_col, table_col_count, hide);
    /* Free. */
    for (unsigned i = 0; i < match_count; i++) {
        tmstat_row_drop(rows[i]);
    }
    free(rows);
    for (unsigned i = 0; i < col_count; i++) {
        free(col_name[i]);
        free(col_value[i]);
    }
    free(col_name);
    free(col_pattern);
    free(col_value);
}

/*
 * Display all tables.
 */
static void
dump_all(TMSTAT tmstat)
{
    TMROW          *row;
    unsigned        table_count;
    signed          ret;
    char           *name;

    /* Obtain list of tables. */
    ret = tmstat_query(tmstat, ".table", 0, NULL, NULL, &row, &table_count);
    if (ret != 0) {
        err(EXIT_FAILURE, "tmstat_query");
    }
    /* Display each one (skipping system table entries). */
    for (unsigned i = 0; i < table_count; i++) {
        /* Fetch name. */
        name = format_value(row[i], "name");
        if ((name[0] != '.') && (internal || (strchr(name, '/') == NULL))) {
            /* Display table header. */
            printf("\n%s\n", name);
            for (unsigned j = 0; j < strlen(name); j++) putchar('=');
            printf("\n\n");
            /* Display table. */
            query(tmstat, 1, &name, false);
        }
        /* Free. */
        tmstat_row_drop(row[i]);
        free(name);
    }
    free(row);
}

/*
 * Statistics subsystem insepection utility.
 */
int
main(int argc, char * const argv[])
{
    int ret = EXIT_SUCCESS;
    int c, option_index, rc;
    TMSTAT tmstat = NULL;
    char directory[PATH_MAX] = TMSTAT_DIR_SUBSCRIBE;
    char *expr = NULL;
    TMSTAT_EVAL eval;
    char *expr_result, *expr_errstr;
    unsigned expr_erridx;
    char *file_path = NULL;
    char *extract_dir = NULL;
    bool extract = false;

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
            goto usage;

        case 'a':
            /* -a, --all: display all tables. */
            display_all = true;
            break;

        case 'b': {
            /* -b, --base=PATH: Set segment directory base path. */
            extern THREAD char *tmstat_path;
            tmstat_path = optarg;
            break;
        }

        case 'c':
            /* -c, --csv: output in csv format. */
            csv = true;
            break;

        case 'd':
            /* -d, --dir=DIR: Subscribe to specific directory. */
            snprintf(directory, sizeof(directory), "%s", optarg);
            break;

        case 'H':
        case 'e':
            /* -e, --eval EXPR: Evaluate expression. */
            /* (also -H EXPR for backwards compatibility). */
            expr = optarg;
            break;

        case 'f':
            /* -f, --file=PATH: Read specific segment file. */
            free(file_path);
            file_path = optarg;
            break;

        case 'h':
            /* -h, --help: Show usage. */
            goto usage;

        case 'j':
            /* -j: alias to -i to maintain similarity with bigpipe. */
        case 'i':
            /* -i, --internal: Include internal tables. */
            internal = true;
            break;

        case 'm':
            /* m, --merge=PATH: Merge subscribed segments into one file. */
            free(merge_path);
            merge_path = optarg;
            break;

        case 'r':
            /* -r, --rollup: Merge all selected rows, ignoring keys. */
            rollup = true;
            break;

        case 'x':
            /* -x, --extract: Extract segments into directory. */
            free(extract_dir);
            extract_dir = optarg;
            extract = true;
            break;

        case 'w':
            /* -w, --wrap=COLS: Change wrap width. */
            wrap = atoi(optarg);
            if (wrap == 0) {
                errx(EXIT_FAILURE, "%s: Invalid wrap width.", optarg);
            }
            break;
        }
    }
end_options:
    argc -= optind;
    argv += optind;

    if (file_path != NULL) {
        if (!extract) {
            rc = tmstat_read(&tmstat, file_path);
            if (rc != 0) {
                /* Read failure; tmstat_read/extract warns. */
                goto out;
            }
        } else {
            if (extract_dir == NULL) {
                extract_dir = ".";
            }
            rc = tmstat_extract(file_path, extract_dir);
            /* Extract complete; tmstat_extract warns on failure. */
            goto out;
        }
    }

    if (tmstat == NULL) {
        rc = tmstat_subscribe(&tmstat, directory);
        if (rc != 0) {
            /* Subscription failure; tmstat_subscribe sets errno. */
            err(EXIT_FAILURE, "tmstat_subscribe %s", directory);
        }
    }
    if (tmstat == NULL) {
        errx(EXIT_FAILURE, "No segment to display.");
    }

    if (expr != NULL) {
        /* Evaluate expression. */
        rc = tmstat_eval_create(&eval);
        if (rc != 0) {
            err(EXIT_FAILURE, "tmstat_eval_create");
        }
        rc = tmstat_eval(eval, tmstat, expr, &expr_result,
            &expr_errstr, &expr_erridx);
        if (rc != 0) {
            errx(EXIT_FAILURE, "expression error: col %u: %s",
                expr_erridx, expr_errstr);
        }
        printf("%s\n", expr_result);
        free(expr_result);
        tmstat_eval_destroy(eval);
        goto out;
    }

    if (display_all) {
        /* Display all tables. */
        dump_all(tmstat);
        goto out;
    }

    if (merge_path != NULL) {
        ret = tmstat_merge(tmstat, merge_path, internal);
        if (ret != 0) {
            warn("tmstat_merge");
        }
        goto out;
    }

    if (argc > 0) {
        /* Perform query. */
        query(tmstat, argc, argv, false);
    } else {
        char *label = ".label";
        char *table = ".table";
        /* Display the label and the table descriptors by default. */
        printf("Segment labels:\n\n");
        query(tmstat, 1, &label, false);
        printf("Segment tables:\n\n");
        query(tmstat, 1, &table, true);
    }
    goto out;

usage:
    fprintf(stderr, usage, argv[0]);
out:
    tmstat_destroy(tmstat);
    return ret;
}
