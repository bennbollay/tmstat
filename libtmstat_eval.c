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
 */
#define _GNU_SOURCE
#include <ctype.h>
#include <err.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tmstat.h"

/**
 * Parser context.
 */
struct TMSTAT_EVAL {
    TMSTAT              tmstat;         //!< Current segment.
    const char         *errp;           //!< Pointer to offending character.
    char               *errstr;         //!< Static error description.
    char               *fieldtxt;       //!< Field text (if any).
};

/*
 * Private prototypes.
 */
static void         tmstat_eval_err(TMSTAT_EVAL, const char *, const char *);
static signed       tmstat_eval_func_max(TMSTAT_EVAL, unsigned,
                        long long *, char **, long long *);
static signed       tmstat_eval_func_min(TMSTAT_EVAL, unsigned,
                        long long *, char **, long long *);
static unsigned     tmstat_parse_argument_list(TMSTAT_EVAL, char *,
                        unsigned, unsigned *, char ***, long long **);
static void         tmstat_parse_pattern(TMSTAT_EVAL, TMCOL, char *, void *);
static unsigned     tmstat_parse_col_name(TMSTAT_EVAL, char *, char **);
static unsigned     tmstat_parse_factor(TMSTAT_EVAL, char *, long long *);
static unsigned     tmstat_parse_field(TMSTAT_EVAL, char *, long long *);
static unsigned     tmstat_parse_function(TMSTAT_EVAL, char *, long long *);
static unsigned     tmstat_parse_function_name(TMSTAT_EVAL, char *, char **);
static unsigned     tmstat_parse_integer(TMSTAT_EVAL, char *, long long *);
static unsigned     tmstat_parse_table_name(TMSTAT_EVAL, char *, char **);
static unsigned     tmstat_parse_query(TMSTAT_EVAL, char *, char *, char **,
                        void **);
static unsigned     tmstat_parse_query_list(TMSTAT_EVAL, char *, char *,
                        unsigned, unsigned *, char ***, void ***);
static unsigned     tmstat_parse_sum(TMSTAT_EVAL, char *, long long *);
static unsigned     tmstat_parse_search(TMSTAT_EVAL, char *, TMROW **,
                        unsigned *);
static unsigned     tmstat_parse_string(TMSTAT_EVAL, char *, char **);
static unsigned     tmstat_parse_test(TMSTAT_EVAL, char *, long long *);
static unsigned     tmstat_parse_truth(TMSTAT_EVAL, char *, long long *);
static unsigned     tmstat_parse_value(TMSTAT_EVAL, char *, long long *);

/**
 * Function descriptor table.
 */
THREAD struct {
    char       *name;           //<! Function name.
    signed     (*func)          //<! Function implementation.
        (TMSTAT_EVAL, unsigned, long long *, char **, long long *);
} tmstat_eval_function[] = {
    { .name = "MAX",        .func = tmstat_eval_func_max },
    { .name = "MIN",        .func = tmstat_eval_func_min },
};

/*
 * Convenience macros.
 */
#define array_count(a)          (sizeof(a) / sizeof((a)[0]))

/*
 * Mark error.
 */
static inline void
tmstat_eval_err(TMSTAT_EVAL eval, const char *errp, const char *errstr)
{
    if (errp != NULL) {
        eval->errp = (char *)errp;
    }
    eval->errstr = (char *)errstr;
}

/*
 * Create evaluation context.
 */
int
tmstat_eval_create(TMSTAT_EVAL *eval)
{
    TMSTAT_EVAL p;

    p = calloc(1, sizeof(struct TMSTAT_EVAL));
    *eval = p;
    return (p != NULL) ? 0 : -1;
}

/*
 * Destroy evaluation context.
 */
void
tmstat_eval_destroy(TMSTAT_EVAL eval)
{
    free(eval->fieldtxt);
    free(eval);
}

/*
 * Evaluate expression.
 */
int
tmstat_eval(TMSTAT_EVAL eval, TMSTAT stat, const char *expr,
            char **result, char **errstr, unsigned *erridx)
{
    int idx, ret;

    eval->tmstat = stat;
    tmstat_eval_err(eval, expr, "unspecified error");
    idx = tmstat_parse_string(eval, (char *)expr, result);
    ret = (idx == strlen(expr)) ? 0 : -1;
    if (errstr != NULL) {
        *errstr = eval->errstr;
    }
    if (erridx != NULL) {
        *erridx = 0;
        if (eval->errp != NULL) {
            *erridx = eval->errp - expr;
        }
    }
    return ret;
}

/*
 * Evaluate expression as a signed integer.
 */
int
tmstat_eval_signed(TMSTAT_EVAL eval, TMSTAT stat, const char *expr,
                   signed long long *result, char **errstr, unsigned *erridx)
{
    int idx, ret;

    eval->tmstat = stat;
    tmstat_eval_err(eval, expr, "unspecified error");
    idx = tmstat_parse_test(eval, (char *)expr, result);
    ret = (idx == strlen(expr)) ? 0 : -1;
    if (errstr != NULL) {
        *errstr = eval->errstr;
    }
    if (erridx != NULL) {
        *erridx = 0;
        if (eval->errp != NULL) {
            *erridx = eval->errp - expr;
        }
    }
    return ret;
}

/**
 * Return the largest argument.
 *
 * @param[in]   eval    Evaluation context.
 * @param[in]   argc    Argument count.
 * @param[in]   argv    Argument list.
 * @param[in]   args    Argument text.
 * @param[out]  val     Resultant value.
 * @return 0 on success, -1 on failure.
 */
signed
tmstat_eval_func_max(TMSTAT_EVAL eval, unsigned argc, long long *argv,
                     char **args, long long *val)
{
    unsigned i;
    long long v;

    if (argc == 0) {
        tmstat_eval_err(eval, NULL, "MAX() requires at least one argument");
        return -1;
    }
    v = argv[0];
    for (i = 1; i < argc; i++) {
        if (argv[i] > v) {
            v = argv[i];
        }
    }
    *val = v;
    return 0;
}

/**
 * Return the smallest argument.
 *
 * @param[in]   eval    Evaluation context.
 * @param[in]   argc    Argument count.
 * @param[in]   argv    Argument list.
 * @param[in]   args    Argument text.
 * @param[out]  val     Resultant value.
 * @return 0 on success, -1 on failure.
 */
signed
tmstat_eval_func_min(TMSTAT_EVAL eval, unsigned argc, long long *argv,
                     char **args, long long *val)
{
    unsigned i;
    long long v;

    if (argc == 0) {
        tmstat_eval_err(eval, NULL, "MIN() requires at least one argument");
        return -1;
    }
    v = argv[0];
    for (i = 1; i < argc; i++) {
        if (argv[i] < v) {
            v = argv[i];
        }
    }
    *val = v;
    return 0;
}

/**
 * Parse argument-list:
 *
 *  argument-list  ::= argument-list ',' test
 *                 |   test
 *                 ;
 *
 * @param[in]   text        Text to parse.
 * @param[in]   depth       Current list depth.
 * @param[out]  argc        Resultant argument count.
 * @param[out]  args        Resultant argument text.
 * @param[out]  argv        Resultant argument values.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_argument_list(TMSTAT_EVAL eval, char *text, unsigned depth,
                        unsigned *argc, char ***args, long long **argv)
{
    unsigned idx;
    char *s = NULL;
    long long v = 0;
    
    idx = tmstat_parse_test(eval, text, &v);
    if (idx == 0) {
        goto out;
    }
    s = malloc(idx + 1);
    if (s == NULL) {
        tmstat_eval_err(eval, text, "malloc failure");
        goto out;
    }
    memcpy(s, text, idx);
    s[idx] = '\0';
    while (text[idx] == ' ') idx++;
    if (text[idx] == ',') {
        idx++;
        while (text[idx] == ' ') idx++;
        idx += tmstat_parse_argument_list(eval, &text[idx], depth + 1,
            argc, args, argv);
    } else {
        *args = calloc(sizeof(char *), depth + 1);
        *argv = calloc(sizeof(long long), depth + 1);
        *argc = depth + 1;
    }
    (*args)[depth] = s;
    (*argv)[depth] = v;
out:
    return idx;
}

/**
 * Convert string pattern into column value.
 *
 * @param[in]   tmcol   Column to convert for.
 * @param[in]   pattern Text pattern to convert from.
 * @param[out]  value   Resultant value.
 */
static void
tmstat_parse_pattern(TMSTAT_EVAL eval, TMCOL tmcol, char *pattern, void *value)
{
    int ret;

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
        ret = tmstat_parse(pattern, NULL, value, tmcol->type, tmcol->size);
        if (ret != 0) {
            tmstat_eval_err(eval, pattern, "invalid key string");
        }
        break;
    }

    default:
        tmstat_eval_err(eval, pattern, "unknown column type");
    }
}

/**
 * Parse col-name:
 *
 *      [a-z0-9][a-z0-9_.]*
 *
 * @param[in]   text        Text to parse.
 * @param[out]  val         Resultant value; caller must free.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_col_name(TMSTAT_EVAL eval, char *text, char **val)
{
    unsigned i = 0;

    if (islower(text[0]) || isdigit(text[0])) {
        while (islower(text[i]) || isdigit(text[i]) ||
               (text[i] == '_') || (text[i] == '.')) i++;
    }
    if (i > 0) {
        *val = malloc(i + 1);
        memcpy(*val, text, i);
        (*val)[i] = '\0';
    } else {
        *val = NULL;
    }
    return i;
}

/**
 * Parse factor:
 *
 *  factor      ::= factor '*' value
 *              |   factor '/' value
 *              |   factor '%' value
 *              |   value
 *              ;
 *
 * @param[in]   text        Text to parse.
 * @param[out]  val         Resultant value.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_factor(TMSTAT_EVAL eval, char *text, long long *val)
{
    long long v1 = 0, v2;
    unsigned idx, idx2;
    char op;

    idx = tmstat_parse_value(eval, text, &v1);
    if (idx == 0) {
        return 0;
    }
    for (;;) {
        while ((text[idx] != '\0') && (text[idx] <= ' ')) idx++; 
        if ((text[idx] != '*') && (text[idx] != '/') && (text[idx] != '%')) {
            break;
        }
        op = text[idx++];
        while ((text[idx] != '\0') && (text[idx] <= ' ')) idx++; 
        idx2 = tmstat_parse_value(eval, &text[idx], &v2);
        if (idx2 == 0) {
            return 0;
        }
        free(eval->fieldtxt);
        eval->fieldtxt = NULL;
        switch (op) {
        case '*':   v1 = v1 * v2;                                       break;
        case '/':   v1 = (v2 != 0) ? v1 / v2 : 0;                       break;
        case '%':   v1 = (v2 != 0) ? v1 % v2 : 0;                       break;
        }
        idx += idx2;
    }
    *val = v1;
    return idx;
}

/**
 * Parse field:
 *
 *  field       ::=  search '[' integer ']' '.' name
 *              |    search '.' name
 *              |    '$' '.' name
 *              ;
 *
 * If the index integer is omitted, the first row (row 0) is selected.
 * The '$' reference refers to the current iterator row.
 *
 * @param[in]   text        Text to parse.
 * @param[out]  val         Resultant value.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_field(TMSTAT_EVAL eval, char *text, long long *val)
{
    TMROW      *row = NULL;
    unsigned    row_count = 0;
    unsigned    row_idx = 0;
    TMCOL       col;
    char       *col_name = NULL;
    unsigned    col_count, i, idx;
    long long   u;

    idx = tmstat_parse_search(eval, text, &row, &row_count);
    if (idx == 0) {
        goto out;
    }
    if (text[idx] == '[') {
        idx++;
        idx += tmstat_parse_integer(eval, &text[idx], &u);
        if (text[idx] != ']') {
            tmstat_eval_err(eval, &text[idx], "missing ']'");
            goto out;
        }
        idx++;
        row_idx = u;
        if (row_idx >= row_count) {
            tmstat_eval_err(eval, &text[idx], "row idx too large");
            goto out;
        }
    }
    if (text[idx] != '.') {
        tmstat_eval_err(eval, &text[idx], "missing '.'");
        goto out;
    }
    idx++;
    idx += tmstat_parse_col_name(eval, &text[idx], &col_name);
    if (col_name == NULL) {
        tmstat_eval_err(eval, &text[idx], "invalid column name");
        goto out;
    }
    if (row_count > 0) {
        tmstat_row_info(row[row_idx], &col, &col_count);
        for (i = 0; i < col_count; i++) {
            if (strcmp(col[i].name, col_name) == 0) {
                if (col[i].type == TMSTAT_T_TEXT) {
                    free(eval->fieldtxt);
                    tmstat_row_field(row[row_idx], NULL, &eval->fieldtxt);
                    *val = 0;
                    eval->fieldtxt += col[i].offset;
                    eval->fieldtxt = strdup(eval->fieldtxt);
                    goto out;
                }
                if ((col[i].type == TMSTAT_T_BIN) ||
                    (col[i].type == TMSTAT_T_DEC) ||
                    (col[i].type == TMSTAT_T_HEX)) {
                    uint8_t *in;
                    free(eval->fieldtxt);
                    eval->fieldtxt
                        = malloc(tmstat_strlen(col[i].type, col[i].size));
                    *val = 0;
                    tmstat_row_field(row[row_idx], NULL, &in);
                    in += col[i].offset;
                    tmstat_print(in, eval->fieldtxt, col[i].type, col[i].size);
                    goto out;
                }
                break;
            }
        }
        *val = tmstat_row_field_signed(row[row_idx], col_name);
        free(eval->fieldtxt);
        eval->fieldtxt = NULL;
    } else {
        *val = 0;
        free(eval->fieldtxt);
        eval->fieldtxt = NULL;
    }
out:
    for (i = 0; i < row_count; ++i) {
        tmstat_row_drop(row[i]);
    }
    free(row);
    free(col_name);
    return idx;
}

/**
 * Parse function:
 *
 *  function    ::=  function-name '(' argument-list ')'
 *              ;
 *
 * @param[in]   text        Text to parse.
 * @param[out]  val         Resultant value.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_function(TMSTAT_EVAL eval, char *text, long long *val)
{
    signed ret = 0;
    unsigned idx = 0;
    char *name = 0;
    long long v = 0;
    unsigned argc = 0;
    char **args = NULL;
    long long *argv = NULL;
    unsigned i;

    /* Parse everything. */
    idx = tmstat_parse_function_name(eval, text, &name);
    if (idx == 0) {
        goto out;
    }
    if (text[idx] != '(') {
        tmstat_eval_err(eval, &text[idx], "missing '('");
        goto out;
    }
    idx++;
    idx += tmstat_parse_argument_list(eval, &text[idx], 0, &argc, &args, &argv);
    if (text[idx] != ')') {
        tmstat_eval_err(eval, &text[idx], "missing ')'");
        goto out;
    }
    idx++;
    /* Locate the function. */
    for (i = 0; i < array_count(tmstat_eval_function); i++) {
        if (strcmp(tmstat_eval_function[i].name, name) == 0) {
            /* Invoke it. */
            ret = tmstat_eval_function[i].func(eval, argc, argv, args, &v);
            goto out;
        }
    }
    tmstat_eval_err(eval, text, "unknown function");
out:
    if (argv != NULL) {
        while (argc > 0) {
            free(args[--argc]);
        }
    }
    free(args);
    free(argv);
    free(name);
    *val = v;
    return (ret == 0) ? idx : 0;
}

/**
 * Parse function-name:
 *
 *      [A-Z]+
 *
 * @param[in]   text        Text to parse.
 * @param[out]  val         Resultant value; caller must free.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_function_name(TMSTAT_EVAL eval, char *text, char **val)
{
    unsigned i = 0;

    while (isupper(text[i])) i++;
    if (i > 0) {
        *val = malloc(i + 1);
        memcpy(*val, text, i);
        (*val)[i] = '\0';
    } else {
        tmstat_eval_err(eval, text, "invalid function name");
        *val = NULL;
    }
    return i;
}

/**
 * Parse integer:
 *
 *      -?[0-9]+[MKGT]?[Bb]?
 *
 *  Numbers are always base 10 regardless of any leading zeros.
 *  The optional prefix specifies a standard scaling factor:
 *
 *      Metric:             Software:
 *          1Tb = 1,000Gb       1TB = 1,024GB
 *          1Gb = 1,000Mb       1GB = 1,024MB
 *          1Mb = 1,000Kb       1MB = 1,024KB
 *          1Kb = 1,000         1KB = 1,024
 *
 * @param[in]   text        Text to parse.
 * @param[out]  val         Resultant value.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_integer(TMSTAT_EVAL eval, char *text, long long *val)
{
    unsigned            i = 0;
    unsigned long long  b, u = 0;
    bool                negative = false;

    if (text[0] == '\0') {
        goto out;
    }
    if (text[0] == '-') {
        negative = true;
        i++;
    }
    u = 0;
    for (; isdigit(text[i]); i++) u = (u * 10) + (text[i] - '0');
    b = (text[i + 1] == 'B') ? 1024 : 1000;
    switch (text[i]) {
    case 'T':   u *= b * b * b * b;     break;
    case 'G':   u *= b * b * b;         break;
    case 'M':   u *= b * b;             break;
    case 'K':   u *= b;                 break;
    }
out:
    *val = (negative ? -u : u);
    return i;
}

/**
 * Parse search:
 *
 *  search      ::=  name '(' query-list ')'
 *              |    name
 *              ;
 *
 * If the query-list is omitted, all rows of the table are returned.
 *
 * @param[in]   text        Text to parse.
 * @param[out]  row         Resultant rows.
 * @param[out]  count       Total rows returned.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_search(TMSTAT_EVAL eval, char *text, TMROW **row, unsigned *count)
{
    char *name = NULL;
    char **col_names = NULL;
    void **col_values = NULL;
    unsigned col_count = 0, idx;
    int ret;

    idx = tmstat_parse_table_name(eval, text, &name);
    if (idx == 0) {
        /* Invalid name. */
        goto fail;
    }
    if (text[idx] == '(') {
        idx++;
        idx += tmstat_parse_query_list(eval, &text[idx], name, 0,
            &col_count, &col_names, &col_values);
        if (text[idx] != ')') {
            tmstat_eval_err(eval, &text[idx], "missing ')'");
            /* Invalid query-list. */
            goto fail;
        }
        idx++;
    }
    ret = tmstat_query(eval->tmstat, name, col_count, col_names, col_values,
            row, count);
    if (ret != 0) {
        /* Query failure. */
        tmstat_eval_err(eval, text, "query failure");
        goto fail;
    }
    goto out;

fail:
    *row = NULL;
    *count = 0;
out:
    free(name);
    free(col_names);
    free(col_values);
    return idx;
}

/**
 * Parse string:
 *
 *  substring-list
 *
 *  string      ::= '"' [^"]* '"'
 *              |   test
 *              ;
 *
 * @param[in]   text        Text to parse.
 * @param[out]  val         Resultant value; caller must free.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_string(TMSTAT_EVAL eval, char *text, char **val)
{
    unsigned i = 0;
    long long v;
    char *s = NULL;

    if (text[0] != '"') {
        free(eval->fieldtxt);
        eval->fieldtxt = NULL;
        i = tmstat_parse_test(eval, text, &v);
        if (eval->fieldtxt != NULL) {
            s = strdup(eval->fieldtxt);
        } else {
            asprintf(&s, "%lld", v);
        }
        goto out;
    }
    for (i = 1; text[i] != '"'; i++) {
        if (text[i] == '\0') {
            tmstat_eval_err(eval, text, "unterminated string");
            goto out;
        }
    }
    s = malloc(i);
    memcpy(s, &text[1], i - 1);
    s[i - 1] = '\0';
    i++;
out:
    *val = s;
    return i;
}

/**
 * Parse query:
 *
 *  query       ::= col-name '=' test
 *              |   col-name '=' string
 *              ;
 *
 * @param[in]   text        Text to parse.
 * @param[in]   table_name  Table to query.
 * @param[out]  col_name    Resultant column name.
 * @param[out]  val         Resultant value.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_query(TMSTAT_EVAL eval, char *text, char *table_name, char **col_name, void **val)
{
    char *name = NULL;
    void *value = NULL;
    char *s = NULL;
    unsigned col_idx, idx, col_count;
    TMCOL cols;

    tmstat_table_info(eval->tmstat, table_name, &cols, &col_count);
    idx = tmstat_parse_col_name(eval, text, &name);
    if (idx == 0) {
        goto out;
    }
    for (col_idx = 0; col_idx < col_count; col_idx++) {
        if (strcmp(name, cols[col_idx].name) == 0) {
            goto col_found;
        }
    }
    goto out;
col_found:
    if (text[idx] != '=') {
        tmstat_eval_err(eval, &text[idx], "missing '='");
        goto out;
    }
    idx++;
    idx += tmstat_parse_string(eval, &text[idx], &s);
    value = malloc(cols[col_idx].size);
    tmstat_parse_pattern(eval, &cols[col_idx], s, value);
out:
    *col_name = name;
    *val = value;
    free(s);
    return idx;
}

/**
 * Parse query-list:
 *
 *  query-list  ::= query-list ',' query
 *              |   query
 *              ;
 *
 * @param[in]   text        Text to parse.
 * @param[in]   table_name  Table to query.
 * @param[in]   depth       Current list depth.
 * @param[out]  col_count   Resultant column count.
 * @param[out]  col_name    Resultant column names.
 * @param[out]  col_value   Resultant column values.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_query_list(TMSTAT_EVAL eval, char *text, char *table_name,
                 unsigned depth, unsigned *col_count, char ***col_name,
                 void ***col_value)
{
    unsigned idx;
    char *name = NULL;
    void *value = NULL;
    
    idx = tmstat_parse_query(eval, text, table_name, &name, &value);
    if (idx == 0) {
        free(name);
        free(value);
        goto out;
    }
    while (text[idx] == ' ') idx++;
    if (text[idx] == ',') {
        idx++;
        while (text[idx] == ' ') idx++;
        idx += tmstat_parse_query_list(eval, &text[idx], table_name, depth + 1,
            col_count, col_name, col_value);
    } else {
        *col_name = calloc(sizeof(char *), depth + 1);
        *col_value = calloc(sizeof(char *), depth + 1);
        *col_count = depth + 1;
    }
    (*col_name)[depth] = name;
    (*col_value)[depth] = value;
out:
    return idx;
}

/**
 * Parse sum:
 *
 *  sum         ::= factor '+' sum
 *              |   factor '-' sum
 *              |   factor
 *              ;
 *
 * @param[in]   text        Text to parse.
 * @param[out]  val         Resultant value.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_sum(TMSTAT_EVAL eval, char *text, long long *val)
{
    unsigned idx, idx2;
    long long v1, v2;
    char op;

    idx = tmstat_parse_factor(eval, text, &v1);
    if (idx == 0) {
        return 0;
    }
    while ((text[idx] != '\0') && (text[idx] <= ' ')) idx++; 
    if ((text[idx] != '+') && (text[idx] != '-')) {
        *val = v1;
        return idx;
    }
    op = text[idx++];
    while ((text[idx] != '\0') && (text[idx] <= ' ')) idx++; 
    idx2 = tmstat_parse_sum(eval, &text[idx], &v2);
    if (idx2 == 0) {
        return 0;
    }
    free(eval->fieldtxt);
    eval->fieldtxt = NULL;
    switch (op) {
    case '+':   *val = v1 + v2;     break;
    case '-':   *val = v1 - v2;     break;
    }
    return idx + idx2;
}

/**
 * Parse table-name:
 *
 *      [a-z][a-z0-9_/]*
 *
 * @param[in]   text        Text to parse.
 * @param[out]  val         Resultant value; caller must free.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_table_name(TMSTAT_EVAL eval, char *text, char **val)
{
    unsigned i = 0;

    if (islower(text[0])) {
        while (islower(text[i]) || isdigit(text[i]) ||
               (text[i] == '_') || (text[i] == '/')) i++;
    }
    if (i > 0) {
        *val = malloc(i + 1);
        memcpy(*val, text, i);
        (*val)[i] = '\0';
    } else {
        tmstat_eval_err(eval, text, "invalid table name");
        *val = NULL;
    }
    return i;
}

/**
 * Parse test:
 *
 *  test        ::= truth '||' test
 *              |   truth '&&' test
 *              |   truth '<' test
 *              |   truth '>' test
 *              |   truth '<=' test
 *              |   truth '>=' test
 *              |   truth '!=' test
 *              |   truth '==' test
 *              |   truth
 *              ;
 *
 * @param[in]   text        Text to parse.
 * @param[out]  val         Resultant value.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_test(TMSTAT_EVAL eval, char *text, long long *val)
{
    unsigned idx, idx2;
    long long v1, v2;
    enum { OP_NONE, OP_ERR, OP_EQ, OP_NEQ, OP_GTE, OP_LTE, OP_GT, OP_LT } op;

    idx = tmstat_parse_truth(eval, text, &v1);
    if (idx == 0) {
        return 0;
    }
    while ((text[idx] != '\0') && (text[idx] <= ' ')) idx++; 
    op = OP_ERR;
    switch (text[idx]) {
    case '=':   op = (text[++idx] == '=') ? (idx++,OP_EQ) : OP_ERR;     break;
    case '!':   op = (text[++idx] == '=') ? (idx++,OP_NEQ) : OP_ERR;    break;
    case '<':   op = (text[++idx] == '=') ? (idx++,OP_LTE) : OP_LT;     break;
    case '>':   op = (text[++idx] == '=') ? (idx++,OP_GTE) : OP_GT;     break;
    default:    op = OP_NONE;                                           break;
    }
    if (op == OP_ERR) {
        return idx;
    }
    if (op != OP_NONE) {
        while ((text[idx] != '\0') && (text[idx] <= ' ')) idx++; 
        idx2 = tmstat_parse_test(eval, &text[idx], &v2);
        if (idx2 == 0) {
            return 0;
        }
    } else {
        idx2 = 0;
    }
    switch (op) {
    case OP_EQ:     *val = (v1 == v2);      break;
    case OP_NEQ:    *val = (v1 != v2);      break;
    case OP_GTE:    *val = (v1 >= v2);      break;
    case OP_LTE:    *val = (v1 <= v2);      break;
    case OP_GT:     *val = (v1 > v2);       break;
    case OP_LT:     *val = (v1 < v2);       break;
    default:        *val = v1;              break;
    }
    return idx + idx2;
}

/**
 * Parse truth:
 *
 *  truth       ::= sum '||' truth
 *              |   sum '&&' truth
 *              |   sum
 *              ;
 *
 * @param[in]   text        Text to parse.
 * @param[out]  val         Resultant value.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_truth(TMSTAT_EVAL eval, char *text, long long *val)
{
    unsigned idx, idx2;
    long long v1, v2;
    char op;

    idx = tmstat_parse_sum(eval, text, &v1);
    if (idx == 0) {
        return 0;
    }
    while ((text[idx] != '\0') && (text[idx] <= ' ')) idx++; 
    if ((text[idx] != '&') && (text[idx] != '|') &&
        (text[idx + 1] != '&') && (text[idx + 1] != '|')) {
        *val = v1;
        return idx;
    }
    op = text[idx];
    idx += 2;
    while ((text[idx] != '\0') && (text[idx] <= ' ')) idx++; 
    idx2 = tmstat_parse_truth(eval, &text[idx], &v2);
    if (idx2 == 0) {
        return 0;
    }   
    free(eval->fieldtxt);
    eval->fieldtxt = NULL;
    switch (op) {
    case '&':   *val = v1 && v2;     break;
    case '|':   *val = v1 || v2;     break;
    }
    return idx + idx2;
}

/**
 * Parse value:
 *
 *  value       ::= '(' test ')'
 *              |   function
 *              |   integer
 *              |   field
 *              ;
 *
 * @param[in]   text        Text to parse.
 * @param[out]  val         Resultant value.
 * @return characters consumed on success, 0 on failure.
 */
static unsigned
tmstat_parse_value(TMSTAT_EVAL eval, char *text, long long *val)
{
    unsigned i;

    if (text[0] == '(') {
        i = tmstat_parse_test(eval, &text[1], val) + 1;
        return (text[i] == ')') ? i + 1 : 0;
    }
    if (isupper(text[0])) {
        return tmstat_parse_function(eval, text, val);
    }
    if (isdigit(text[0]) || (text[0] == '-')) {
        return tmstat_parse_integer(eval, text, val);
    }
    return tmstat_parse_field(eval, text, val);
}
