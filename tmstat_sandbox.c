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
 * Sandbox stub functions.
 *
 */
#include <errno.h>
#include <stddef.h>
#include "tmstat.h"

/*
 * This file contains all of default logic provided to a sandbox client
 * by this module when no interface provier overrides these functions.
 * Since this interface is not useful within the sandbox, these functions
 * are all stubbed out.
 */

int
tmstat_create(TMSTAT *stat, char *name)
{
    errno = ENOSYS;
    return -1;
}

int
tmstat_publish(TMSTAT stat, char *directory)
{
    errno = ENOSYS;
    return -1;
}

void
tmstat_destroy(TMSTAT stat)
{
    errno = ENOSYS;
}

int
tmstat_unlink(TMSTAT stat)
{
    errno = ENOSYS;
    return -1;
}

void
tmstat_dealloc(TMSTAT stat)
{
    errno = ENOSYS;
}

int
tmstat_union(TMSTAT *stat, TMSTAT *children, unsigned count)
{
    errno = ENOSYS;
    return -1;
}

int
tmstat_subscribe(TMSTAT *stat, char *directory)
{
    errno = ENOSYS;
    return -1;
}

int
tmstat_read(TMSTAT *stat, char *path)
{
    errno = ENOSYS;
    return -1;
}

void
tmstat_refresh(TMSTAT stat, int force)
{
    errno = ENOSYS;
}

int
tmstat_table_register(TMSTAT stat, TMTABLE *table, char *name,
        struct TMCOL *cols, unsigned count, unsigned rowsz)
{
    errno = ENOSYS;
    return -1;
}

void
tmstat_table_info(TMSTAT stat, char *table_name,
        struct TMCOL **cols, unsigned *col_count)
{
    errno = ENOSYS;
}

int
tmstat_is_table_sorted(TMSTAT stat, char *table_name)
{
    errno = ENOSYS;
    return 0;
}

void
tmstat_table_row_size(TMSTAT stat, char *table_name, unsigned *rowsz)
{
    errno = ENOSYS;
}

char *
tmstat_table_name(TMTABLE table)
{
    errno = ENOSYS;
    return NULL;
}

int
tmstat_row_create(TMSTAT stat, TMTABLE table, TMROW *row)
{
    errno = ENOSYS;
    return -1;
}

TMROW
tmstat_row_ref(TMROW row)
{
    errno = ENOSYS;
    return NULL;
}

TMROW
tmstat_row_drop(TMROW row)
{
    errno = ENOSYS;
    return NULL;
}

void
tmstat_row_description(TMROW row, TMCOL *col, unsigned *col_count)
{
    errno = ENOSYS;
}

int
tmstat_row_field(TMROW row, char *name, void *p)
{
    errno = ENOSYS;
    return -1;
}

signed long long
tmstat_row_field_signed(TMROW row, char *name)
{
    errno = ENOSYS;
    return 0ll;
}

unsigned long long
tmstat_row_field_unsigned(TMROW row, char *name)
{
    errno = ENOSYS;
    return 0ull;
}

void
tmstat_row_info(TMROW row, struct TMCOL **cols, unsigned *col_count)
{
    errno = ENOSYS;
}

const char *
tmstat_row_table(TMROW row)
{
    errno = ENOSYS;
    return NULL;
}

int
tmstat_query(TMSTAT stat, char *table_name,
        unsigned col_count, char **col_names, void **col_values,
        TMROW **row_handles, unsigned *match_count)
{
    errno = ENOSYS;
    return -1;
}

int
tmstat_query_rollup(TMSTAT stat, char *table_name,
        unsigned col_count, char **col_names, void **col_values,
        TMROW *row_handle)
{
    errno = ENOSYS;
    return -1;
}

int
tmstat_merge(TMSTAT stat, char *path, enum tmstat_merge merge)
{
    errno = ENOSYS;
    return -1;
}

int
tmstat_eval_create(TMSTAT_EVAL *eval)
{
    errno = ENOSYS;
    return -1;
}

void
tmstat_eval_destroy(TMSTAT_EVAL eval)
{
    errno = ENOSYS;
}

int
tmstat_eval(TMSTAT_EVAL eval, TMSTAT stat, const char *expr,
    char **result, char **errstr, unsigned *erridx)
{
    errno = ENOSYS;
    return -1;
}

int
tmstat_eval_signed(TMSTAT_EVAL eval, TMSTAT stat, const char *expr,
    signed long long *result, char **errstr, unsigned *erridx)
{
    errno = ENOSYS;
    return -1;
}
