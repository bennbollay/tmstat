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
 * Statistics publishing and subscription facilities.
 *
 */
#define _GNU_SOURCE
#include <sys/mman.h>
#include <sys/procfs.h>
#include <sys/queue.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/user.h>
#include <assert.h>
#include <ctype.h>
#include <dirent.h>
#include <elf.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <libgen.h>

#include "tmstat.h"

#define HUGE_PAGE_SIZE (2 * 1024 * 1024)

#define ROUND_UP(n, m)  (((n) + ((m) - 1)) & -(m))

#define ROW_ALIGN 8

#define TMSTAT_MIN(a, b)               \
    ({ typeof(a) _a = (a);             \
        typeof(b) _b = (b);            \
        _a < _b ? _a : _b; })
#define TMSTAT_MAX(a, b)               \
    ({ typeof(a) _a = (a);             \
        typeof(b) _b = (b);            \
        _a > _b ? _a : _b; })

#define TM_SLAB_MAGIC   (*(uint32_t *)"TMSS")   //!< Slab magic.
#define TM_SZ_LINE      64                      //!< Slab line size.
#define TM_ID_TABLE     0                       //!< Table descriptor id.
#define TM_ID_COLUMN    1                       //!< Column descriptor id.
#define TM_ID_INODE     2                       //!< Inode id.
#define TM_ID_LABEL     3                       //!< Inode id.
#define TM_ID_USER      4                       //!< First user id.
#define TM_SZ_TMSS      256                     //!< Initial tmss table size.
#define TM_MAX_NAME     TMSTAT_MAX_NAME         //!< Max col/tbl name length.

/**
 * Forward declaration.
 */
static void _tmstat_dealloc(TMSTAT stat);

/**
 * Column descriptor.
 */
struct tmstat_column {
    char                name[TM_MAX_NAME+1];//!< Column name.
    uint16_t            tableid;            //!< Associated table.
    uint16_t            offset;             //!< Column offset.
    uint16_t            size;               //!< Column size.
    uint8_t             type;               //!< Data type.
    uint8_t             rule;               //!< Merge rule.
} __attribute__((packed));

/**
 * Index container--a dynamically-sized array of pointers.
 * Order is not preserved across item removals.
 */
struct tmidx {
    unsigned        n;                  //!< Total entries.
    unsigned        c;                  //!< Used entries.
    void          **a;                  //!< Entry array.
};

/**
 * Index node.
 *
 * These are always the same size as a line.
 *
 * TM_SZ_INODE is the number of entries in an inode.
 */
#define TM_SZ_INODE         (((TM_SZ_LINE) / sizeof(uint32_t)) - 1)
struct tmstat_inode {
    uint32_t        child[TM_SZ_INODE]; //!< Child list.
    uint32_t        next;               //!< Next node.
};

/**
 * Inode child components.
 *
 * Inode numbers are 32 bits.  The upper 24 bits contain a 0-based
 * slab offset into the segment.  The lower 8 bits contain a 0-based
 * row index.  Note that the row index skips first line in the slab
 * used to hold the tmstat_slab structure and also that it cannot be
 * used to determine the starting address of the row without the
 * tmstat_slab structure's lines_per_row member.
 *
 * The value TM_INODE_LEAF is used in the low-order bits of inode
 * numbers to mark objects in the index structure that have no
 * children.
 */
#define TM_INODE_LEAF       0xff                //!< Leaf marker.
#define TM_INODE_ROW(n)     ((n) & 0xff)        //!< Inode row index.
#define TM_INODE_SLAB(n)    ((n) >> 8)          //!< Inode slab index.
#define TM_INODE(s, i)      (((s) << 8) | i)    //!< Calculate inode value.

/**
 * Label descriptor.
 */
struct tmstat_label {
    char                tree[8];                //!< ASCII art.
    char                name[TM_MAX_NAME+1];    //!< Label name.
    char                ctime[26];              //!< Creation time string.
    time_t              time;                   //!< Creation time in secs.
} __attribute__((packed));

/**
 * Slab line.
 */
struct tmstat_line {
    uint8_t             data[TM_SZ_LINE];   //!< Line data.
};

/**
 * Row slab.
 *
 * The bitmap member tracks row allocation, not line allocation.
 */
struct tmstat_slab {
    uint32_t            magic;              //!< TM_SLAB_MAGIC.
    uint16_t            tableid;            //!< Associated table Id.
    uint16_t            lines_per_row;      //!< Lines per row.
    uint64_t            bitmap;             //!< Allocation bitmap.
    uint32_t            inode;              //!< Slab inode address.
    uint32_t            parent;             //!< Parent inode (if any).
    uint32_t            statid;             //!< Owning stat context.
    uint8_t             pad[TM_SZ_LINE-28]; //!< Pad to TM_SZ_LINE.
    struct tmstat_line  line[];             //!< Slab lines (containing rows).
} __attribute__((packed));

/**
 * Table descriptor.  These objects exist in the .table table and
 * must not be more than one line long.
 */
struct tmstat_table {
    char                name[TM_MAX_NAME+1];//!< Table name.
    uint32_t            inode;              //!< Root inode.
    uint32_t            rows;               //!< Total rows (informational).
    uint16_t            rowsz;              //!< Row size (bytes).
    uint16_t            cols;               //!< Total columns (informational).
    uint8_t             is_sorted;          //!< If sorted use fast query
    uint16_t            tableid;            //!< Id of this table.
} __attribute__((packed));

/**
 * Modes for allocating pages from the system for slabs.
 */
enum alloc_policy {
    PREALLOCATE,        //<! Pre-allocate pages to reduce number of mmap calls.
    AS_NEEDED,          //<! Allocate pages only when they're needed.
};

/**
 * Methods by which a stat handle can be created.
 */
enum origin {
    INVALID_ORIGIN,
    CREATE,
    SUBSCRIBE,
    SUBSCRIBE_FILE,
    READ,
    UNION,
};

/**
 * A record of an allocation of slab pages.
 */
struct alloc {
    char               *base;               //<! Beginning of region.
    char               *limit;              //<! First address after region.
    struct alloc       *prev;               //<! Previous allocation.
};

/**
 * Statistics segment handle.
 */
struct TMSTAT {
    char                name[32];           //!< Segment name.
    char                directory[32];      //!< Segment directory name.
    uint32_t            statid;             //!< Process-unique ID
    signed              fd;                 //!< File descriptor.
    enum alloc_policy   alloc_policy;       //!< How to map pages for slabs.
    enum origin         origin;             //!< How this segment was created.
    size_t              slab_size;          //!< Slab size.
    struct tmidx        slab_idx;           //!< Slab index.
    struct tmidx        table_idx;          //!< Table index.
    struct tmidx        child_idx;          //!< Child segment index.
    char               *next_page;          //!< Next free page in curr. alloc.
    struct alloc       *allocs;             //!< Allocations for slabs.
    struct timespec     ctime;              //!< Ctime of dir when last read.
};

/**
 * Statistics table handle.
 */
struct TMTABLE {
    TMSTAT                  stat;           //!< Parent segment.
    uint16_t                tableid;        //!< Table Id.
    size_t                  rowsz;          //!< Row size (in bytes).
    uint32_t               *inode;          //!< Root inode pointer.
    struct tmstat_table    *td;             //!< Table descriptor.
    struct tmidx            avail_idx;      //!< Partially-allocated slab index.
    TMCOL                   col;            //!< All Column metadata.
    unsigned                col_count;      //!< Total number of columns.
    TMCOL                   key_col;        //!< Key column metadata (duped).
    unsigned                key_col_count;  //!< Number of Key columns.
    bool                    want_merge : 1; //!< Table needs row merge pass.
    LIST_HEAD(, TMROW)      row_list;       //!< Row handles.
};

/**
 * Row handle.
 */
struct TMROW {
    LIST_ENTRY(TMROW)       entry;          //!< Row list linkage.
    signed                  ref_count;      //!< Total references.
    uint8_t                *data;           //!< Start of row data.
    uint32_t                inode_addr;     //!< Row address.
    TMTABLE                 table;          //!< Parent table.
    bool                    own_row : 1;    //!< This handle owns this row.
};

/**
 * Calculate the number of entries in a static array.
 */
#define array_size(a)   (sizeof(a) / sizeof((a)[0]))

/**
 * Calculate the number of objects a slab can store.
 *
 * @param[in]   stat    The slab's owning stat.
 * @param[in]   stat    The relevant slab.
 * @return The number of objects the slab can store.
 */
#define slab_max(stat, slab)                                                \
    ((((stat)->slab_size / TM_SZ_LINE) - 1) / (slab)->lines_per_row)

/**
 * Iterate over entries in an index.
 *
 * @param[in]   t       The tmidx.
 * @param[in]   p       Iterator variable.
 */
#define TMIDX_FOREACH(t, p)                                                 \
    for (unsigned tmidxforeachi = 0;                                        \
         (tmidxforeachi < (t)->c) && (p = (t)->a[tmidxforeachi]);           \
         tmidxforeachi++)

/**
 * Retrieve a specific row from an index.
 *
 * @param[in]   t       The tmidx.
 * @param[in]   i       The index of the desired row.
 * @return The value at index i or NULL if there is no such entry.
 */
#define TMIDX_ROW(t, i)                                                     \
    ((i < (t)->c) ? (t)->a[i] : NULL)

/**
 * Iterate over allocated rows in a slab.
 *
 * @param[in]   st      The stat owning the slabs.
 * @param[in]   sl      The slab over whose lines to iterate.
 * @param[in]   r       Row number iterator, an integer.
 * @param[in]   p       A pointer, assigned the address of each row.
 */
#define TMSTAT_SLAB_FOREACH(st, sl, r, p)                                   \
    for (r = 0; (sl)->bitmap >> r; r++)                                     \
        if (((sl)->bitmap & (1ULL << r)) &&                                 \
            ((p) = (void *)&(sl)->line[r * (sl)->lines_per_row]) &&         \
            ((char *)(p) < ((char *)sl) + st->slab_size))

/**
 * Calculate the number of lines in a slab, which depends upon the
 * segment.
 *
 * @param[in]   stat    The segment in whose slabs you are interested.
 * @return The number of lines in a slab in this segment.
 */
#define TM_LINES_PER_SLAB(stat) ((stat)->slab_size / TM_SZ_LINE)

/**
 * Return a pointer to the first row in a slab or NULL if no rows are
 * allocated.
 *
 * @param[in]   stat    The stat owning the slab.
 * @param[in]   s       The slab from which to return a row.
 * @param[out]  rowno   If non-null, assigned the index of the first row.
 * @return The address of the first allocated row.
 */
static void *
tmstat_slab_first(TMSTAT stat, struct tmstat_slab *s, uint32_t *rowno)
{
    uint32_t r;
    void *p = NULL;
    const uint32_t limit = TM_LINES_PER_SLAB(stat) / s->lines_per_row;

    for (r = 0; (s->bitmap >> r) && (r < limit); ++r)  {
        if (s->bitmap & (1ULL << r)) {
            p = (void *)&s->line[r * (s)->lines_per_row];
            break;
        }
    }
    *rowno = r;
    return p;
}

#define TMSTAT_SEGMENT_DAMAGED(stat) \
    tmstat_segment_damaged(__func__, __LINE__, stat)

static void
tmstat_segment_damaged(const char *func, unsigned line, TMSTAT stat)
{
    warnx("%s(%d): tmstat segment %s/%s appears to be damaged",
          func, line, stat->directory, stat->name);
    errno = EINVAL;
}

static bool
tmstat_is_internal_name(const char *name)
{
    for (unsigned i = 0; (name[i] != '\0') && (i < TM_MAX_NAME); ++i) {
        if (name[i] == '/') {
            return true;
        }
    }
    return false;
}

static char *
tmstat_name_copy(const char *name)
{
    char *copy;

    copy = malloc(TM_MAX_NAME + 1);
    if (copy != NULL) {
        memset(copy, 0, TM_MAX_NAME + 1);
        strncpy(copy, name, TM_MAX_NAME);
    }
    return copy;
}

/**
 * Return a pointer to the last row in a slab or NULL if no rows are
 * allocated.
 *
 * @param[in]   stat    The stat owning the slab.
 * @param[in]   s       The slab from which to return a row.
 * @param[out]  rowno   If non-null, assigned the index of the last row.
 * @return The address of the last allocated row.
 */
static void *
tmstat_slab_last(TMSTAT stat, struct tmstat_slab *s, uint32_t *rowno)
{
    uint32_t r;
    void *p = NULL;
    const uint32_t limit = TM_LINES_PER_SLAB(stat) / s->lines_per_row;

    for (r = limit - 1; r != (uint32_t)-1; --r)  {
        if (s->bitmap & (1ULL << r)) {
            p = (void *)&s->line[r * (s)->lines_per_row];
            break;
        }
    }
    *rowno = r;
    return p;
}

/**
 * Table-descriptor (struct tmstat_table) descriptor.
 *
 * Each row in this table describes a table in the segment.
 */
static THREAD struct TMCOL tm_cols_table[] = {
    TMCOL_TEXT(struct tmstat_table, name),
    TMCOL_UINT(struct tmstat_table, rows,       .rule = TMSTAT_R_SUM),
    TMCOL_UINT(struct tmstat_table, rowsz,      .rule = TMSTAT_R_MAX),
    TMCOL_UINT(struct tmstat_table, cols,       .rule = TMSTAT_R_MAX),
    TMCOL_UINT(struct tmstat_table, is_sorted,  .rule = TMSTAT_R_MIN),
};

/**
 * Label (struct tmstat_label) descriptor.
 *
 * Each row in this table describes a source for the segment.
 */
static THREAD struct TMCOL tm_cols_label[] = {
    TMCOL_TEXT(struct tmstat_label, tree,       .rule = TMSTAT_R_MIN),
    TMCOL_TEXT(struct tmstat_label, name),
    TMCOL_TEXT(struct tmstat_label, ctime),
    TMCOL_INT(struct tmstat_label, time,        .rule = TMSTAT_R_MAX),
};

/**
 * Segment base path.
 */
THREAD char *tmstat_path = TMSTAT_PATH;

/**
 * Segment id
 */
static GLOBALSET uint32_t tmstat_nextid = 1;

/**
 * Header of the base.
 */
#define TMSTAT_BASE_HEADER "+- "

/**
 * Header of the segment.
 */
#define TMSTAT_SEGMENT_HEADER "|  +- "

/*
 * The red-black tree implementation herein was adapted from the
 * (public domain) code found at
 * http://eternallyconfuzzled.com/tuts/datastructures/jsw_tut_rbtree.aspx
 * There is a delete routine to be found there; it is not present here
 * because we never delete from our trees.  Note that this
 * implementation is incapable of supporting iteration but is snappy
 * due to the absence of parent pointers.
 */

/**
 * Red-black search tree node.
 */
struct tmrbt_node {
    int red;                        /* Node color.  0 = black, 1 = red. */
    TMROW key;                      /* The node's key. */
    struct tmrbt_node *link[2];     /* Children.  0 = left, 1 = right. */
};
typedef struct tmrbt_node *tmrbt_node;

/**
 * A block of nodes allocated by a tmrbt.  This is like a slab in
 * allocator parlance but we don't want to confuse our terminology.
 */
#define TMRBT_SZ_POOL 62
struct tmrbt_pool {
    struct tmrbt_node node[TMRBT_SZ_POOL]; /* The goodies. */
    unsigned c;                     /* Index of next free node. */
    struct tmrbt_pool *next;        /* The next pool or NULL. */
};
typedef struct tmrbt_pool *tmrbt_pool;

/**
 * Red-black search tree.
 */
struct tmrbt {
    tmrbt_node root;                /* Tree's root node. */
    tmrbt_pool pool;                /* Our memory allocations. */
};
typedef struct tmrbt *tmrbt;

TMTABLE tmstat_table(TMSTAT, char *);

/**
 * Resize index.
 *
 * @param[in]   idx     Index to resize.
 * @param[in]   n       Entries to preallocate.
 */
static signed
tmidx_resize(struct tmidx *idx, unsigned n)
{
    void          **a = (void **)realloc(idx->a, n * sizeof(void *));

    assert(n >= idx->c);
    if (a == NULL) {
        /* Allocation failure; realloc sets errno. */
        return -1;
    }
    memset(&a[idx->n], 0, (n - idx->n) * sizeof(void *));
    idx->a = a;
    idx->n = n;
    return 0;
}

/**
 * Initialize index.
 *
 * @param[in]   idx     Index to initialize.
 */
static inline void
tmidx_init(struct tmidx *idx)
{
    memset(idx, 0, sizeof(struct tmidx));
}

/**
 * Free index.
 *
 * @param[in]   idx     Index to free.
 */
static inline void
tmidx_free(struct tmidx *idx)
{
    free(idx->a);
}

/**
 * Add entry to idx.
 *
 * @param[in]   idx     Associated index.
 * @param[in]   p       Entry to add.
 * @return index on success, -1 on error.
 */
static signed
tmidx_add(struct tmidx *idx, void *p)
{
    assert(p != NULL);
    if (idx->c == idx->n) {
        /* Extend idx. */
        signed          ret = tmidx_resize(idx, (idx->n > 0) ? idx->n * 2 : 1);

        if (ret != 0) {
            /* Allocation failure; tmidx_resize sets errno. */
            return -1;
        }
    }
    /* Add entry. */
    idx->a[idx->c] = p;
    return idx->c++;
}

/**
 * Remove entry from index.
 *
 * @param[in]   idx     Associated index.
 * @param[in]   i       Index of entry to remove.
 */
static inline void
tmidx_remove(struct tmidx *idx, unsigned i)
{
    assert(i < idx->c);
    idx->c--;
    idx->a[i] = idx->a[idx->c];
    idx->a[idx->c] = NULL;
}

/**
 * Fetch entry from index.
 *
 * @param[in]   idx     Associated index.
 * @param[in]   i       Index of entry to fetch.
 * @return pointer to entry.
 */
static inline void *
tmidx_entry(struct tmidx *idx, unsigned i)
{
    return (i < idx->c) ? idx->a[i] : NULL;
}

/**
 * Return total used entries.
 *
 * @param[in]   idx     Associated index.
 * @return count of used entries.
 */
static inline unsigned
tmidx_count(struct tmidx *idx)
{
    return idx->c;
}

static int64_t tmstat_row_cmp(TMROW r1, TMROW r2);
int tmstat_pseudo_row_create(TMTABLE table, TMROW *row);
int tmstat_alloc_weak_ref_row(TMTABLE table, struct tmidx *rows, uint8_t *row,
                              struct tmstat_slab *slab, unsigned line);

/**
 * Allocate a red-black tree node.  We use pools of nodes not to
 * reduce the number of calls to malloc but to make freeing the tree
 * easier; this way, we don't have to walk the tree to free it.  A
 * non-recursive free would be very challenging without an
 * auxiliary data structure because the nodes do not contain parent
 * pointers.
 */
static tmrbt_node
tmrbt_node_alloc(tmrbt tree, TMROW key)
{
    tmrbt_node node;
    if (tree->pool->c == TMRBT_SZ_POOL) {
        tmrbt_pool next = tree->pool;
        tree->pool = (tmrbt_pool)malloc(sizeof(*tree->pool));
        if (tree->pool == NULL) {
            tree->pool = next;
            return NULL;
        }
        tree->pool->next = next;
        tree->pool->c = 0;
    }
    node = &tree->pool->node[tree->pool->c++];
    node->red = 1;
    node->key = key;
    node->link[0] = NULL;
    node->link[1] = NULL;
    return node;
}

static inline int
tmrbt_is_red(tmrbt_node node)
{
    return (node != NULL) && (node->red == 1);
}

static tmrbt_node
tmrbt_rot_single(tmrbt_node root, int dir)
{
    tmrbt_node save = root->link[!dir];

    root->link[!dir] = save->link[dir];
    save->link[dir] = root;

    root->red = 1;
    save->red = 0;

    return save;
}

static tmrbt_node
tmrbt_rot_double(tmrbt_node root, int dir)
{
    root->link[!dir] = tmrbt_rot_single(root->link[!dir], !dir);
    return tmrbt_rot_single(root, dir);
}

static tmrbt
tmrbt_alloc()
{
    tmrbt tree;
    tree = (tmrbt)malloc(sizeof(*tree));
    if (tree == NULL) {
        return NULL;
    }
    tree->root = NULL;
    tree->pool = (tmrbt_pool)malloc(sizeof(*tree->pool));
    if (tree->pool == NULL) {
        free(tree);
        return NULL;
    }
    tree->pool->c = 0;
    tree->pool->next = NULL;
    return tree;
}

static void
tmrbt_free(tmrbt tree)
{
    if (tree == NULL) return;
    tmrbt_pool pool = tree->pool;
    while (pool) {
        tmrbt_pool next = pool->next;
        free(pool);
        pool = next;
    }
    free(tree);
}

static TMROW
tmrbt_find(tmrbt tree, TMROW key)
{
    tmrbt_node node = tree->root;
    while (node) {
        int64_t cmp = tmstat_row_cmp(node->key, key);
        if (cmp == 0) {
            return node->key;
        }
        node = node->link[cmp < 0];
    }
    return NULL;
}

/**
 * Red-black search tree insertion.
 *
 * Warning!  On failure, the tree will no longer maintain the
 * red-black properties.
 *
 * @param[in]   tree        Tree to modify.
 * @param[in]   key         Element to insert.
 * @return 0 on success, -1 on failure.
 */
static int
tmrbt_insert(tmrbt tree, TMROW key)
{
    int ret = 0;

    if (tree->root == NULL) {
        /* Empty tree case. */
        tree->root = tmrbt_node_alloc(tree, key);
        if (tree->root == NULL) {
            /* Memory exhausted. */
            ret = -1;
            goto out;
        }
    } else {
        struct tmrbt_node head = { 0 };     /* False tree root. */
        tmrbt_node g, t;                    /* Grandparent & parent. */
        tmrbt_node p, q;                    /* Iterator & parent. */
        int dir = 0, last;
        int64_t cmp;

        /* Set up helpers. */
        t = &head;
        g = p = NULL;
        q = t->link[1] = tree->root;

        /* Search down the tree. */
        for (;;) {
            if (q == NULL) {
                /* Insert new node at the bottom. */
                q = tmrbt_node_alloc(tree, key);
                if (q == NULL) {
                    /* Memory exhausted. */
                    ret = -1;
                    goto out;
                }
                p->link[dir] = q;
            }
            else if (tmrbt_is_red(q->link[0]) && tmrbt_is_red(q->link[1])) {
                /* Color flip. */
                q->red = 1;
                q->link[0]->red = 0;
                q->link[1]->red = 0;
            }

            /* Fix red violation. */
            if (tmrbt_is_red(q) && tmrbt_is_red(p)) {
                int dir2 = t->link[1] == g;

                if (q == p->link[last])
                    t->link[dir2] = tmrbt_rot_single(g, !last);
                else
                    t->link[dir2] = tmrbt_rot_double(g, !last);
            }

            /* Stop if found. */
            cmp = tmstat_row_cmp(q->key, key);
            if (cmp == 0) {
                break;
            }

            last = dir;
            dir = cmp < 0;

            /* Update helpers. */
            if (g != NULL)
                t = g;
            g = p, p = q;
            q = q->link[dir];
        }

        /* Update root. */
        tree->root = head.link[1];
    }

    /* Make root black. */
    tree->root->red = 0;

out:
    return ret;
}

/**
 * Map a portion of a file.
 *
 * @param[in]   stat        Segment whose file we're mapping.
 * @param[in]   size        Length of mapping (in slabs).
 * @param[in]   perm        Permissions, passed to mmap.
 * @param[in]   offset      Offset in file of mapping (in slabs).
 * @return 0 on success, -1 on failure.
 */
#define tmstat_mmap(a, b, c, d) (tmstat_mmap)((a), (b), (c), (d), __func__)
static int (
tmstat_mmap)(TMSTAT stat, size_t size, int perm, size_t offset,
              const char *func)
{
    char *p;
    struct alloc *a;

    /*
     * If we're mmaping more of the file, either we should have
     * already made slabs out of everything we had already mmaped or this
     * should be the first mmap.
     */
    assert((stat->next_page == NULL) ||
           (stat->next_page == stat->allocs->limit));

    a = calloc(1, sizeof(struct alloc));
    if (a == NULL) {
        warn("%s: calloc", func);
        return -1;
    }
    size = size * stat->slab_size;
    offset = offset * stat->slab_size;
    if (stat->fd != -1) {
        p = mmap(NULL, size, perm, MAP_SHARED, stat->fd, offset);
        if (p == MAP_FAILED) {
            warn("%s: mmap", func);
            p = NULL;
        }
    } else {
        p = calloc(1, size);
        if (p == NULL) {
            warn("%s: calloc", func);
        }
    }
    if (p != NULL) {
        a->prev = stat->allocs;
        a->base = p;
        a->limit = p + size;
        stat->next_page = p;
        stat->allocs = a;
        return 0;
    } else {
        free(a);
        return -1;
    }
}

/**
 * Unmap mappings for this file.
 *
 * @param[in]   stat        Segment whose mappings we're releasing.
 */
static void
tmstat_munmap(TMSTAT stat)
{
    struct alloc *a, *prev;
    int ret;

    for (a = stat->allocs; a != NULL; a = prev) {
        if (stat->fd != -1) {
            ret = munmap(a->base, a->limit - a->base);
            if (ret != 0) {
                warn("%s: munmap", __func__);
            }
        } else {
            free(a->base);
        }
        prev = a->prev;
        free(a);
    }
    stat->allocs = NULL;
    stat->next_page = NULL;
}

/**
 * Obtain slab for inode address.
 *
 * @param[in]   stat        Parent segment.
 * @param[in]   inode_addr  Inode address.
 * @return slab pointer or NULL upon error.
 */
static struct tmstat_slab *
tmstat_slab(TMSTAT stat, uint32_t inode_address)
{
    struct tmstat_slab     *slab;

    slab = (struct tmstat_slab *)
        tmidx_entry(&stat->slab_idx, TM_INODE_SLAB(inode_address));
    if ((slab == NULL) && stat->origin != CREATE) {
        struct stat     stat_buf;
        unsigned        slab_count, new_count;
        signed          ret;
        char           *p;

        /*
         * Publisher extended segment since we subscribed; extend our map.
         */
        ret = fstat(stat->fd, &stat_buf);
        if (ret != 0) {
            /* Filesystem failure; fstat sets errno. */
            goto out;
        }
        if ((stat_buf.st_size % stat->slab_size) != 0) {
            warnx("segment is not an even number of slabs: %u %% %u = %u",
                  stat_buf.st_size, stat->slab_size,
                  (stat_buf.st_size % stat->slab_size));
            goto out;
        }
        slab_count = stat_buf.st_size / stat->slab_size;
        if (slab_count <= tmidx_count(&stat->slab_idx)) {
            /* File was not extended.  This is a bogus request. */
            goto out;
        }
        new_count = slab_count - tmidx_count(&stat->slab_idx);
        /* Map new slabs. */
        ret = tmstat_mmap(stat, new_count, PROT_READ,
                          tmidx_count(&stat->slab_idx));
        if (ret == -1) {
            /* Mapping failure; tmstat_mmap sets errno. */
            goto out;
        }
        p = stat->next_page;
        /* Insert slab index entries. */
        for (unsigned i = 0; i < new_count; i++) {
            ret = tmidx_add(&stat->slab_idx, p);
            if (ret == -1) {
                /* Insertion failure; tmidx_add sets errno. */
                goto out;
            }
            p += stat->slab_size;
        }
        stat->next_page = p;
        /* Use the new index. */
        slab = (struct tmstat_slab *)
            tmidx_entry(&stat->slab_idx, TM_INODE_SLAB(inode_address));
    }
out:
    return slab;
}

/*
 * Obtain inode from address.
 */
static inline struct tmstat_inode *
tmstat_inode(TMSTAT stat, uint32_t addr)
{
    struct tmstat_slab     *slab;
    struct tmstat_inode    *inode = NULL;
    struct tmstat_inode    *limit;

    slab = tmstat_slab(stat, addr);
    if (slab != NULL) {
        limit = (struct tmstat_inode *)((char *)slab + stat->slab_size);
        inode = (struct tmstat_inode *)
            &slab->line[TM_INODE_ROW(addr) * slab->lines_per_row];
        if (inode >= limit) {
            inode = NULL;
        }
    }
    return inode;
}

/**
 * Allocate slab for table.
 *
 * @param[in]   tms         Parent segment.
 * @param[in]   tableid     Table Id.
 * @param[in]   rowsz       Row size (in bytes).
 * @param[out]  new_slab    New slab.
 * @return 0 on success, -1 on error.
 */
static int
tmstat_slab_alloc(TMSTAT stat, uint16_t tableid, uint16_t rowsz,
                  struct tmstat_slab **new_slab)
{
    struct tmstat_slab     *slab = NULL;
    unsigned                i = tmidx_count(&stat->slab_idx);
    signed                  ret;
    unsigned                c;

    if ((stat->next_page == NULL) || (stat->next_page == stat->allocs->limit)) {
        if ((stat->alloc_policy == AS_NEEDED) || (stat->allocs == NULL)) {
            if (stat->fd == -1) {
                c = 1;
            } else {
                c = HUGE_PAGE_SIZE / stat->slab_size;
            }
        } else {
            c = (stat->allocs->limit - stat->allocs->base) / stat->slab_size;
            c = 2 * c;
        }
        if (stat->fd != -1) {
            /* Extend. */
            ret = ftruncate(stat->fd, (i + c) * stat->slab_size);
            if (ret != 0) {
                warn("%s: ftruncate", __func__);
                return -1;
            }
        }
        /* Map. */
        ret = tmstat_mmap(stat, c, PROT_READ|PROT_WRITE, i);
        if (ret == -1) {
            /* Mapping failure; tmstat_mmap sets errno. */
            return -1;
        }
    }
    slab = (struct tmstat_slab *)stat->next_page;
    ret = tmidx_add(&stat->slab_idx, slab);
    if (ret == -1) {
        /* Allocation failure; tmidx_add sets errno. */
        return -1;
    }
    stat->next_page = ((char *)stat->next_page) + stat->slab_size;
    /* Construct (the newly-added bytes will be zeros). */
    slab->lines_per_row = (rowsz + (TM_SZ_LINE - 1)) / TM_SZ_LINE;
    slab->tableid = tableid;
    slab->magic = TM_SLAB_MAGIC;
    slab->statid = stat->statid;
    slab->inode = TM_INODE(i, TM_INODE_LEAF);
    /* Done. */
    *new_slab = slab;
    return 0;
}

/**
 * Produce a slab index for a table, one index entry per row, ordered
 * as they occur in the inode table.
 *
 * @param[in]   stat        Associated segment.
 * @param[in]   td          Table descriptor.
 * @param[out]  idx         Result index.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_slab_idx(TMSTAT stat, struct tmstat_table *td, struct tmidx *idx)
{
    struct tmstat_inode    *inode;
    signed                  ret;
    uint32_t                addr;

    addr = td->inode;
    if (TM_INODE_ROW(addr) == TM_INODE_LEAF) {
        /* This table only has one slab. */
        ret = tmidx_add(idx, tmstat_slab(stat, addr));
        ret = (ret != -1) ? 0 : -1;
        goto out;
    }
    /* Iterate over inode index. */
    while (addr != 0) {
        inode = tmstat_inode(stat, addr);
        if (inode == NULL) {
            /*
             * If someone did something silly, like copied
             * a text file to /var/tmstat/blade, the inode 
             * can be NULL
             */
            ret = -1;
            goto out;
        }
        for (unsigned i = 0; i < TM_SZ_INODE; i++) {
            addr = inode->child[i];
            if (addr != 0) {
                ret = tmidx_add(idx, tmstat_slab(stat, addr));
                if (ret == -1) {
                    /* Insertion failure; tmidx_add sets errno. */
                    goto out;
                }
            }
        }
        addr = inode->next;
    }
    ret = 0;
out:
    return ret;
}

/**
 * Allocate table row.
 *
 * @param[in]   stat        Assocated segment.
 * @param[in]   table       Associated table.
 * @param[out]  inode       Inode address.
 * @param[out]  row         Row pointer.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_row_alloc(TMSTAT stat, TMTABLE table, uint32_t *inode, void *row)
{
    signed                  ret = 0;
    struct tmstat_slab     *slab;
    unsigned                line, last;
    uint64_t                fullmap;
    bool                    existing_slab;

    /* Obtain slab. */
    slab = tmidx_entry(&table->avail_idx, 0);
    if (slab == NULL) {
        /* Allocate a new slab for the table. */
        ret = tmstat_slab_alloc(stat, table->tableid, table->rowsz, &slab);
        if (ret != 0) {
            /* Allocation failure; tmstat_slab_alloc sets errno. */
            goto out;
        }
        existing_slab = false;
    } else {
        existing_slab = true;
    }
    /* Obtain a row worth of lines. */
    last = slab_max(stat, slab);
    line = 0;
    while (((slab->bitmap & (1ULL << line)) != 0) && (line < last)) {
        line++;
    }
    if (line == last) {
        /*
         * Either the segment is under concurrent modification from
         * within this process or from somewhere else (or there is a
         * bug in this library).  In the former two cases, we might be
         * doing the caller a favor by aborting; however, in the
         * latter case, the caller might not be so happy about program
         * termination.  We compromise by printing a warning and
         * returning failure.
         */
        TMSTAT_SEGMENT_DAMAGED(stat);
        return -1;
    }
    /* Mark the lines as used. */
    slab->bitmap |= 1ULL << line;
    /* Adjust partially-filled slab index. */
    fullmap = (1ULL << last) - 1;
    if (!existing_slab && (slab->bitmap != fullmap)) {
        /* Insert newly-allocated slab. */
        ret = (tmidx_add(&table->avail_idx, slab) >= 0) ? 0 : -1;
    }
    if (existing_slab && (slab->bitmap == fullmap)) {
        /* Remove filled slab. */
        tmidx_remove(&table->avail_idx, 0);
    }
    if (ret == 0) {
        /* Return row inode address. */
        *inode = TM_INODE(TM_INODE_SLAB(slab->inode), line);
        /* Return row pointer. */
        *(void **)row = (void *)&slab->line[line * slab->lines_per_row];
    }
out:
    return ret;
}

static int
tmstat_row_alloc_n(TMSTAT stat, TMTABLE table, TMROW *row, unsigned n,
        struct tmidx *slabs)
{
    struct tmstat_slab     *slab;
    uint64_t                fullmap;
    unsigned                line, last;
    unsigned                i;
    signed                  ret = 0;

    last = (((stat->slab_size / TM_SZ_LINE) - 1) /
            ((table->rowsz + TM_SZ_LINE - 1) / TM_SZ_LINE));
    fullmap = (1ULL << last) - 1;
    slab = tmidx_entry(&table->avail_idx, 0);
    if ((slab != NULL) && (slab->parent == 0)) {
        ret = tmidx_add(slabs, slab) == -1 ? -1 : 0;
        if (ret != 0) {
            return ret;
        }
    }
    line = 0;
    for (i = 0; (slab != NULL) && (i < n); ++i) {
        while (((slab->bitmap & (1ULL << line)) != 0) && (line < last)) {
            line++;
        }
        if (line == last) {
            TMSTAT_SEGMENT_DAMAGED(stat);
            return -1;
        }
        slab->bitmap |= 1ULL << line;
        row[i]->inode_addr = TM_INODE(TM_INODE_SLAB(slab->inode), line);
        row[i]->data = (uint8_t *)&slab->line[line * slab->lines_per_row];
        if (slab->bitmap == fullmap) {
            tmidx_remove(&table->avail_idx, 0);
            slab = tmidx_entry(&table->avail_idx, 0);
            if ((slab != NULL) && (slab->parent == 0)) {
                ret = tmidx_add(slabs, slab) == -1 ? -1 : 0;
                if (ret != 0) {
                    return ret;
                }
            }
            line = 0;
        }
    }
    for (slab = NULL; i < n; ++i, ++line) {
        if (slab == NULL) {
            ret = tmstat_slab_alloc(stat, table->tableid, table->rowsz, &slab);
            if (ret != 0) {
                goto out;
            }
            ret = tmidx_add(slabs, slab) == -1 ? -1 : 0;
            if (ret != 0) {
                return ret;
            }
            line = 0;
        }
        slab->bitmap |= 1ULL << line;
        row[i]->inode_addr = TM_INODE(TM_INODE_SLAB(slab->inode), line);
        row[i]->data = (uint8_t *)&slab->line[line * slab->lines_per_row];
        if (slab->bitmap == fullmap) {
            slab = NULL;
        }
    }
    if (slab != NULL) {
        ret = (tmidx_add(&table->avail_idx, slab) >= 0) ? 0 : -1;
    }
 out:
    return ret;
}

/**
 * Free row, meaning that we zero it and clear its spot in the owning
 * slab's row-in-use bitmap.
 *
 * Note that we don't free slabs (this could cause slabs to reused for
 * different tables in the future, removing type stability for segment
 * consumers).
 *
 * @param[in]   stat        Associated segment.
 * @param[in]   table       Associated table.
 * @param[in]   inode_addr  Row inode address to remove.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_row_free(TMSTAT stat, TMTABLE table, uint32_t inode_addr)
{
    struct tmstat_slab     *slab = tmstat_slab(stat, inode_addr);
    unsigned                rowno = TM_INODE_ROW(inode_addr);
    signed                  ret = 0;

    assert(slab->magic == TM_SLAB_MAGIC);
    assert(slab->tableid == table->tableid);
    if (slab->bitmap == (1ULL << slab_max(stat, slab)) - 1) {
        /* This slab was full; insert into available index. */
        ret = (tmidx_add(&table->avail_idx, slab) >= 0) ? 0 : -1;
    }
    slab->bitmap &= ~(1ULL << rowno);
    memset(&slab->line[rowno * slab->lines_per_row], 0,
        sizeof(struct tmstat_line) * slab->lines_per_row);
    return ret;
}

/**
 * Insert the inode address for a row's containing slab into the row's
 * table's index.  It is expected that the row is already marked
 * in-use in the slab's bitmap.
 *
 * @param[in]   stat        Associated segment.
 * @param[in]   table       Associated table.
 * @param[in]   inode_addr  Inode address of the slab where the row resides.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_row_insert(TMSTAT stat, TMTABLE table, uint32_t inode_addr)
{
    struct tmstat_slab         *slab, *inode_slab, *first_slab;
    struct tmstat_inode        *inode;
    TMTABLE                     inodetable;
    uint32_t                   *link, addr, table_inode;
    unsigned                    i;
    signed                      ret;

    table_inode = *table->inode;

    /*
     * If this is the first row for the table, simply point the table to
     * this slab.
     */
    if (table_inode == 0) {
        *table->inode = TM_INODE(TM_INODE_SLAB(inode_addr), TM_INODE_LEAF);
        return 0;
    }

    /*
     * If this table has only one slab and this row is a member of that
     * slab, there is no index and we don't need one yet.
     */
    if (TM_INODE_SLAB(table_inode) == TM_INODE_SLAB(inode_addr)) {
        /* Nothing to do. */
        return 0;
    }

    /*
     * If this row's slab already has a parent inode, it is already
     * indexed.
     */
    slab = tmstat_slab(stat, inode_addr);
    if (slab == NULL) {
        /* Segment is likely corrupted. */
        TMSTAT_SEGMENT_DAMAGED(stat);
        return -1;
    }
    if (slab->parent != 0) {
        /* Nothing to do. */
        return 0;
    }

    /*
     * If we are adding the second slab now, insert an index node with
     * both slabs as its first two entries.
     */
    if (TM_INODE_ROW(table_inode) == TM_INODE_LEAF) {
        /* Allocate inode. */
        inodetable = tmidx_entry(&stat->table_idx, TM_ID_INODE);
        ret = tmstat_row_alloc(stat, inodetable, &addr, &inode);
        if (ret != 0) {
            /* Allocation error; tmstat_row_alloc sets errno. */
            return -1;
        }
        /* tmstat_row_alloc may have modified *table->inode. */
        table_inode = *table->inode;
        first_slab = tmstat_slab(stat, table_inode);
        if (first_slab == NULL) {
            /* Segment is likely corrupted. */
            TMSTAT_SEGMENT_DAMAGED(stat);
            return -1;
        }
        inodetable->td->rows++;
        /* Insert slabs. */
        inode->child[0] = TM_INODE(TM_INODE_SLAB(table_inode), TM_INODE_LEAF);
        first_slab->parent = addr;
        inode->child[1] = TM_INODE(TM_INODE_SLAB(inode_addr), TM_INODE_LEAF);
        slab->parent = addr;
        /* Update root inode link last. */
        *table->inode = addr;
        return 0;
    }

    /*
     * Attempt to find an empty index entry.
     */
    link = table->inode;
    while ((addr = *link) != 0) {
        inode_slab = tmstat_slab(stat, addr);
        /* Locate inode. */
        if (inode_slab == NULL) {
            /* Segment is likely corrupted. */
            TMSTAT_SEGMENT_DAMAGED(stat);
            return -1;
        }
        inode = (struct tmstat_inode *)
            &inode_slab->line[TM_INODE_ROW(addr) * inode_slab->lines_per_row];
        /* Verify that this is a legitimate inode. */
        if ((char *)(&inode[1]) > (char *)inode_slab + stat->slab_size) {
            /* Segment is likely corrupted. */
            TMSTAT_SEGMENT_DAMAGED(stat);
            return -1;
        }
        /* Scan inode. */
        for (i = 0; i < TM_SZ_INODE; i++) {
            if (inode->child[i] == 0) {
                /* Empty child entry; use it. */
                goto insert_child;
            }
        }
        link = &inode->next;
    }

    /*
     * If the index was full, extend the index by one node.
     */
    inodetable = tmidx_entry(&stat->table_idx, TM_ID_INODE);
    ret = tmstat_row_alloc(stat, inodetable, link, &inode);
    if (ret != 0) {
        /* Allocation error; tmstat_row_alloc sets errno. */
        return -1;
    }
    inodetable->td->rows++;
    i = 0;

    /*
     * Insert into index.
     */
insert_child:
    inode->child[i] = TM_INODE(TM_INODE_SLAB(inode_addr), TM_INODE_LEAF);
    slab->parent = *link;
    return 0;
}

static int
tmstat_slab_insert_n(TMSTAT stat, TMTABLE table, struct tmidx *slabs)
{
    struct tmstat_slab         *inode_slab, *slab;
    struct tmstat_inode        *inode;
    TMTABLE                     inodetable;
    uint32_t                   *link, addr;
    unsigned                    i;
    unsigned                    n;
    unsigned                    n_slabs;
    signed                      ret;

    n_slabs = tmidx_count(slabs);
    n = 0;

    /*
     * Eliminate edge cases.
     */
    if (n_slabs == 0) {
        return 0;
    }
    n = 0;
    if (*table->inode == 0) {
        slab = tmidx_entry(slabs, n);
        assert(slab->parent == 0);
        ret = tmstat_row_insert(stat, table, slab->inode);
        if (ret != 0) {
            return -1;
        }
        if (++n == n_slabs) {
            return 0;
        }
    }
    while (TM_INODE_ROW(*table->inode) == TM_INODE_LEAF) {
        slab = tmidx_entry(slabs, n);
        assert(slab->parent == 0);
        ret = tmstat_row_insert(stat, table, slab->inode);
        if (ret != 0) {
            return -1;
        }
        if (++n == n_slabs) {
            return 0;
        }
    }
    for (i = 0; i < n; ++i) {
        slab = tmidx_entry(slabs, i);
        assert(slab->parent != 0);
    }

    /*
     * Use any empty index entries.
     */
    link = table->inode;
    while ((addr = *link) != 0) {
        inode_slab = tmstat_slab(stat, addr);
        /* Locate inode. */
        if (inode_slab == NULL) {
            /* Segment is likely corrupted. */
            TMSTAT_SEGMENT_DAMAGED(stat);
            return -1;
        }
        inode = (struct tmstat_inode *)
            &inode_slab->line[TM_INODE_ROW(addr) * inode_slab->lines_per_row];
        /* Verify that this is a legitimate inode. */
        if ((char *)(&inode[1]) > (char *)inode_slab + stat->slab_size) {
            /* Segment is likely corrupted. */
            TMSTAT_SEGMENT_DAMAGED(stat);
            return -1;
        }
        /* Scan inode. */
        for (i = 0; i < TM_SZ_INODE; ++i) {
            if (inode->child[i] == 0) {
                /* Empty child entry; use it. */
                slab = tmidx_entry(slabs, n);
                assert(slab->parent == 0);
                inode->child[i] = TM_INODE(TM_INODE_SLAB(slab->inode),
                        TM_INODE_LEAF);
                slab->parent = *link;
                if (++n == n_slabs) {
                    return 0;
                }
            }
        }
        link = &inode->next;
    }

    /*
     * Create new inodes as necessary.
     */
    inodetable = tmidx_entry(&stat->table_idx, TM_ID_INODE);
    for (;;) {
        ret = tmstat_row_alloc(stat, inodetable, link, &inode);
        if (ret != 0) {
            /* Allocation error; tmstat_row_alloc sets errno. */
            return -1;
        }
        inodetable->td->rows++;
        for (i = 0; i < TM_SZ_INODE; ++i) {
            slab = tmidx_entry(slabs, n);
            assert(slab->parent == 0);
            inode->child[i] = TM_INODE(TM_INODE_SLAB(slab->inode),
                    TM_INODE_LEAF);
            slab->parent = *link;
            if (++n == n_slabs) {
                return 0;
            }
        }
        link = &inode->next;
    }
}

/**
 * Add (allocate and insert) row to table.
 *
 * @param[in]   stat        Assocated segment.
 * @param[in]   table       Associated table.
 * @param[out]  row         New row.
 * @param[out]  inode_addr  Row's inode address.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_row_add(TMSTAT stat, TMTABLE table, void *row, uint32_t *inode_addr)
{
    uint32_t        inode;
    signed          ret;

    ret = tmstat_row_alloc(stat, table, &inode, row);
    if (ret == 0) {
        ret = tmstat_row_insert(stat, table, inode);
    }
    table->td->rows++;
    table->td->is_sorted = false;
    if (inode_addr != NULL) {
        *inode_addr = inode;
    }
    return ret;
}

static int
tmstat_row_add_n(TMSTAT stat, TMTABLE table, TMROW *row, unsigned n)
{
    struct tmidx            slabs;
    signed                  ret;

    tmidx_init(&slabs);
    ret = tmstat_row_alloc_n(stat, table, row, n, &slabs);
    if (ret != 0) {
        tmidx_free(&slabs);
        return ret;
    }
    ret = tmstat_slab_insert_n(stat, table, &slabs);
    tmidx_free(&slabs);
    table->td->is_sorted = false; /* Even on failure, some may be inserted. */
    if (ret == 0) {
        table->td->rows += n;
        return 0;
    } else {
        return ret;
    }
}

/**
 * Remove the inode address for a row's containing slab from the row's
 * table's index.
 *
 * @param[in]   stat        Associated segment.
 * @param[in]   table       Associated table.
 * @param[in]   inode_addr  Inode address to remove.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_row_remove(TMSTAT stat, TMTABLE table, uint32_t inode_addr)
{
    unsigned                i;
    uint32_t               *link, addr;
    struct tmstat_slab     *inode_slab, *row_slab;
    struct tmstat_inode    *inode;
    signed                  ret = 0;
    TMTABLE                 inodetable;

    if (TM_INODE_ROW(*table->inode) == TM_INODE_LEAF) {
        /* This table only has one slab; no inode list to remove from. */
        goto bookkeeping;
    }

    row_slab = tmstat_slab(stat, inode_addr);
    if (row_slab == NULL) {
        /* Segment is likely corrupted. */
        TMSTAT_SEGMENT_DAMAGED(stat);
        ret = -1;
        goto out;
    }
    if (row_slab->bitmap != 0) {
        /* This slab still has entries; don't remove. */
        goto bookkeeping;
    }

    /*
     * Search for the entry.
     */
    inode_addr = TM_INODE(TM_INODE_SLAB(inode_addr), TM_INODE_LEAF);
    for (link = table->inode; *link != 0; link = &inode->next) {
        /* Locate inode. */
        inode_slab = tmstat_slab(stat, *link);
        if (inode_slab == NULL) {
            /* Segment is likely corrupted. */
            TMSTAT_SEGMENT_DAMAGED(stat);
            ret = -1;
            goto out;
        }
        addr = *link;
        inode = (struct tmstat_inode *)
            &inode_slab->line[TM_INODE_ROW(addr) * inode_slab->lines_per_row];
        /* Verify that this is a legitimate inode. */
        if ((char *)(&inode[1]) > (char *)inode_slab + stat->slab_size) {
            /* Segment is likely corrupted. */
            TMSTAT_SEGMENT_DAMAGED(stat);
            ret = -1;
            goto out;
        }
        /* Scan inode. */
        for (i = 0; i < TM_SZ_INODE; i++) {
            if (inode->child[i] == inode_addr) {
                /* Found the entry to remove. */
                goto remove;
            }
        }
    }
    /* Address is not in the list. */
    errno = ENOENT;
    ret = -1;
    goto out;

    /*
     * Remove entry.
     */
remove:
    inode->child[i] = 0;
    row_slab = tmstat_slab(stat, inode_addr);
    row_slab->parent = 0;
    for (i = 0; i < TM_SZ_INODE; ++i) {
        if (inode->child[i] != 0) {
            break;
        }
    }
    if (i == TM_SZ_INODE) {
        inodetable = tmidx_entry(&stat->table_idx, TM_ID_INODE);
        /* Unlink inode. */
        *link = inode->next;
        /* Free row. */
        ret = tmstat_row_free(stat, inodetable, addr);
    }
bookkeeping:
    table->td->rows--;
out:
    return ret;
}

/*
 * Create segment.
 */
static int
_tmstat_create(TMSTAT tmstat, char *name)
{
    TMTABLE                 labeltable;
    struct tmstat_label    *label;
    char                    pathname[PATH_MAX];
    int                     ret;
    unsigned                i;
    time_t                  now;
    char                    nowstr[26];
    char                    leaf_name[sizeof(label->name)];

    /*
     * Validate name.
     */
    if (name != NULL) {
        if (!islower(name[0])) {
            /* Name must start with a lower-case alphanumeric character. */
            errno = EINVAL;
            goto fail;
        }
        if (strnlen(name, sizeof(tmstat->name)) == sizeof(tmstat->name)) {
            /* Name must fit within allotted space. */
            errno = EINVAL;
            goto fail;
        }
        for (i = 1; name[i] != '\0'; i++) {
            if (!islower(name[i]) && !isdigit(name[i]) &&
                (name[i] != '_') && (name[i] != '.') && (name[i] != '-')) {
                /* Invalid name. */
                errno = EINVAL;
                goto fail;
            }
        }
    }

    /*
     * Construct empty segment.
     */
    memset(tmstat, 0, sizeof(*tmstat));
    snprintf(leaf_name, sizeof(leaf_name), "%s", name);

    snprintf(tmstat->name, sizeof(tmstat->name), "%s", name);
    snprintf(tmstat->directory, sizeof(tmstat->directory), TMSTAT_DIR_PRIVATE);
    tmstat->statid = __sync_fetch_and_add(&tmstat_nextid, 1);
    tmstat->slab_size = sysconf(_SC_PAGE_SIZE);
    tmstat->alloc_policy = AS_NEEDED;
    tmstat->origin = CREATE;
    tmidx_init(&tmstat->slab_idx);
    tmidx_init(&tmstat->table_idx);
    tmidx_init(&tmstat->child_idx);

    if (name != NULL) {
        /*
         * Make a token effort to ensure that the private directory exists.
         * If this doesn't work, the open command will fail and we'll report
         * that.
         */
        snprintf(pathname, PATH_MAX, "%s/%s", tmstat_path, TMSTAT_DIR_PRIVATE);
        mkdir(pathname, 0777);
        /* Open backing store. */
        snprintf(pathname, PATH_MAX, "%s/%s/%s", tmstat_path,
            TMSTAT_DIR_PRIVATE, name);
        tmstat->fd = open(pathname, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (tmstat->fd == -1) {
            /* Open failure; open sets errno. */
            warn("%s: open", __func__);
            /*
             * No allocations to cleanup, just fail.
             */
            goto fail;
        }
    } else {
        /* This is an anonymous segment. */
        tmstat->fd = -1;
    }

    /*
     * Register table-descriptor table.
     */
    ret = tmstat_table_register(tmstat, NULL, ".table", tm_cols_table,
        array_size(tm_cols_table), sizeof(struct tmstat_table));
    if (ret != 0) {
        /* 
         * Allocation failure; tmstat_table_register sets errno
         * and does all table specific memory cleanup, but we need
         * to call dealloc to close the fd potentially opened above.
         */
        goto cleanup;
    }

    /*
     * Register inode table.
     */
    ret = tmstat_table_register(tmstat, NULL, ".inode", NULL, 0,
        sizeof(struct tmstat_inode));
    if (ret != 0) {
        /*
         * Allocation failure; tmstat_table_register sets errno
         * and does all table specific memory cleanup, but we need
         * to call dealloc to close the fd potentially opened above.
         */
        goto cleanup;
    }

    /*
     * Register label table and add our label.
     */
    ret = tmstat_table_register(tmstat, &labeltable, ".label", tm_cols_label,
        array_size(tm_cols_label), sizeof(struct tmstat_label));
    if (ret != 0) {
        /* Allocation failure; tmstat_table_register sets errno. */
        goto cleanup;
    }
    /* Add row. */
    ret = tmstat_row_add(tmstat, labeltable, &label, NULL);
    if (ret != 0) {
        /* Allocation faliure; tmstat_row_create sets errno. */
        goto cleanup;
    }
    /* Construct label. */
    now = time(NULL);
    ctime_r(&now, nowstr);
    nowstr[strlen(nowstr) - 1] = '\0'; /* Trim off trailing '\n'. */
    snprintf(label->tree, sizeof(label->tree), TMSTAT_BASE_HEADER);
    snprintf(label->name, sizeof(label->name), "%s", leaf_name);
    snprintf(label->ctime, sizeof(label->ctime), "%s", nowstr);
    label->time = now;

    /*
     * Success!
     */
    return 0;
cleanup:
    /* 
     * Cleanup any allocations.
     */
    _tmstat_dealloc(tmstat);
fail:
    /*
     * Failure.
     */
    return -1;
}

/*
 * Create segment.
 */
int
tmstat_create(TMSTAT *stat, char *name)
{
    TMSTAT tmstat;
    int ret;

    tmstat = (TMSTAT)malloc(sizeof(struct TMSTAT));
    if (tmstat == NULL) {
        /* Memory exhaustion; calloc sets errno. */
        return -1;
    }
    ret = _tmstat_create(tmstat, name);
    if (ret != 0) {
        /* Failure.  _tmstat_create sets errno. */
        free(tmstat);
        return -1;
    }
    *stat = tmstat;
    return 0;
}

/*
 * Publish segment.
 */
int
tmstat_publish(TMSTAT stat, char *directory)
{
    char        path[2][PATH_MAX];
    signed      ret;

    if (stat->origin != CREATE) {
        /* One may only publish segments created through tmstat_create. */
        errno = EINVAL;
        return -1;
    }

    snprintf(stat->directory, sizeof(stat->directory), "%s", directory);

    /*
     * Make a token effort to ensure that the publisher directory exists.
     * If this doesn't work, the rename will fail and we'll report
     * that.
     */
    snprintf(path[0], PATH_MAX, "%s/%s", tmstat_path, stat->directory);
    ret = mkdir(path[0], 0777);

    /*
     * Rename the segment to move it from the private directory to
     * to the publisher directory.  This will atomically insert the
     * segment into the directory and replace any previous incarnation.
     */
    snprintf(path[0], PATH_MAX, "%s/%s/%s", tmstat_path, TMSTAT_DIR_PRIVATE,
            stat->name);
    snprintf(path[1], PATH_MAX, "%s/%s/%s",
        tmstat_path, stat->directory, stat->name);
    ret = rename(path[0], path[1]);
    return ret;
}

/**
 * Free in-process resources for a segment without freeing the
 * struct TMSTAT itself.
 */
static void
_tmstat_dealloc(TMSTAT stat)
{
    TMTABLE             table;
    TMSTAT              child;
    TMROW               row;

    if (stat == NULL) {
        return;
    }

    /*
     * Free tables.
     */
    TMIDX_FOREACH(&stat->table_idx, table) {
        tmidx_free(&table->avail_idx);
        if (stat->origin == SUBSCRIBE) {
            /*
             * We leak and complain only for handles made with
             * tmstat_subscribe because this is the only type we
             * automatically refresh.  (Persistent row handles inhibit
             * refresh and so are discouraged.)
             */
            if (table->row_list.lh_first != NULL) {
                /*
                 * This list should be empty right now.  Issue warning and
                 * make a crash somewhat less likely and more easily
                 * diagnosed by leaking the TMROW objects and setting their
                 * data pointers to NULL.
                 */
                warnx("BUG: tmstat_dealloc invoked on a handle with "
                      "rows outstanding; release all rows before "
                      "calling tmstat_dealloc");
                while ((row = table->row_list.lh_first) != NULL) {
                    LIST_REMOVE(row, entry);
                    row->data = NULL;
                }
            }
        } else {
            while ((row = table->row_list.lh_first) != NULL) {
                /* Free row handles, but don't modify segment. */
                LIST_REMOVE(row, entry);
                free(row);
            }
        }
        for (unsigned i = 0; i < table->col_count; i++) {
            free(table->col[i].name);
        }
        free(table->col);
        free(table->key_col);
        free(table);
    }

    /*
     * Unmap slabs.
     */
    tmstat_munmap(stat);

    /*
     * Destroy children.
     */
    TMIDX_FOREACH(&stat->child_idx, child) {
        tmstat_destroy(child);
    }

    /*
     * Free segment.
     */
    if (stat->fd != -1) {
        /* Truncate to the size that's actually used and close. */
        if (stat->origin == CREATE)
            ftruncate(stat->fd, tmidx_count(&stat->slab_idx) * stat->slab_size);
        close(stat->fd);
    }
    tmidx_free(&stat->table_idx);
    tmidx_free(&stat->slab_idx);
    tmidx_free(&stat->child_idx);
}

/**
 * Free in-process resources for a segment.
 */
void
tmstat_dealloc(TMSTAT stat)
{
    _tmstat_dealloc(stat);
    free(stat);
}

/**
 * Attempt to remove the segment from the filesystem.
 */
int
tmstat_unlink(TMSTAT stat)
{
    if (stat->origin == CREATE) {
        char pathname[PATH_MAX];
        int ret;
        snprintf(pathname, PATH_MAX, "%s/%s/%s",
            tmstat_path, stat->directory, stat->name);
        ret = unlink(pathname);
        if (ret >= 0) {
            stat->name[0] = '\0';
        }
        return ret;
    } else {
        errno = EINVAL;
        return -1;
    }
}

/**
 * Destroy segment, which means unlink and dealloc.
 */
void
tmstat_destroy(TMSTAT stat)
{
    if (stat == NULL) {
        return;
    }
    tmstat_unlink(stat);
    tmstat_dealloc(stat);
}

/**
 * Register union table set.
 *
 * @param[in]   stat        Union segment.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_union_register(TMSTAT stat)
{
    struct tmidx    table_idx;
    TMSTAT          child;
    TMTABLE         child_table, table;
    signed          ret;
    bool            one_child;

    tmidx_init(&table_idx);
    TMIDX_FOREACH(&stat->child_idx, child) {
        TMIDX_FOREACH(&child->table_idx, child_table) {
            /* Insert table (if new name). */
            TMIDX_FOREACH(&table_idx, table) {
                if (strcmp(child_table->td->name, table->td->name) == 0) {
                    goto known_table;
                }
            }
            ret = tmidx_add(&table_idx, child_table);
            if (ret == -1) {
                /* Insertion failure; tmidx_add sets errno. */
                goto out;
            }
known_table: ;
        }
    }
    /* Register tables. */
    one_child = tmidx_count(&stat->child_idx) == 1;
    TMIDX_FOREACH(&table_idx, child_table) {
        if (child_table->tableid >= TM_ID_USER) {
            ret = tmstat_table_register(stat, &table, child_table->td->name,
                child_table->col, child_table->col_count, child_table->rowsz);
            if (ret != 0) {
                /* Registration failure: tmstat_table_register sets errno. */
                goto out;
            }
            /*
             * Unions are considered sorted only if there is only one
             * child segment and the table is sorted in it.  This is a
             * somewhat arbitrary determination, as this attribute is
             * never consulted directly by the library.  We set it
             * because it is exposed through tmstat_is_table_sorted.
             */
            table->td->is_sorted = child_table->td->is_sorted && one_child;
        }
    }
    ret = 0;
out:
    tmidx_free(&table_idx);
    return ret;
}

/*
 * Make a handle which refers to the union of multiple segments.
 */
static int
_tmstat_union(TMSTAT tmstat, TMSTAT *child, unsigned count)
{
    signed          ret;

    /* Create segment. */
    ret = _tmstat_create(tmstat, NULL);
    if (ret == -1) {
        /* Creation failure; tmstat_create sets errno. */
        /* Cannot jump to out because it assumes success from here. */
        return -1;
    }
    for (unsigned i = 0; i < count; i++) {
        ret = tmidx_add(&tmstat->child_idx, child[i]);
        if (ret == -1) {
            /* Insertion failure; tmstat_idx_add sets errno. */
            goto out;
        }
    }
    tmstat->origin = UNION;
    ret = tmstat_union_register(tmstat);
out:
    if (ret != 0) {
        _tmstat_dealloc(tmstat);
    }
    return ret;
}

/*
 * Make a handle which refers to the union of multiple segments.
 */
int
tmstat_union(TMSTAT *stat, TMSTAT *child, unsigned count)
{
    TMSTAT          tmstat;
    signed          ret;

    *stat = NULL;
    tmstat = (TMSTAT)malloc(sizeof(struct TMSTAT));
    if (tmstat == NULL) {
        /* Memory exhaustion; calloc sets errno. */
        return -1;
    }
    ret = _tmstat_union(tmstat, child, count);
    if (ret == -1) {
        /* Failure; tmstat_union sets errno. */
        free(tmstat);
        return -1;
    }
    *stat = tmstat;
    return 0;
}

/*
 * Subscribe to slab.
 *
 * @param[in]   stat        Associated segment.
 * @param[in]   slab        Target slab.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_subscribe_slab(TMSTAT stat, struct tmstat_slab *slab)
{
    unsigned                line;
    struct tmstat_table    *table;
    signed                  ret;

    TMSTAT_SLAB_FOREACH(stat, slab, line, table) {
        TMTABLE         tmtable = (TMTABLE)calloc(1, sizeof(struct TMTABLE));

        if (tmtable == NULL) {
            /* Allocation failure; calloc sets errno. */
            ret = -1;
            goto out;
        }
        tmtable->stat = stat;
        tmtable->tableid = table->tableid;
        tmtable->rowsz = table->rowsz;
        tmtable->td = table;
        tmtable->inode = &table->inode;
        ret = tmidx_add(&stat->table_idx, tmtable);
        if (ret == -1) {
            /* Insertion failure; tmidx_add sets errno. */
            free(tmtable);
            goto out;
        } else if (ret != tmtable->tableid) {
            /*
             * Either this segment was corrupted or we have been
             * passed a stat structure that has already had its
             * tables loaded.
             */
            errno = EINVAL;
            free(tmtable);
            ret = -1;
            goto out;
        }
    }
    ret = 0;
out:
    return ret;
}

/*
 * As a favor to tmstat_row_cmp, copy key column definitions into
 * their own little space.
 *
 * @param[in]   tmtable     The table to operate on.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_pull_key_cols(TMTABLE tmtable)
{
    unsigned key_col_count = 0;
    for (unsigned i = 0; i < tmtable->col_count; i++) {
        key_col_count += (tmtable->col[i].rule == TMSTAT_R_KEY);
    }
    assert(tmtable->key_col == NULL);
    tmtable->key_col = calloc(key_col_count, sizeof(struct TMCOL));
    if (tmtable->key_col == NULL) {
        return -1;
    }
    tmtable->key_col_count = key_col_count;
    for (unsigned i = 0, j = 0; i < tmtable->col_count; i++) {
        if (tmtable->col[i].rule == TMSTAT_R_KEY) {
            memcpy(tmtable->key_col+j, tmtable->col+i, sizeof(struct TMCOL));
            j++;
        }
    }
    return 0;
}

/*
 * Pick up the column descriptors from one slab.
 *
 * @param[in]   stat        Associated segment.
 * @param[in]   slab        Source slab.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_subscribe_cols(TMSTAT stat, struct tmstat_slab *slab)
{
    unsigned                rowno;
    struct tmstat_column   *column;
    TMTABLE                 tmtable;
    TMCOL                   tmcol;
    signed                  ret;

    TMSTAT_SLAB_FOREACH(stat, slab, rowno, column) {
        /* Construct TMCOL from column descriptor. */
        tmtable = (TMTABLE)tmidx_entry(&stat->table_idx, column->tableid);
        if (tmtable == NULL) {
            /* Missing column descriptor table; bail. */
            errno = ENOENT;
            return -1;
            goto out;
        }
        if (tmtable->col == NULL) {
            /* Allocate TMCOL objects. */
            tmtable->col = (TMCOL)
                calloc(tmtable->td->cols, sizeof(struct TMCOL));
            if (tmtable->col == NULL) {
                /* Allocation failure; calloc sets errno. */
                ret = -1;
                goto out;
            }
        }
        assert(tmtable->col_count < tmtable->td->cols);
        tmcol = &tmtable->col[tmtable->col_count++];
        tmcol->name = tmstat_name_copy(column->name);
        if (tmcol->name == NULL) {
            /* Allocation failure; tmstat_name_copy sets errno. */
            ret = -1;
            goto out;
        }
        tmcol->type = column->type;
        tmcol->rule = column->rule;
        tmcol->size = column->size;
        tmcol->offset = column->offset;
        if (tmcol->rule != TMSTAT_R_KEY) {
            /* This table has merge instructions; enable merge pass. */
            tmtable->want_merge = true;
        }
        if (tmtable->td->cols == tmtable->col_count) {
            if (tmstat_pull_key_cols(tmtable) != 0) {
                ret = -1;
                goto out;
            }
        }
    }
    ret = 0;
out:
    return ret;
}

struct tmstat_core_slab {
    unsigned int offset;
    int size;
};

struct tmstat_core_segment {
    uint32_t id;
    struct tmstat_core_slab *slabs;
    int max_inode;
};

/*
 *  Determine whether file is a core file.
 *
 * @param[in]   p           Pointer to mmap'd file contents.
 * @param[in]   size        Size of file contents.
 * @return true if file is a core file.
 */
static bool
tmstat_is_core(unsigned char *p, off_t size)
{
    Elf32_Ehdr *elf32_hdr = (Elf32_Ehdr *)p;
    Elf64_Ehdr *elf64_hdr = (Elf64_Ehdr *)p;

    if (size < SELFMAG) {
        /* File is too small; fail. */
        return false;
    }

    if (memcmp(p, ELFMAG, SELFMAG) != 0) {
        /* Correct magic not present; fail. */
        return false; 
    }

    return ((elf32_hdr->e_ident[EI_CLASS] == ELFCLASS32) &&
                (elf32_hdr->e_type == ET_CORE)) ||
           ((elf64_hdr->e_ident[EI_CLASS] == ELFCLASS64) &&
                (elf64_hdr->e_type == ET_CORE));
}

/*
 * Extract tmstat information from core dump to tmss file(s).  Only one of the
 * pointers of elf32_hdr or elf64_hdr will be valid, the other will be NULL.
 *
 * @param[in]   core        Pointer to mmap'd core dump file.
 * @param[in]   size        Length of mmap'd core dump file.
 * @param[in]   dirname     Name of directory for segments.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_extract_core_segments(unsigned char *core, off_t size,
        const char *dirname)
{
    int                         ret = -1;
    int                         count, next_off = 0;
    Elf32_Ehdr                 *elf32_hdr = NULL;
    Elf64_Ehdr                 *elf64_hdr = NULL;
    Elf32_Phdr                 *elf32_proghdr = NULL;
    Elf64_Phdr                 *elf64_proghdr = NULL;
    int                         i, inode;
    struct tmstat_core_segment *segments = NULL;
    int                         segments_length = 0;
    struct tmstat_slab         *tmss_slab;
    struct tmstat_core_segment *segment;
    struct tmstat_core_slab    *slab;

#define PROGHDR(field) ((elf32_proghdr != NULL) ? (elf32_proghdr->field) : \
        (elf64_proghdr->field))
#define ELFHDR(field) ((elf32_hdr != NULL) ? (elf32_hdr->field) : \
        (elf64_hdr->field))

    if (!tmstat_is_core(core, size)) {
        /* File is not a core file; fail. */
        goto out;
    }

    if (((Elf32_Ehdr *)core)->e_ident[EI_CLASS] == ELFCLASS32) {
        elf32_hdr = (Elf32_Ehdr *)core;
    } else if (((Elf64_Ehdr *)core)->e_ident[EI_CLASS] == ELFCLASS64) {
        elf64_hdr = (Elf64_Ehdr *)core;
    } else {
        warnx("%s: ELF file of unsupported architecture", __func__);
        goto out;
    }
    if (((elf32_hdr != NULL) && (elf32_hdr->e_type != ET_CORE)) ||
            ((elf64_hdr != NULL) && (elf64_hdr->e_type != ET_CORE))) {
        warnx("%s: ELF file of unsupported type (not a core file)",
              __func__);
        goto out;
    }

    /* Initialize the elf header structure as defined in include/elf.h. */
    for (count = 0; count < ELFHDR(e_phnum); count++) {
        /* Set the offset appropriately. */
        if (elf32_hdr != NULL) {
            elf32_proghdr = (Elf32_Phdr *) ((unsigned char *)
                    &core[(unsigned int) (next_off + elf32_hdr->e_phoff)]);
        } else {
            elf64_proghdr = (Elf64_Phdr *) ((unsigned char *)
                    &core[(unsigned int) (next_off + elf64_hdr->e_phoff)]);
        }

        /* Find LOAD segment. */
        if ((PROGHDR(p_type) == PT_LOAD) &&
                (PROGHDR(p_filesz) > sizeof(struct tmstat_slab))) {
            tmss_slab = (elf32_hdr != NULL) ?
                (struct tmstat_slab *)&core[elf32_proghdr->p_offset] :
                (struct tmstat_slab *)&core[elf64_proghdr->p_offset];
            /* Check TMSS magic. */
            if (tmss_slab->magic == TM_SLAB_MAGIC) {
                /* Locate segment for slab, if present. */
                for(i = 0, segment = NULL; i < segments_length; i++) {
                    if (segments[i].id == tmss_slab->statid) {
                        segment = &segments[i];
                        break;
                    }
                }
                /* Create segment, if not present. */
                if (segment == NULL) {
                    void *p = realloc(segments,
                            sizeof(struct tmstat_core_segment) *
                            (segments_length + 1));
                    if (p == NULL) {
                        warn("%s: realloc", __func__);
                        ret = -1;
                        goto out;
                    }
                    segments = p;
                    segment = &segments[segments_length++];
                    memset(segment, 0, sizeof(*segment));
                    segment->id = tmss_slab->statid;
                    segment->max_inode = -1;
                }

                /* This is TMSS segment. */
                inode = TM_INODE_SLAB(tmss_slab->inode);
                if (inode > segment->max_inode) {
                    /* Need more memory for the tmss table; expand. */
                    void *p = realloc(segment->slabs,
                            sizeof(struct tmstat_core_slab) * (inode + 1));
                    if (p == NULL) {
                        /* Memory allocation error. */
                        warn("%s: realloc", __func__);
                        ret = -1;
                        goto out;
                    }
                    segment->slabs = (struct tmstat_core_slab *)p;
                    if (segment->max_inode >= 0) {
                        memset(&segment->slabs[segment->max_inode + 1], 0,
                                sizeof(struct tmstat_core_slab) *
                                    (inode - segment->max_inode));
                    }
                    segment->max_inode = inode;
                }
                slab = &segment->slabs[inode];
                slab->offset = PROGHDR(p_offset);
                slab->size = PROGHDR(p_filesz);
            }
        }
        next_off += ELFHDR(e_phentsize);
    }

    /* Write segments to individual segment files. */
    for (i = 0; i < segments_length; i++) {
        char filename[PATH_MAX];
        int fd;

        snprintf(filename, sizeof(filename), "%s/segment_%d", dirname, i);
        fd = open(filename, O_CREAT | O_WRONLY, S_IRWXU);
        if (fd == -1) {
            warn("%s: open", __func__);
            ret = -1;
            goto out;
        }
        segment = &segments[i];
        for (inode = 0; inode <= segment->max_inode; ++inode) {
            slab = &segment->slabs[inode];
            if (write(fd, &core[slab->offset], slab->size) == -1) {
                /* Mapping failure; write sets errno. */
                warn("%s: write", __func__);
                ret = -1;
                goto out;
            }
        }
        close(fd);
    }

    ret = 0;

out:
    if (segments != NULL) {
        for (i = 0; i < segments_length; i++) {
            free(segments[i].slabs);
        }
        free(segments);
    }
    return ret;

#undef ELFHDR
#undef PROGHDR
}

/*
 * Subscribe to core file.
 *
 * @param[out]  tmstat      New segment.
 * @param[in]   fd          File descriptor.
 * @param[in]   size        File size.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_subscribe_core(TMSTAT *tmstat, int fd, off_t size)
{
    int                     ret = -1;
    char                    dirname[] = "/tmp/tmctl.XXXXXX";
    DIR                    *dir;
    struct dirent          *dirent;
    unsigned char          *p = MAP_FAILED;

    p = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
        /* Unable to map entire file; fail. */
        dirname[0] = '\0';
        goto out;
    }

    if (!tmstat_is_core(p, size)) {
        /* File is not a core file; fail. */
        dirname[0] = '\0';
        goto out;
    }

    /* Make temporary directory for core segments. */
    if (mkdtemp(dirname) == NULL) {
        warn("%s: mkdtemp", __func__);
        dirname[0] = '\0';
        goto out;
    }

    /* Extract core segments into files in directory. */
    if (tmstat_extract_core_segments(p, size, dirname) != 0) {
        /* Unable to extract tmstat information from core dump. */
        goto out;
    }

    /* Subscribe to extracted files. */
    if (tmstat_subscribe(tmstat, dirname) != 0) {
        /* Failure; _tmstat_subscribe sets errno. */
        goto out;
    }

    ret = 0;

out:
    if (p != MAP_FAILED) {
        munmap(p, size);
    }

    if (dirname[0] != '\0') {
        /* Unlink temporary directory and files */
        dir = opendir(dirname);
        if (dir != NULL) {
            while ((dirent = readdir(dir)) != NULL) {
                char filename[PATH_MAX];
                if (dirent->d_name[0] == '.') {
                    /* Ignore everything starting with '.'. */
                    continue;
                }
                snprintf(filename, sizeof(filename), "%s/%s", 
                        dirname, dirent->d_name);
                unlink(filename);
            }
        }
        rmdir(dirname);
    }

    return ret;
}


/*
 * Subscribe to file.
 *
 * @param[out]  stat        New segment.
 * @param[in]   fd          File descriptor.
 * @param[in]   size        File size.
 * @param[in]   directory   Path to new segment.
 * @param[in]   name        New segment name.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_subscribe_file(TMSTAT *stat, int fd, off_t size,
                      const char *directory, const char *name)
{
    struct tmidx            slab_idx;
    struct tmstat_slab     *slab;
    struct tmstat_table    *table;
    TMSTAT                  tmstat = NULL;
    char                   *p;
    signed                  ret;
    unsigned                i, slab_count;

    /*
     * Attempt subscription to core file.
     */
    ret = tmstat_subscribe_core(&tmstat, fd, size);
    if (ret == 0) {
        /* File is a core file - subscription complete. */
        goto out;
    }

    tmstat = (TMSTAT)calloc(1, sizeof(struct TMSTAT));
    if (tmstat == NULL) {
        /* Memory exhaustion; calloc sets errno. */
        ret = -1;
        goto out;
    }

    /*
     * Construct empty segment.
     */
    snprintf(tmstat->name, sizeof(tmstat->name), "%s", name);
    snprintf(tmstat->directory, sizeof(tmstat->directory), "%s", directory);
    tmstat->slab_size = sysconf(_SC_PAGE_SIZE);
    tmstat->fd = fd;
    tmstat->alloc_policy = AS_NEEDED;
    tmstat->origin = SUBSCRIBE_FILE;
    tmidx_init(&tmstat->slab_idx);
    tmidx_init(&tmstat->table_idx);

    /*
     * Map the entire file.
     */
    ret = tmstat_mmap(tmstat, size / tmstat->slab_size, PROT_READ, 0);
    if (ret == -1) {
        /* Mapping failure; tmstat_mmap sets errno. */
        goto cleanup;
    }
    p = tmstat->next_page;

    /*
     * Mark the freshly mapped pages as not free.
     */
    tmstat->next_page = p + ((size / tmstat->slab_size) * tmstat->slab_size);

    /*
     * Create slab index for file.
     */
    if ((size % tmstat->slab_size) != 0) {
        warnx("%s: segment is not an even number of slabs", __func__);
        errno = EINVAL;
        ret = -1;
        goto cleanup;
    }
    slab_count = size / tmstat->slab_size;
    for (i = 0; i < slab_count; i++) {
        ret = tmidx_add(&tmstat->slab_idx, &p[i * tmstat->slab_size]);
        if (ret == -1) {
            /* Insertion failure; tmidx_add sets errno. */
            goto cleanup;
        }
    }

    /*
     * Construct tables.
     */
    tmidx_init(&slab_idx);
    slab = (struct tmstat_slab *)p;
    table = (struct tmstat_table *)&slab->line[TM_ID_TABLE];
    ret = tmstat_slab_idx(tmstat, table, &slab_idx);
    if (ret != 0) {
        goto cleanup;
    }
    TMIDX_FOREACH(&slab_idx, slab) {
        ret = tmstat_subscribe_slab(tmstat, slab);
        if (ret != 0) {
            /* Internal error; tmstat_subscribe_slab sets errno. */
            tmidx_free(&slab_idx);
            goto cleanup;
        }
    }
    tmidx_free(&slab_idx);

    /*
     * Construct column descriptors.
     */
    tmidx_init(&slab_idx);
    slab = (struct tmstat_slab *)p;
    table = (struct tmstat_table *)&slab->line[TM_ID_COLUMN];
    ret = tmstat_slab_idx(tmstat, table, &slab_idx);
    TMIDX_FOREACH(&slab_idx, slab) {
        ret = tmstat_subscribe_cols(tmstat, slab);
        if (ret != 0) {
            /* Internal error; tmstat_subscribe_slab sets errno. */
            tmidx_free(&slab_idx);
            goto cleanup;
        }
    }
    tmidx_free(&slab_idx);

    /*
     * Done.
     */
    ret = 0;

out:
    *stat = tmstat;
    return ret;

cleanup:
    tmstat_munmap(tmstat);
    free(tmstat);
    return ret;
}

/*
 * Subscribe to directory.
 */
static int
_tmstat_subscribe(TMSTAT tmstat, const char *directory)
{
    struct stat             status;
    DIR                    *dir;
    struct dirent          *dirent;
    char                    path[PATH_MAX], filename[PATH_MAX];
    TMSTAT                  child;
    struct tmidx            child_idx;
    signed                  fd, ret;
    unsigned                i;
    struct stat             st;

    if (directory[0] == '/') {
        /* Caller specified absolute path */
        strncpy(path, directory, sizeof(path));
    } else {
        /* Directory is relative to tmstat directory.*/
        snprintf(path, sizeof(path), "%s/%s", tmstat_path, directory);
    }
    dir = opendir(path);
    if (dir == NULL) {
        /* Error opening directory; opendir sets errno. */
        return -1;
    }
    ret = stat(path, &st);
    if (ret != 0) {
        /* Error stating directory; stat sets errno. */
        closedir(dir);
        return -1;
    }
    tmidx_init(&child_idx);
    while ((dirent = readdir(dir)) != NULL) {
        if (dirent->d_name[0] == '.') {
            /* Ignore everything starting with '.'. */
            continue;
        }
        snprintf(filename, sizeof(filename), "%s/%s", path, dirent->d_name);
        fd = open(filename, O_RDONLY);
        if (fd == -1) {
            /* Failure opening: silently ignore (race conditions). */
            continue;
        }
        ret = fstat(fd, &status);
        if (ret != 0) {
            /* Failure obtaining status; warn, but continue. */
            warn("tmstat_subscribe: stat: %s", filename);
            close(fd);
            continue;
        }
        if (S_ISREG(status.st_mode) && (status.st_size > 0)) {
            /* This is a regular file with data in it; subscribe. */
            ret = tmstat_subscribe_file(&child, fd, status.st_size,
                    directory, dirent->d_name);
            if (ret != 0) {
                /* Failure subscribing to file; warn, but continue. */
                warn("tmstat_subscribe: %s", filename);
                close(fd);
                continue;
            }
            ret = tmidx_add(&child_idx, child);
            if (ret == -1) {
                /* Insertion error; warn, but continue. */
                warn("tmidx_add");
                /* The fd is passed into the tmstat_subscribe_file()
                 * call above and copied into the stat segement.
                 * The call to tmstat_dealloc() below then closes
                 * the file. 
                 * There is no fd leak here.
                 */
                tmstat_dealloc(child);
                continue;
            }
        } else {
            /* Not a segment; ignore. */
            close(fd);
        }
    }
    closedir(dir);
    ret = _tmstat_union(tmstat, (TMSTAT *)child_idx.a, child_idx.c);
    if (ret != 0) {
        /* Failure creating union segment; tmstat_union sets errno. */
        for (i = 0; i < child_idx.c; i++) {
            /* Destroy subscriber segment. */
            tmstat_destroy((TMSTAT)child_idx.a[i]);
        }
    } else {
        tmstat->ctime.tv_sec = st.st_ctim.tv_sec;
        tmstat->ctime.tv_nsec = st.st_ctim.tv_nsec;
        tmstat->origin = SUBSCRIBE;
        strncpy(tmstat->directory, directory, sizeof(tmstat->directory));
    }
    tmidx_free(&child_idx);
    return ret;
}


/*
 * Subscribe to directory.
 */
int
tmstat_subscribe(TMSTAT *stat, char *directory)
{
    TMSTAT          tmstat;
    signed          ret;

    *stat = NULL;
    tmstat = (TMSTAT)malloc(sizeof(struct TMSTAT));
    if (tmstat == NULL) {
        /* Memory exhaustion; calloc sets errno. */
        return -1;
    }
    ret = _tmstat_subscribe(tmstat, directory);
    if (ret == -1) {
        /* Failure; _tmstat_subscribe sets errno. */
        free(tmstat);
        return -1;
    }
    *stat = tmstat;
    return 0;
}

/*
 * Extract segments to directory.
 */
int
tmstat_extract(const char *path, const char *dirname)
{
    int                     fd, ret = -1;
    struct stat             status;
    unsigned char          *p = MAP_FAILED;

    fd = open(path, O_RDONLY | O_LARGEFILE);
    if (fd == -1) {
        /* Failure opening; fail. */
        warn("%s: open: %s", __func__, path);
        goto out;
    }
    
    ret = fstat(fd, &status);
    if (ret != 0) {
        /* Unable to get status; fail. */
        warn("%s: stat: %s", __func__, path);
        goto out;
    }

    p = mmap(NULL, status.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
        /* Unable to map entire file; fail. */
        warn("%s: mmap", __func__);
        goto out;
    }

    /* Extract core segments into files in directory. */
    if (tmstat_extract_core_segments(p, status.st_size, dirname) != 0) {
        /* Unable to extract tmstat information from core dump. */
        warn("%s: tmstat_extract_core_segments", __func__);
        goto out;
    }

out:
    if (p != MAP_FAILED) {
        munmap(p, status.st_size);
    }
    if (fd != -1) {
        close(fd);
    }

    return ret;
}

/*
 * Subscribe to individual file.
 */
int
tmstat_read(TMSTAT *stat, char *path)
{
    struct stat             status;
    signed                  fd, ret;
    TMSTAT                  child;
    struct tmidx            child_idx;

    tmidx_init(&child_idx);
    fd = open(path, O_RDONLY | O_LARGEFILE);
    if (fd == -1) {
        /* Failure opening; fail. */
        warn("tmstat_read: open: %s", path);
        ret = -1;
        goto out;
}
    ret = fstat(fd, &status);
    if (ret != 0) {
        /* Failure obtaining status; warn, but continue. */
        warn("tmstat_read: stat: %s", path);
        goto fail;
    }
    if (S_ISREG(status.st_mode)) {
        /* This is a regular file; subscribe. */

        /*
         * XXX: Why are we setting directory to "" and 
         * name to the complete path?  This differs from
         * tmstat_subscribe.  Intentional?
         */
        ret = tmstat_subscribe_file(&child, fd, status.st_size, "", path);
        if (ret != 0) {
            /* Failure subscribing to file. */
            warn("tmstat_read: %s", path);
            goto fail;
        }
        if (child->child_idx.c == 0) { 
            /* Success. */
            ret = tmidx_add(&child_idx, child);
            if (ret == -1) {
                /* Insertion error. */
                warn("tmidx_add");
                tmstat_destroy(child);
                goto out;
            }
            ret = tmstat_union(stat, (TMSTAT *)child_idx.a, child_idx.c);
            if (ret != 0) {
                /* Failure creating union segment; tmstat_union sets errno. */
                warn("tmstat_union");
                tmstat_destroy(child);
                goto out;
            }
        } else {
            /*
             * Child is already a union.  This should only happen when the
             * subscribed file is a core file with multiple contexts.
             */
            *stat = child;
        }

        (*stat)->origin = READ;

        goto out;
    } else {
        /* Not a segment; ignore. */
        warnx("tmstat_read: %s: not a regular file", path);
        goto fail;
    }
fail:
    close(fd);
out:
    tmidx_free(&child_idx);
    return ret;
}

/*
 * Register table.
 */
int
tmstat_table_register(TMSTAT stat, TMTABLE *table, char *name,
                      TMCOL col, unsigned count, unsigned size)
{
    TMTABLE                 tmtable = NULL, tdtable, coltable;
    struct tmstat_column   *column;
    unsigned                i, j, ofs;
    signed                  ret;
    uint32_t                inode;
    uint8_t                 fake_row[size];
    uint32_t                colinode[count];

    if (stat == NULL || (count > 0 && col == NULL)) {
        errno = EINVAL;
        goto fail;
    }

    /*
     * Name must start with a lower-case letter or a dot, fit
     * in the alloted space, and be unique.
     */
    if (name == NULL || (!islower(name[0]) && (name[0] != '.')) ||
        (strlen(name) >= sizeof(tmtable->td->name)) ||
        (tmstat_table(stat, name) != NULL)) {
        errno = EINVAL;
        goto fail;
    }
    for (i = 1; name[i] != '\0'; i++) {
        if (!islower(name[i]) && !isdigit(name[i]) &&
            (name[i] != '_') && (name[i] != '/')) {
            /* Invalid name. */
            errno = EINVAL;
            goto fail;
        }
    }

    /*
     * Validate column names and verify that no columns overlap nor
     * extend past the row size.
     */
    memset(fake_row, 0, size);
    for (j = 0; j < count; j++) {
        if (!islower(col[j].name[0]) && !isdigit(col[j].name[0])) {
            /* Name must start with a lower-case letter or a digit. */
            errno = EINVAL;
            goto fail;
        }
        for (i = 1; col[j].name[i] != '\0'; i++) {
            if ((!islower(col[j].name[i]) && !isdigit(col[j].name[i]) &&
                (col[j].name[i] != '_') && (col[j].name[i] != '.')) ||
                (i >= TM_MAX_NAME)) {
                /* Invalid name. */
                errno = EINVAL;
                goto fail;
            }
        }
        if ((col[j].offset + col[j].size > size) || (col[j].size == 0)) {
            /* Column extends beyond end of row or has zero size. */
            errno = EINVAL;
            goto fail;
        }
        for (i = 0; i < col[j].size; i++) {
            if (fake_row[col[j].offset + i]++) {
                /* Column overlaps with another. */
                errno = EINVAL;
                goto fail;
            }
        }
    }

    /*
     * Allocate table handle.
     */
    tmtable = (TMTABLE)calloc(sizeof(struct TMTABLE), 1);
    if (tmtable == NULL) {
        /* Memory exhaustion. */
        goto out;
    }
    tmtable->col = (TMCOL)calloc(count, sizeof(struct TMCOL));
    if (tmtable->col == NULL) {
        /* Memory exhaustion. */
        goto fail;
    }
    memcpy(tmtable->col, col, sizeof(struct TMCOL) * count);
    tmtable->col_count = count;
    for (unsigned i = 0; i < tmtable->col_count; i++) {
        tmtable->col[i].name = strdup(tmtable->col[i].name);
        if (tmtable->col[i].name == NULL) {
            /*
             * Hacktastic: set col_count so that the failure code will
             * only free the names we have already duplicated.
             */
            tmtable->col_count = i;
            goto fail;
        }
    }
    tmtable->stat = stat;
    tmtable->tableid = tmidx_add(&stat->table_idx, tmtable);
    tmtable->rowsz = size;
    tmidx_init(&tmtable->avail_idx);
    if (tmstat_pull_key_cols(tmtable) != 0) {
        goto fail;
    }

    /*
     * Manually allocate and construct table-descriptor row.  We might
     * be registering the table-descriptor table here, in which case we
     * can't simply use tmstat_row_add() because not all of the indexing
     * metadata is ready yet (in this case, tdtable == tmtable).
     */
    tdtable = (TMTABLE)tmidx_entry(&stat->table_idx, TM_ID_TABLE);
    ret = tmstat_row_alloc(stat, tdtable, &inode, &tmtable->td);
    if (ret != 0) {
        /* Allocation faliure; tmstat_row_create sets errno. */
        goto fail;
    }
    memset(tmtable->td, 0, sizeof(struct tmstat_table));
    snprintf(tmtable->td->name, sizeof(tmtable->td->name), name);
    tmtable->td->rowsz = size;
    tmtable->td->is_sorted = false;
    tmtable->td->cols = count;
    tmtable->td->tableid = tmtable->tableid;
    tmtable->inode = &tmtable->td->inode;
    /* Insert into index. */
    ret = tmstat_row_insert(stat, tdtable, inode);
    if (ret != 0) {
        /* Insertion faliure; tmstat_row_insert sets errno. */
        goto fail2;
    }
    tdtable->td->rows++;

    /*
     * If we are registering the table-desciprtor table (the first
     * table to be allocated and thus id 0), the column table does
     * not exist; recurse to allocate it.
     */
    if (tmtable->tableid == TM_ID_TABLE) {
        tmtable->td->inode = TM_INODE(TM_INODE_SLAB(inode), TM_INODE_LEAF);
        ret = tmstat_table_register(stat, &coltable, ".column", NULL, 0,
            sizeof(struct tmstat_column));
        if (ret != 0) {
            /* Registration faliure; tmstat_table_register sets errno. */
            goto fail2;
        }
    } else {
        coltable = tmidx_entry(&stat->table_idx, TM_ID_COLUMN);
    }

    /*
     * Allocate column-descriptor rows.
     */
    ofs = 0;
    for (i = 0; i < count; i++) {
        if (col[i].type != TMSTAT_T_HIDDEN) {
            ret = tmstat_row_add(stat, coltable, &column, &colinode[i]);
            if (ret != 0) {
                /* Allocation faliure; tmstat_row_create sets errno. */
                goto fail3;
            }
            snprintf(column->name, sizeof(column->name), col[i].name);
            column->tableid = tmtable->tableid;
            column->type = col[i].type;
            column->rule = col[i].rule;
            column->offset = col[i].offset;
            column->size = col[i].size;
            if (col[i].rule != TMSTAT_R_KEY) {
                /* This table has merge instructions; enable merge pass. */
                tmtable->want_merge = true;
            }
        }
        ofs = col[i].offset + col[i].size;
    }
    goto out;

    /*
     * Deal with failure.
     */
fail3:
    for (unsigned n = 0; n < i; ++n) {
        tmstat_row_remove(stat, coltable, colinode[n]);
    }
fail2:
    tmstat_row_free(stat, tdtable, inode);
fail:
    if ((tmtable != NULL) && (tmtable->col != NULL)) {
        for (i = 0; i < tmtable->col_count; i++) {
            free(tmtable->col[i].name);
        }
        free(tmtable->col);
    }
    free(tmtable);
    tmtable = NULL;

    /*
     * Done.
     */
out:
    if (table != NULL) {
        *table = tmtable;
    }
    return (tmtable != NULL) ? 0 : -1;
}

/**
 * Locate table by name.
 *
 * (This is a private interface).
 */
TMTABLE
tmstat_table(TMSTAT stat, char *table_name)
{
    TMTABLE             table;

    TMIDX_FOREACH(&stat->table_idx, table) {
        if (strcmp(table->td->name, table_name) == 0) {
            return table;
        }
    }
    return NULL;
}

/*
 * Obtain format information for a row.
 */
void
tmstat_row_description(TMROW row, TMCOL *col_out, unsigned *col_count_out)
{
    *col_out = row->table->col;
    *col_count_out = row->table->col_count;
}

/*
 * Is the table sorted?
 */ 
int
tmstat_is_table_sorted(TMSTAT stat, char *table_name)
{
    TMTABLE         table = tmstat_table(stat, table_name);
    
    if (table != NULL) {
        return table->td->is_sorted;
    } else {
        return false;
    }
}

/*
 * Obtain the column metadata for table.
 */
void
tmstat_table_info(TMSTAT tmstat, char *table_name,
                  TMCOL *cols, unsigned *col_count)
{
    TMTABLE         table = tmstat_table(tmstat, table_name);

    if (table != NULL) {
        *cols = table->col;
        *col_count = table->col_count;
    } else {
        *cols = NULL;
        *col_count = 0;
    }
}

/*
 * Obtain the row size for a table.
 */
void
tmstat_table_row_size(TMSTAT tmstat, char *table_name, unsigned *rowsz)
{
    TMTABLE         table = tmstat_table(tmstat, table_name);

    if (table != NULL) {
        *rowsz = table->rowsz;
    } else {
        *rowsz = 0;
    }
}

/*
 * Obtain a table's name.
 */
char *
tmstat_table_name(TMTABLE table)
{
    return table->td->name;
}

/*
 * Create a pseudo row.  Note that these must be compatible with
 * tmstat_row_drop.
 */
int
tmstat_pseudo_row_create(TMTABLE table, TMROW *row)
{
    TMROW           r;

    /* Allocate row handle. */
    r = (TMROW)malloc(ROUND_UP(sizeof(struct TMROW), ROW_ALIGN) + table->rowsz);
    if (r == NULL) {
        /* Allocation failure; malloc sets errno. */
        return -1;
    }
    r->own_row = false;
    r->inode_addr = -1;
    r->table = table;
    r->data = (uint8_t*)r + ROUND_UP(sizeof(struct TMROW), ROW_ALIGN);
    r->ref_count = 1;
    /* Insert into table's row list. */
    LIST_INSERT_HEAD(&table->row_list, r, entry);
    *row = r;
    return 0;
}

/*
 * Create a new row.
 */
int
tmstat_row_create(TMSTAT stat, TMTABLE table, TMROW *row)
{
    TMROW           r;
    signed          ret;

    /* Allocate row handle. */
    r = (TMROW)malloc(sizeof(struct TMROW));
    if (r == NULL) {
        /* Allocation failure; malloc sets errno. */
        ret = -1;
        goto out;
    }
    r->ref_count = 1;
    r->table = table;
    r->own_row = true;
    /* Add row to table. */
    ret = tmstat_row_add(stat, table, &r->data, &r->inode_addr);
    if (ret != 0) {
        /* Allocation failure; tmstat_row_add sets errno. */
        free(r);
        r = NULL;
        goto out;
    }
    /* Insert into table's row list. */
    LIST_INSERT_HEAD(&table->row_list, r, entry);
out:
    *row = r;
    return ret;
}

/*
 * Create n new rows.
 */
int
tmstat_row_create_n(TMSTAT stat, TMTABLE table, TMROW *row, unsigned n)
{
    int     i;
    int     ret;
    int     errno_save;
    
    for (i = 0; i < n; ++i) {
        /* Allocate row handle. */
        row[i] = (TMROW)malloc(sizeof(struct TMROW));
        if (row[i] == NULL) {
            /* Allocation failure; malloc sets errno. */
            errno_save = errno;
            goto fail;
        }
        row[i]->ref_count = 1;
        row[i]->table = table;
        row[i]->own_row = true;
        /* Insert into table's row list. */
        LIST_INSERT_HEAD(&table->row_list, row[i], entry);
    }
    ret = tmstat_row_add_n(stat, table, row, n);
    if (ret != 0) {
        /* Allocation failure; tmstat_row_add_n sets errno. */
        /* XXX Leak added rows. */
        errno_save = errno;
        goto fail;
    }
    return 0;
 fail:
    for (n = i, i = 0; i < n; ++i) {
        LIST_REMOVE(row[i], entry);
        free(row[i]);
    }
    errno = errno_save;
    return -1;
}

/*
 * Cause a row not to be removed from its table when it is deallocated.
 */
void
tmstat_row_preserve(TMROW row)
{
    row->own_row = false;
}

/*
 * Obtain extra reference to a row.
 */
TMROW
tmstat_row_ref(TMROW row)
{
    row->ref_count++;
    return row;
}

/*
 * Drop reference to a row.  Note that this must work for both real
 * and pseudo rows.
 */
TMROW
tmstat_row_drop(TMROW row)
{
    signed          ret;

    if (row->ref_count < 1) {
        /* You have a bug in your program.  Let us help you find it. */
        warnx("%s: use after free detected", __func__);
        abort();
    }
    row->ref_count--;
    if (row->ref_count == 0) {
        /* Free the resources for this row. */
        if (row->own_row) {
            /* Remove the row from its table. */
            ret = tmstat_row_free(row->table->stat, row->table,
                                  row->inode_addr);
            if (ret != 0) {
                /*
                 * In this case, tmstat_row_free has failed to mark a
                 * slab as available because memory could not be
                 * allocated to add the slab to the list of available
                 * slabs.  Choose to warn and leak instead of fail
                 * unexpectedly.
                 */
                warn("%s: row leaked due to internal allocation failure",
                     __func__);
            }
            ret = tmstat_row_remove(row->table->stat, row->table,
                                    row->inode_addr);
            if (ret != 0) {
                /*
                 * This means that the row was not found in the
                 * table.  Something is very much amiss.  There is
                 * nothing we can do about it.  Because this
                 * function's return value is constant, we cannot
                 * relate this fact to callers to allow them to choose
                 * what to do; furthermore, since this means that
                 * there must be a bug somewhere, it may be better not
                 * to rely on the caller to check the return value and
                 * do something appropriate.  For the purpose of
                 * obtaining diagnostic information, we abort.
                 */
                warn("%s: attempt to remove invalid row", __func__);
                abort();
            }
        }
        LIST_REMOVE(row, entry);
        free(row);
    }
    return NULL;
}

/*
 * Locate field within row.
 */
int
tmstat_row_field(TMROW row, char *name, void *p)
{
    TMCOL               col;
    unsigned            i;

    if (name == NULL) {
        /* Return the base of the structure. */
        *(void **)p = row->data;
        return 0;
    }
    col = row->table->col;
    for (i = 0; i < row->table->col_count; i++) {
        if (strcmp(col[i].name, name) == 0) {
            /* Return this field. */
            *(void **)p = &row->data[col[i].offset];
            return 0;
        }
    }
    errno = ENOENT;
    return -1;
}

/*
 * Return row's field's signed value.
 */
signed long long
tmstat_row_field_signed(TMROW row, char *name)
{
    TMCOL               col;
    unsigned            i;
    void               *p;

    col = row->table->col;
    for (i = 0; i < row->table->col_count; i++) {
        if (strcmp(col[i].name, name) == 0) {
            /* Return this field. */
            p = &row->data[col[i].offset];
            switch (col[i].type) {
            case TMSTAT_T_SIGNED:
                switch (col[i].size) {
                case 1:     return (signed long long)*(int8_t *)p;
                case 2:     return (signed long long)*(int16_t *)p;
                case 4:     return (signed long long)*(int32_t *)p;
                case 8:     return (signed long long)*(int64_t *)p;
                default:    return 0;
                }
            case TMSTAT_T_UNSIGNED:
                switch (col[i].size) {
                case 1:     return (signed long long)*(uint8_t *)p;
                case 2:     return (signed long long)*(uint16_t *)p;
                case 4:     return (signed long long)*(uint32_t *)p;
                case 8:     return (signed long long)*(uint64_t *)p;
                default:    return 0;
                }
            default:
                return 0;
            }
        }
    }
    return 0;
}

/*
 * Return row's field's unsigned value.
 */
unsigned long long
tmstat_row_field_unsigned(TMROW row, char *name)
{
    TMCOL               col;
    unsigned            i;
    void               *p;

    col = row->table->col;
    for (i = 0; i < row->table->col_count; i++) {
        if (strcmp(col[i].name, name) == 0) {
            /* Return this field. */
            p = &row->data[col[i].offset];
            switch (col[i].type) {
            case TMSTAT_T_SIGNED:
                switch (col[i].size) {
                case 1:     return (unsigned long long)*(int8_t *)p;
                case 2:     return (unsigned long long)*(int16_t *)p;
                case 4:     return (unsigned long long)*(int32_t *)p;
                case 8:     return (unsigned long long)*(int64_t *)p;
                default:    return 0;
                }
            case TMSTAT_T_UNSIGNED:
                switch (col[i].size) {
                case 1:     return (unsigned long long)*(uint8_t *)p;
                case 2:     return (unsigned long long)*(uint16_t *)p;
                case 4:     return (unsigned long long)*(uint32_t *)p;
                case 8:     return (unsigned long long)*(uint64_t *)p;
                default:    return 0;
                }
            default:
                return 0;
            }
        }
    }
    return 0;
}

/*
 * Obtain the column metadata for table.
 */
void
tmstat_row_info(TMROW row, TMCOL *cols, unsigned *col_count)
{
    *cols = row->table->col;
    *col_count = row->table->col_count;
}

const char *
tmstat_row_table(TMROW row)
{
    return row->table->td->name;
}

/*
 * Create a row that refers to an existing row and insert the row
 * into an index. The row must be freed w/ row_drop
 */ 
int
tmstat_alloc_weak_ref_row(TMTABLE table, struct tmidx *rows, uint8_t *row,
                            struct tmstat_slab *slab, unsigned rowno)
{
    TMROW                   tmrow;
    signed                  ret; 

    tmrow = (TMROW)malloc(sizeof(struct TMROW));
    if (tmrow == NULL) {
        return -1;
    }
    tmrow->ref_count = 1;
    tmrow->table = table;
    tmrow->data = row;
    tmrow->inode_addr = TM_INODE(TM_INODE_SLAB(slab->inode), rowno);
    tmrow->own_row = false;
    /* Add to index. */
    ret = tmidx_add(rows, tmrow);
    if (ret == -1) {
        tmstat_row_drop(tmrow);
        return -1;
    }
    /* Insert into table's row list. */
    LIST_INSERT_HEAD(&table->row_list, tmrow, entry);
    return 0;
}

/*
 * Locate rows by column values within slab.
 *
 * @param[in]   table       Associated table.
 * @param[out]  rows        Array containing result rows.
 * @param[in]   slab        Slab to search.
 * @param[in]   col_count   Number of columns to key on.
 * @param[in]   col         Columns to key upon.
 * @param[in]   value       Column values to match.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_query_slab(TMTABLE table, struct tmidx *rows, struct tmstat_slab *slab,
                  unsigned col_count, TMCOL *col, void **value)
{
    unsigned                rowno;
    uint8_t                *row;
    signed                  match, ret;

    TMSTAT_SLAB_FOREACH(table->stat, slab, rowno, row) {
        /* Consider this row. */
        for (unsigned i = 0; i < col_count; i++) {
            if (col[i]->type == TMSTAT_T_TEXT) {
                match = strncmp((const char*)value[i],
                                (const char*)&row[col[i]->offset],
                                col[i]->size-1);
            } else {
                match = memcmp(value[i], &row[col[i]->offset], col[i]->size);
            }
            if (match != 0) {
                /* Key mismatch; reject row. */
                goto next_row;
            }
        }
        /* Row match; allocate row handle. */
        ret = tmstat_alloc_weak_ref_row(table, rows, row, slab, rowno);
        if (ret == -1) {
            return -1;
        }
     
next_row: ;
    }
    return 0;
}

/*
 * Compare two rows.  In readers, this function is called often; most
 * of the runtime spent in this library will be spent inside this
 * function.  It is, therefore, performance-critical.
 *
 * @param[in]   r1      Handle to first row.
 * @param[in]   r2      Handle to second row.
 * @return 0 on match, positive if r1 > r2, negative if r1 < r2.
 */
static int64_t
tmstat_row_cmp(TMROW r1, TMROW r2)
{
    TMCOL       col;
    unsigned    i, col_count;
    int64_t     match;

    col = r1->table->key_col;
    col_count = r1->table->key_col_count;
    for (i = 0; i < col_count; i++) {
        switch (col[i].type) {
        case TMSTAT_T_SIGNED:
            switch (col[i].size) {
            case 1:
                match = (int64_t)*(int8_t *)(&r1->data[col[i].offset]) -
                        (int64_t)*(int8_t *)(&r2->data[col[i].offset]);
                break;
            case 2:
                match = (int64_t)*(int16_t *)(&r1->data[col[i].offset]) -
                        (int64_t)*(int16_t *)(&r2->data[col[i].offset]);
                break;
            case 4:
                match = (int64_t)*(int32_t *)(&r1->data[col[i].offset]) -
                        (int64_t)*(int32_t *)(&r2->data[col[i].offset]);
                break;
            case 8: 
                match = (*(int64_t *)(&r1->data[col[i].offset]) >
                         *(int64_t *)(&r2->data[col[i].offset]))? 1 : (
                            (*(int64_t *)(&r1->data[col[i].offset]) <
                            *(int64_t *)(&r2->data[col[i].offset])) ? -1 : 0 );
                break;
            }
            break;
        case TMSTAT_T_UNSIGNED:
            switch (col[i].size) {
            case 1:
                match = (int64_t)*(uint8_t *)(&r1->data[col[i].offset]) -
                        (int64_t)*(uint8_t *)(&r2->data[col[i].offset]);
                break;
            case 2:
                match = (int64_t)*(uint16_t *)(&r1->data[col[i].offset]) -
                        (int64_t)*(uint16_t *)(&r2->data[col[i].offset]);
                break;
            case 4:
                match = (int64_t)*(uint32_t *)(&r1->data[col[i].offset]) -
                        (int64_t)*(uint32_t *)(&r2->data[col[i].offset]);
                break;
            case 8:
                match = (*(uint64_t *)(&r1->data[col[i].offset]) >
                         *(uint64_t *)(&r2->data[col[i].offset]))? 1 : (
                            (*(uint64_t *)(&r1->data[col[i].offset]) <
                            *(uint64_t *)(&r2->data[col[i].offset])) ? -1 : 0 );
                break;
            }
            break;
        case TMSTAT_T_TEXT:
            match = strncmp((char*)&r1->data[col[i].offset], (char*)&r2->data[col[i].offset],
                           col[i].size-1);
            break;
        default:
            match = memcmp(&r1->data[col[i].offset], &r2->data[col[i].offset],
                           col[i].size);
        }
        if (match != 0) {
            return match;
        }
    }
    return 0;
}

/**
 * Locate rows by column values within table.
 *
 * @param[in]   stat        Segment to search.
 * @param[in]   table       Table to search.
 * @param[out]  rows        Array containing result row indexes.
 * @param[in]   col_count   Number of columns to key on.
 * @param[in]   col_name    Column names to key upon.
 * @param[in]   values      Column values to match.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_query_table(TMSTAT stat, TMTABLE table, struct tmidx *rows,
                   unsigned col_count, char **col_name, void **values)
{
    struct tmidx            slabs;
    struct tmstat_slab     *slab;
    signed                  ret;
    TMCOL                   cols[col_count];
    bool                    all_keys = col_count > 0;
    TMROW                   row, srow;
    struct TMROW            srowd;

    /*
     * Do this before touching any of the autos, since cols may
     * cause a stack overflow.
     */
    if (col_count > table->col_count) {
        errno = EINVAL;
        return -1;
    }
    
    /* Allocate a pseudo row for doing fast searches. */
    if (table->td->is_sorted) {
        ret = tmstat_pseudo_row_create(table, &row);
        if (ret != 0) {
            /* tmstat_pseudo_row_create sets errno. */
            return ret;
        }
        srow = &srowd;
        srow->table = table;
    }
    tmidx_init(&slabs);
    /* Locate columns. */
    for (unsigned i = 0; i < col_count; i++) {
        for (unsigned j = 0; j < table->col_count; j++) {
            if (strcmp(col_name[i], table->col[j].name) == 0) {
                cols[i] = &table->col[j];
                if (!table->td->is_sorted || (cols[i]->rule != TMSTAT_R_KEY)) {
                    all_keys = false;
                } else {
                    /* Populate the keys of the pseudo row. */
                    if (cols[i]->type == TMSTAT_T_TEXT) {
                        strncpy((char *)&row->data[cols[i]->offset], values[i],
                                cols[i]->size);
                    } else {
                        memcpy(&row->data[cols[i]->offset], values[i],
                               cols[i]->size);
                    }
                }
                goto next_col;
            }
        }
        /* Column does not exist; treat as if no rows match. */
        ret = 0;
        goto out;
next_col:
        ;
    }
    /* Locate slabs. */
    ret = tmstat_slab_idx(table->stat, table->td, &slabs);
    if (ret != 0) {
        /* tmstat_slab_idx sets errno. */
        goto out;
    }
    /*
     * If the table meets certain criteria it can be searched more quickly 
     * In this instance, there will be only one result. If it is not found
     * in the binary search, fallback to linear.
     */
    if (table->td->is_sorted && all_keys &&
            (col_count == table->key_col_count)) {
        int search_first = 0;
        int search_last = tmidx_count(&slabs) - 1;
        int search_idx;
        unsigned rowno;
        int64_t cmp;
        
        while (search_first <= search_last) {
            search_idx = (search_last - search_first) / 2 + search_first;
            slab = tmidx_entry(&slabs, search_idx);
            srow->data = tmstat_slab_first(stat, slab, &rowno);
            cmp = tmstat_row_cmp(row, srow);
            if (cmp < 0) {
                search_last = search_idx - 1;
                continue;
            } else if (cmp == 0) {
                /* The only match was found. */
                ret = tmstat_alloc_weak_ref_row(table, rows, srow->data, slab,
                                                rowno);
                goto out;
            }
            srow->data = tmstat_slab_last(stat, slab, &rowno);
            cmp = tmstat_row_cmp(row, srow);
            if (cmp > 0) {
                search_first = search_idx + 1;
                continue;
            } else if (cmp == 0) {
                /* The only match was found. */
                ret = tmstat_alloc_weak_ref_row(table, rows, srow->data, slab,
                                                rowno);
                goto out;
            }
            /* It should be in this slab. */
            ret = tmstat_query_slab(table, rows, slab, col_count, cols, values);
            goto out;
        }
        ret = 0;
        goto out;
    } else {
        TMIDX_FOREACH(&slabs, slab) {
            ret = tmstat_query_slab(table, rows, slab, col_count, cols, values);
            if (ret != 0) {
                /* Internal error; tmstat_query_slab sets errno. */
                goto out;
            }
        }
    }
out:
    if (table->td->is_sorted) {
        tmstat_row_drop(row);
    }
    tmidx_free(&slabs);
    return ret;
}

/**
 * Merge row fields.
 *
 * @param[in]   dst_row     Target row and result.
 * @param[in]   src_row     Source row.
 * @return 0 on success, -1 on failure.
 */
int
tmstat_merge_row(TMROW dst_row, TMROW src_row)
{
    TMTABLE         table = dst_row->table;
    TMCOL           col;
    signed          match;
    void           *a, *b;
    int             ret = 0;

    for (unsigned i = 0; i < table->col_count; i++) {
        col = &table->col[i];
        a = (void *)&dst_row->data[col->offset];
        b = (void *)&src_row->data[col->offset];
        switch (col->rule) {
        case TMSTAT_R_KEY:
            /* This column stores row identity; ignore. */
            break;
        case TMSTAT_R_OR:
            /* Logical or (useful for bit sets). */
            for (unsigned j = col->offset; j < col->offset + col->size; j++) {
                dst_row->data[j] |= src_row->data[j];
            }
            break;
        case TMSTAT_R_SUM:
            /* Sum fields. */
            switch (col->size) {
            case sizeof(uint8_t):   *(uint8_t *)a += *(uint8_t *)b;     break;
            case sizeof(uint16_t):  *(uint16_t *)a += *(uint16_t *)b;   break;
            case sizeof(uint32_t):  *(uint32_t *)a += *(uint32_t *)b;   break;
            case sizeof(uint64_t):  *(uint64_t *)a += *(uint64_t *)b;   break;
            default:
                warnx("%s(%d): unsupported size %d for column `%s' in"
                      " table `%s' with sum rule",
                      __func__, __LINE__, col->name, table->td->name);
                errno = EINVAL;
                ret = -1;
                break;
            }
            break;
        case TMSTAT_R_MIN:
            /* Select smallest. */
            if (col->type == TMSTAT_T_UNSIGNED) {
                switch (col->size) {
                case sizeof(int8_t):
                    *(uint8_t *)a = TMSTAT_MIN(*(uint8_t *)a, *(uint8_t *)b);
                    break;
                case sizeof(int16_t):
                    *(uint16_t *)a = TMSTAT_MIN(*(uint16_t *)a, *(uint16_t *)b);
                    break;
                case sizeof(int32_t):
                    *(uint32_t *)a = TMSTAT_MIN(*(uint32_t *)a, *(uint32_t *)b);
                    break;
                case sizeof(int64_t):
                    *(uint64_t *)a = TMSTAT_MIN(*(uint64_t *)a, *(uint64_t *)b);
                    break;
                default:
                    warnx("%s(%d): unsupported size %d for column `%s' in"
                          " table `%s' with min rule",
                          __func__, __LINE__, col->name, table->td->name);
                    errno = EINVAL;
                    ret = -1;
                    break;
                }
            } else if (col->type == TMSTAT_T_SIGNED) {
                switch (col->size) {
                case sizeof(int8_t):
                    *(int8_t *)a = TMSTAT_MIN(*(int8_t *)a, *(int8_t *)b);
                    break;
                case sizeof(int16_t):
                    *(int16_t *)a = TMSTAT_MIN(*(int16_t *)a, *(int16_t *)b);
                    break;
                case sizeof(int32_t):
                    *(int32_t *)a = TMSTAT_MIN(*(int32_t *)a, *(int32_t *)b);
                    break;
                case sizeof(int64_t):
                    *(int64_t *)a = TMSTAT_MIN(*(int64_t *)a, *(int64_t *)b);
                    break;
                default:
                    warnx("%s(%d): unsupported size %d for column `%s' in"
                          " table `%s' with min rule",
                          __func__, __LINE__, col->name, table->td->name);
                    errno = EINVAL;
                    ret = -1;
                    break;
                }
            } else {
                match = memcmp(a, b, col->size);
                if (match > 0) {
                    memcpy(a, b, col->size);
                }
            }
            break;
        case TMSTAT_R_MAX:
            /* Select largest. */
            if (col->type == TMSTAT_T_UNSIGNED) {
                switch (col->size) {
                case sizeof(uint8_t):
                    *(uint8_t *)a = TMSTAT_MAX(*(uint8_t *)a, *(uint8_t *)b);
                    break;
                case sizeof(uint16_t):
                    *(uint16_t *)a = TMSTAT_MAX(*(uint16_t *)a, *(uint16_t *)b);
                    break;
                case sizeof(uint32_t):
                    *(uint32_t *)a = TMSTAT_MAX(*(uint32_t *)a, *(uint32_t *)b);
                    break;
                case sizeof(uint64_t):
                    *(uint64_t *)a = TMSTAT_MAX(*(uint64_t *)a, *(uint64_t *)b);
                    break;
                default:
                    warnx("%s(%d): unsupported size %d for column `%s' in"
                          " table `%s' with max rule",
                          __func__, __LINE__, col->name, table->td->name);
                    errno = EINVAL;
                    ret = -1;
                    break;
                }
            } else if (col->type == TMSTAT_T_SIGNED) {
                switch (col->size) {
                case sizeof(int8_t):
                    *(int8_t *)a = TMSTAT_MAX(*(int8_t *)a, *(int8_t *)b);
                    break;
                case sizeof(int16_t):
                    *(int16_t *)a = TMSTAT_MAX(*(int16_t *)a, *(int16_t *)b);
                    break;
                case sizeof(int32_t):
                    *(int32_t *)a = TMSTAT_MAX(*(int32_t *)a, *(int32_t *)b);
                    break;
                case sizeof(int64_t):
                    *(int64_t *)a = TMSTAT_MAX(*(int64_t *)a, *(int64_t *)b);
                    break;
                default:
                    warnx("%s(%d): unsupported size %d for column `%s' in"
                          " table `%s' with max rule",
                          __func__, __LINE__, col->name, table->td->name);
                    errno = EINVAL;
                    ret = -1;
                    break;
                }
            } else {
                match = memcmp(a, b, col->size);
                if (match < 0) {
                    memcpy(a, b, col->size);
                }
            }
            break;
        }
    }
    return ret;
}

/**
 * Produce merged result set.
 *
 * This is a common operation for readers, so it is performance
 * critical.  It is assumed that the incoming rows are unsorted (which
 * is almost always the case), so we do what we can to reduce merge
 * time via a red-black tree and obtain something like O(n log n).
 *
 * @param[in]   table       Table whose rows we are merging.
 * @param       rows        On entry, contains source rows.
 *                          On return, contains merged rows.
 *                          Regardless of success, the original index is freed
 *                          and all the original rows are dropped.
 *                          The returned rows are pseudo rows.
 * @param[out]  treep       If non-null, on success it will point to
 *                          a red-black tree containing all the merged
 *                          rows.  On failure, its contents are unchanged.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_merge_rows(TMTABLE table, struct tmidx *rows, tmrbt *treep)
{
    struct tmidx    src;
    TMROW           src_row, row;
    signed          ret = 0;
    tmrbt           tree;
    unsigned        i;

    tree = tmrbt_alloc();
    if (tree == NULL) {
        return -1;
    }
    /* Move results into source index. */
    memcpy(&src, rows, sizeof(struct tmidx));
    tmidx_init(rows);
    /* Copy into results, merging as we find identical entries. */
    ret = 0;
    for (i = 0; i < tmidx_count(&src); i++) {
        src_row = tmidx_entry(&src, i);
        /* Look for row with identical key. */
        row = tmrbt_find(tree, src_row);
        if (row) {
            /* Found.  Merge rows. */
           tmstat_merge_row(row, src_row);
        } else {
            /* Not found.  Create a new pseudo row, copy data, insert. */
            ret = tmstat_pseudo_row_create(table, &row);
            if (ret < 0) {
                break;
            }
            memcpy(row->data, src_row->data, table->rowsz);
            ret = tmrbt_insert(tree, row);
            if (ret < 0) {
                tmstat_row_drop(row);
                break;
            }
            ret = tmidx_add(rows, row);
            ret = (ret < 0) ? -1 : 0;
            if (ret < 0) {
                tmstat_row_drop(row);
                break;
            }
        }
        /* Free source row. */
        tmstat_row_drop(src_row);
    }
    /* Free any unprocessed source rows in the event of an error. */
    for (; i < tmidx_count(&src); i++) {
        tmstat_row_drop(tmidx_entry(&src, i));
    }
    tmidx_free(&src);
    if (ret != 0) {
        /* Failure.  Clean up partial results. */
        TMIDX_FOREACH(rows, row) {
            tmstat_row_drop(row);
        }
        tmidx_free(rows);
        tmidx_init(rows);
    }
    /* Save tree if caller asked for it and all went well; otherwise, free. */
    if ((ret == 0) && (treep != NULL)) {
        *treep = tree;
    } else {
        tmrbt_free(tree);
    }
    return ret;
}

/**
 * Produce merged result set from child query.
 *
 * @param[in]   stat        Associated segment.
 * @param[in]   table_name  Table name to put the merged rows into.
 * @param[in]   rows        Array containing source row indexes.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_query_rollup_merge(TMSTAT stat, char *table_name, struct tmidx *rows)
{
    struct tmidx    src;
    TMROW           src_row, row;
    TMTABLE         table;
    signed          idx, ret;

    if (tmidx_count(rows) < 2) {
        /* No need to merge. */
        return 0;
    }

    /* Find the table. */
    table = tmstat_table(stat, table_name);
    if (table == NULL) {
        /* If not found, we must still free the source rows. */
        errno = EINVAL;
        ret = -1;
    } else {
        ret = 0;
    }
    /* Move results into source index. */
    memcpy(&src, rows, sizeof(struct tmidx));
    tmidx_init(rows);
    /* Copy into results, merging as we find identical entries. */
    TMIDX_FOREACH(&src, src_row) {
        if (ret == -1) {
            /* We had a problem; just free. */
            goto next_row;
        }
        /*
         * We are ignoring keys, because this was called
         * from tmstat_query_rollup which means the caller
         * wants all rows merged, regardless of keys.
         */
        TMIDX_FOREACH(rows, row) {
            /* Merge rows. */
            ret = tmstat_merge_row(row, src_row);
            goto next_row;
        }
        /* Create new row. */
        ret = tmstat_row_create(stat, table, &row);
        if (ret == -1) {
            /* Creation error; continue but report error upon completion. */
            goto next_row;
        }
        memcpy(row->data, src_row->data, table->rowsz);
        idx = tmidx_add(rows, row);
        if (idx == -1) {
            /* Insertion error; continue but report error upon completion. */
            tmstat_row_drop(row);
            ret = -1;
            goto next_row;
        }
next_row:
        tmstat_row_drop(src_row);
    }
    tmidx_free(&src);
    return ret;
}

/**
 * Locate rows by column values among children segments.
 *
 * @param[in]   stat        Segment to search.
 * @param[out]  rows        Array containing result row indexes.
 * @param[in]   table_name  Table name.
 * @param[in]   col_count   Number of columns to key on.
 * @param[in]   col_name    Column names to key upon.
 * @param[in]   col_value   Column values to match.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_query_children(TMSTAT stat, struct tmidx *rows, char *table_name,
                      unsigned col_count, char **col_name, void **col_value)
{
    TMSTAT          child;
    TMTABLE         table;
    signed          ret = 0;

    /* Locate rows. */
    TMIDX_FOREACH(&stat->child_idx, child) {
        table = tmstat_table(child, table_name);
        if (table == NULL) {
            /* Table doesn't exist in child; try other children. */
            continue;
        }
        ret = tmstat_query_table(stat, table, rows,
                                 col_count, col_name, col_value);
        if (ret == -1) {
            /* Internal failure; tmstat_query_table sets errno. */
            goto out;
        }
    }
out:
    return ret;
}

static bool tmstat_is_unreferenced(TMSTAT);

/*
 * Returne true iff no one is holding references to TMROWs that point
 * to any of the slabs for this segment itself (ignores children).
 */
static bool
tmstat_is_directly_unreferenced(TMSTAT stat)
{
    TMTABLE         table;

    TMIDX_FOREACH(&stat->table_idx, table) {
        if (table->row_list.lh_first != NULL) {
            return false;
        }
    }
    return true;
}

/*
 * Return true iff no one is holding references to TMROWs that point
 * to any of the slabs for any of the tables in this segment's
 * children.
 */
static bool
tmstat_children_are_unreferenced(TMSTAT stat)
{
    TMSTAT          child;

    TMIDX_FOREACH(&stat->child_idx, child) {
        if (!tmstat_is_unreferenced(child)) {
            return false;
        }
    }
    return true;
}

/*
 * Return true iff no one is holding references to TMROWs that point
 * to any of the slabs for any of the tables in this segment or any of
 * its children.
 */
static bool
tmstat_is_unreferenced(TMSTAT stat)
{
    return (tmstat_is_directly_unreferenced(stat) &&
            tmstat_children_are_unreferenced(stat));
}

/*
 * Return true iff the directory to which stat is subscribed has
 * changed since it was read.
 */
static bool
tmstat_is_subscription_unfresh(TMSTAT tmstat)
{
    int                 ret;
    struct stat         st;
    char                dir[PATH_MAX];

    snprintf(dir, sizeof(dir), "%s/%s", tmstat_path, tmstat->directory);
    ret = stat(dir, &st);
    if (ret != 0) {
        /* Cannot stat directory; pretend that all is well. */
        return false;
    }
    return ((st.st_ctim.tv_sec > tmstat->ctime.tv_sec) ||
            ((st.st_ctim.tv_sec == tmstat->ctime.tv_sec) &&
             (st.st_ctim.tv_nsec > tmstat->ctime.tv_nsec)));
}

/*
 * Reread the files in the directory to which stat is subscribed.
 */
static void
tmstat_freshen_subscription(TMSTAT stat)
{
    struct TMSTAT       old;
    struct TMSTAT       new;
    int                 ret;

    /*
     * This is a bit tricky because all our calls must use the address
     * passed in stat but we need briefly to maintain multiple
     * functional identities (aka contents of the struct, aka "guts")
     * assocated with that address.
     */
    /* Save the old guts. */
    memcpy(&old, stat, sizeof(struct TMSTAT));
    /* Re-subscribe, creating new guts. */
    ret = _tmstat_subscribe(stat, old.directory);
    /* Save new guts (or maybe nothing if we failed). */
    memcpy(&new, stat, sizeof(struct TMSTAT));
    /*
     * Replace the old guts in preparation for either returning things
     * to the user unmodified or freeing the old guts.
     */
    memcpy(stat, &old, sizeof(struct TMSTAT));
    if (ret != 0) {
        /* We failed.  Return old guts. */
        return;
    }
    /* Success!  Free the old guts. */
    _tmstat_dealloc(stat);
    /* Swap in the new guts. */
    memcpy(stat, &new, sizeof(struct TMSTAT));
}

/**
 * Reread data if possible and appropriate.  If force is true, the
 * data will be reread even if they appear to be up-to-date.
 * If this fails, it does so silently and leaves stat untouched.
 *
 * @param[in]   stat        Stat handle to refresh.
 * @param[in]   force       Force refresh?
 */
void
tmstat_refresh(TMSTAT stat, int force)
{
    switch (stat->origin) {
    case SUBSCRIBE:
        if (tmstat_is_unreferenced(stat) &&
            ((force != 0) || tmstat_is_subscription_unfresh(stat))) {
            tmstat_freshen_subscription(stat);
        }
        break;
    default:
        break;
    }
}

/*
 * Locate rows by column values.
 */
static int
_tmstat_query(TMSTAT stat, struct tmidx *rows, char *table_name,
              unsigned col_count, char **col_name, void **col_value)
{
    TMTABLE             table;
    signed              ret;

    if (stat->origin != CREATE) {
        /* We are querying the child(ren), not this segment itself. */
        ret = tmstat_query_children(stat, rows, table_name,
            col_count, col_name, col_value);
    } else {
        /* This segment is the one we're interested in. */
        table = tmstat_table(stat, table_name);
        if (table == NULL) {
            /* No matching table; treat as if the table were empty. */
            ret = 0;
        } else {
            /* Locate rows. */
            ret = tmstat_query_table(stat, table, rows, col_count,
                                     col_name, col_value);
        }
    }
    return ret;
}

/*
 * Locate rows by column values.
 */
int
tmstat_query(TMSTAT stat, char *table_name,
             unsigned col_count, char **col_name, void **col_value,
             TMROW **row_handle, unsigned *match_count)
{
    struct tmidx        rows;
    TMROW               row;
    TMTABLE             table;
    signed              ret;

    *match_count = 0;
    if (row_handle != NULL) {
        *row_handle = NULL;
    }
    tmstat_refresh(stat, false);
    tmidx_init(&rows);
    ret = _tmstat_query(stat, &rows, table_name, col_count,
                        col_name, col_value);
    if (ret != 0) {
        goto end;
    }
    table = tmstat_table(stat, table_name);
    if (table == NULL) {
        /* No matching table; treat as if the table were empty. */
        /* match_count and row_handle already cleared above. */
        ret = 0;
        goto end;
    }
    if (table->want_merge) {
        ret = tmstat_merge_rows(table, &rows, NULL);
    }
    if (ret != 0) {
        goto end;
    }
    *match_count = tmidx_count(&rows);
    if (row_handle == NULL) {
        /*
         * Caller doesn't care about result rows, only the count, so
         * free row handles.
         */
        TMIDX_FOREACH(&rows, row) {
            tmstat_row_drop(row);
        }
    } else {
        /* Allocate and populate result array. */
        *row_handle = (TMROW *)calloc(*match_count, sizeof(TMROW));
        if (*row_handle != NULL) {
            unsigned i = 0;
            TMIDX_FOREACH(&rows, row) {
                (*row_handle)[i++] = row;
            }
        } else {
            /* Out of memory.  Free row handles. */
            TMIDX_FOREACH(&rows, row) {
                tmstat_row_drop(row);
            }
            ret = -1;
        }
    }
end:
    tmidx_free(&rows);
    return ret;
}

/*
 * Locate rows by column values and rollup all values to one row.
 */
int
tmstat_query_rollup(TMSTAT stat, char *table_name,
                    unsigned col_count, char **col_name, void **col_value,
                    TMROW *row_handle)
{
    struct tmidx        rows;
    TMROW               row;
    TMTABLE             table;
    signed              ret;
    TMROW               src;

    tmstat_refresh(stat, false);
    *row_handle = NULL;
    tmidx_init(&rows);
    if (stat->origin != CREATE) {
        ret = tmstat_query_children(stat, &rows, table_name,
            col_count, col_name, col_value);
        if (ret != 0) {
            goto out;
        }
        goto merge;
    }
    /* Locate table. */
    table = tmstat_table(stat, table_name);
    if (table == NULL) {
        /* No matching table; treat as if the table were empty. */
        ret = 0;
        goto out;
    }
    /* Locate rows. */
    ret = tmstat_query_table(stat, table, &rows,
                             col_count, col_name, col_value);
    if (ret) {
        goto out;
    }

merge:
    /* Merge rows, but don't check indices. */
    ret = tmstat_query_rollup_merge(stat, table_name, &rows);

    /* Return results. */
    if (tmidx_count(&rows) != 1) {
        ret = -1;
    } else {
        /* Return result row. Increment ref counter. */
        src = TMIDX_ROW(&rows, 0);
        if (src == NULL) {
            ret = -1;
        } else {
            tmstat_row_ref(src);
            *row_handle = src;
        }
    }

out:
    TMIDX_FOREACH(&rows, row) {
        tmstat_row_drop(row);
    }
    tmidx_free(&rows);
    return ret;
}

/**
 * Perform an in-order tree walk of a tree of pseudo-rows, creating
 * real rows as we go, thereby producing sorted output.
 *
 * Regardless of success, the entire tree under this node will be
 * walked and the row at each node will be dropped.
 *
 * @param[in]   stat        Associated segment.
 * @param[in]   table       Table to put sorted rows into.
 * @param[out]  rows        Index to put sorted rows into.
 * @param       node        Tree node to walk; will be freed.
 * @param[in]   good        If false, node is freed and no other work is done.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_sort_walk(TMSTAT stat, TMTABLE table, struct tmidx *rows,
    tmrbt_node node, int good)
{
    signed idx = 0, ret = 0;
    TMROW row;
    
    if (node->link[0] != NULL) {
        ret = tmstat_sort_walk(stat, table, rows, node->link[0], good);
    }
    /*
     * Don't make new rows if everything isn't okay - but continue
     * to traverse to free the pseudo rows.
     */
    if (ret == -1 || good == -1) {
        ret = -1;
        goto out;
    }
    /* Create a real row in order. */
    ret = tmstat_row_create(stat, table, &row);
    if (ret == -1) {
        /* Creation error; continue but report error upon completion. */
        goto out;
    }
    memcpy(row->data, node->key->data, table->rowsz);
    idx = tmidx_add(rows, row);
    if (idx == -1) {
        /* Insertion error; continue but report error upon completion. */
        tmstat_row_drop(row);
        ret = -1;
        goto out;
    }
out:
    tmstat_row_drop(node->key);
    if (node->link[1] != NULL) {
        /* ret will always be -1 if good is -1 */
        ret = tmstat_sort_walk(stat, table, rows, node->link[1], ret);
    }
    return ret;
}

/**
 * Merge all the rows from child segments into a new table, writing
 * them in sorted order.
 *
 * @param[in]   stat        Associated segment.
 * @param[in]   table       Table to put the merged rows in.
 * @param[in]   rows        Source rows, which are freed.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_table_copy_rows(TMSTAT stat, TMTABLE table, struct tmidx *rows)
{
    signed          ret;
    tmrbt           tree;
    struct tmidx    dest;
    TMROW           row;

    /* We must own a segment if we're going to add rows to it. */
    if (stat->origin != CREATE) {
        errno = EINVAL;
        return -1;
    }
    /* Change page allocation policy to reduce frequency of mmap calls. */
    stat->alloc_policy = PREALLOCATE;
    /* Perform a merge.  This creates pseudo-rows and puts them in a tree. */
    ret = tmstat_merge_rows(table, rows, &tree);
    /* tmstat_merge_rows has freed our source rows no matter its success. */
    if (ret != 0) {
        goto out;
    }
    /* In any event, we don't need this index because we have the tree. */
    tmidx_free(rows);
    tmidx_init(rows);
    /* Walk tree in order, creating real rows and freeing pseudo-rows. */
    tmidx_init(&dest);
    ret = tmstat_sort_walk(stat, table, &dest, tree->root, 0);
    if (ret != 0) {
        /* tmstat_sort_walk freed source rows but not its partial results. */
        TMIDX_FOREACH(&dest, row) {
            tmstat_row_drop(row);
        }
        tmidx_free(&dest);
        goto out;
    }
    table->td->is_sorted = true;
    /* Clean up. Now that the rows are in the new table, we don't need them. */
    tmrbt_free(tree);
    TMIDX_FOREACH(&dest, row) {
        tmstat_row_preserve(row);
        tmstat_row_drop(row);
    }
    tmidx_free(&dest);
out:
    /* Don't need to prealloc anymore. */
    stat->alloc_policy = AS_NEEDED;
    return ret;
}

/**
 * Copy a table, producing sorted output in the destination.
 *
 * @param[in]   dest       Target segment.
 * @param[in]   src        Source segment.
 * @param[in]   name       Table name.
 * @return 0 on success, -1 on failure.
 */
static int
tmstat_table_copy(TMSTAT dest, TMSTAT src, char *table_name)
{
    TMCOL           col;
    TMROW           row;
    TMTABLE         table;
    unsigned        col_count, size;
    int             ret = 0;
    struct tmidx    rows;

    tmidx_init(&rows);

    if (table_name[0] == '.') {
        /* Skip internal tables; they can't be created. */
        goto out;
    }

    /* Obtain column information. */
    tmstat_table_info(src, table_name, &col, &col_count);
    if (col_count == 0) {
        /* Skip empty table. */
        goto out;
    }
    tmstat_table_row_size(src, table_name, &size);
    if (size == 0) {
        /* Skip empty table. */
        goto out;
    }
    
    /* Obtain all of the source rows. */
    ret = _tmstat_query(src, &rows, table_name, 0, NULL, NULL);
    if (ret != 0) {
        goto out;
    }
    if (tmidx_count(&rows) == 0) {
        /* Skip empty table. */
        goto out;
    }

    /* Register destination table. */
    ret = tmstat_table_register(dest, &table, table_name, col, col_count, size);
    if (ret != 0) {
        goto out;
    }

    /* Merge, sort, and copy the rows. */
    ret = tmstat_table_copy_rows(dest, table, &rows);
    if (ret != 0) {
        goto out;
    }

out:
    TMIDX_FOREACH(&rows, row) {
        tmstat_row_drop(row);
    }
    tmidx_free(&rows);

    return ret;
}

/**
 * Merge all tables into one segment file.
 *
 * @param[in]   stat        Source segment.
 * @param[in]   path        Path to output file.
 * @param[in]   merge       Only public tables or all.
 * @return 0 on success, -1 on failure.
*/
int
tmstat_merge(TMSTAT stat, char *path, enum tmstat_merge merge)
{
    TMSTAT                  dest = NULL;
    TMROW                  *label_row = NULL;
    unsigned                label_count = 0;
    TMROW                  *table_row = NULL;
    unsigned                table_count = 0;
    unsigned                i;
    int                     ret;
    char                   *name;
    TMTABLE                 labeltable;
    struct tmstat_label    *label, *child;
    char                    private_path[strlen(tmstat_path)+
                                         sizeof(TMSTAT_DIR_PRIVATE)+
                                         sizeof(basename(path))+4];

    snprintf(private_path, sizeof(private_path), "%s/%s", tmstat_path,
             TMSTAT_DIR_PRIVATE);
    mkdir(private_path, 0777);
    snprintf(private_path, sizeof(private_path), "%s/%s/%s", tmstat_path,
             TMSTAT_DIR_PRIVATE, basename(path));

    /* Create segment. */
    ret = tmstat_create(&dest, basename(path));
    if (ret != 0) {
        goto out;
    }
    /* Produce merged label table with pretty ASCII art. */
    ret = tmstat_query(stat, ".label", 0, NULL, NULL, &label_row, &label_count);
    if (ret != 0) {
        goto out;
    }
    labeltable = tmstat_table(dest, ".label");
    if (labeltable == NULL) {
        TMSTAT_SEGMENT_DAMAGED(stat);
        goto out;
    }
    for (i = 0; i < label_count; i++) {
        tmstat_row_field(label_row[i], NULL, &child);
        /* Allocate label. */
        ret = tmstat_row_add(dest, labeltable, &label, NULL);
        if (ret != 0) {
           /* Allocation failure; tmstat_row_create sets errno. */
           goto out;
        }
        /* Construct label. */
        snprintf(label->tree, sizeof(label->tree), TMSTAT_SEGMENT_HEADER);
        snprintf(label->name, sizeof(label->name), "%s", child->name);
        snprintf(label->ctime, sizeof(label->ctime), "%s", child->ctime);
        label->time = child->time;
    }
    /* Obtain list of tables. */
    ret = tmstat_query(stat, ".table", 0, NULL, NULL, &table_row, &table_count);
    if (ret != 0) {
        goto out;
    }
    /* Copy relevant tables. */
    for (unsigned i = 0; i < table_count; i++) {
        /* Fetch name. */
        ret = tmstat_row_field(table_row[i], "name", &name);
        if (ret != 0) {
            TMSTAT_SEGMENT_DAMAGED(stat);
            goto out;
        }
        if ((name[0] != '.') && (merge == TMSTAT_MERGE_ALL ||
                                 !tmstat_is_internal_name(name))) {
            /* Copy table. */
            ret = tmstat_table_copy(dest, stat, name);
            if (ret != 0) {
                goto out;
            }
        }
    }

out:
    for (unsigned i = 0; i < label_count; i++) {
        tmstat_row_drop(label_row[i]);
    }
    if (label_row != NULL) {
        free(label_row);
    }
    for (unsigned i = 0; i < table_count; i++) {
        tmstat_row_drop(table_row[i]);
    }
    if (table_row != NULL) {
        free(table_row);
    }
    if (ret == 0) {
        if (dest != NULL) {
            tmstat_dealloc(dest);
        }
        if (path != NULL) {
            /* Move the segment file to the user's destination. */
            rename(private_path, path);
        }
    } else if (dest != NULL) {
        tmstat_destroy(dest);
    }

    return ret;
}


/*
 * Printing and parsing.
 */

static int
tmstat_parse_bin(const char *in, char **endp, void *vout, unsigned size)
{
    uint8_t x;
    uint8_t *out = vout;
    unsigned j;
    unsigned i = 0;
    unsigned o = 0;
    unsigned len = strlen(in);

    if (len + 1 != 10 * size) {
        goto fail;
    }
    for (;;) {
        x = 0;
        for (j = 0; j < 4; ++j) {
            if (in[i] == '0' || in[i] == '1') {
                x = (x << 1) | (in[i] - '0');
                ++i;
            } else {
                goto fail;
            }
        }
        if (in[i] == ':') {
            ++i;
        } else {
            goto fail;
        }
        for (j = 0; j < 4; ++j) {
            if (in[i] == '0' || in[i] == '1') {
                x = (x << 1) | (in[i] - '0');
                ++i;
            } else {
                goto fail;
            }
        }
        out[o] = x;
        if (++o == size) {
            break;
        }
        if (in[i] == ' ') {
            ++i;
        } else {
            goto fail;
        }
    }
    if (endp != NULL) {
        *endp = (char *)in + i;
    }
    return 0;
 fail:
    if (endp != NULL) {
        *endp = (char *)in + i;
    }
    errno = EINVAL;
    return -1;
}

static int
tmstat_parse_dec(const char *in, char **endp, void *vout, unsigned size)
{
    uint8_t x;
    uint8_t *out = vout;
    unsigned i = 0;
    unsigned o = 0;
    unsigned len = strlen(in);
    enum { digit1, digit2, digit3, dot } state = digit1;

    for (;;) {
        switch (state) {
        case digit1:
            if ((i == len) || !isdigit(in[i])) {
                goto fail;
            }
            x = in[i] - '0';
            ++i;
            state = digit2;
            break;
        case digit2:
        case digit3:
            if (i == len) {
                out[o++] = x;
                goto stop;
            } else if (in[i] == '.') {
                out[o++] = x;
                state = digit1;
            } else if (isdigit(in[i])) {
                x = (x * 10) + (in[i] - '0');
                ++state;
            } else {
                goto fail;
            }
            ++i;
            break;
        case dot:
            if (i == len) {
                out[o++] = x;
                goto stop;
            } else if (in[i] == '.') {
                out[o++] = x;
                state = digit1;
            } else {
                goto fail;
            }
            ++i;
            break;
        }
    }
 stop:
    if (o == size) {
        if (endp != NULL) {
            *endp = (char *)in + i;
        }
        return 0;
    }
 fail:
    if (endp != NULL) {
        *endp = (char *)in + i;
    }
    errno = EINVAL;
    return -1;
}

static int
tmstat_parse_hex(const char *in, char **endp, void *vout, unsigned size)
{
    uint8_t x;
    uint8_t *out = vout;
    unsigned i = 0;
    unsigned o = 0;
    unsigned len = strlen(in);

    if (len + 1 != 3 * size) {
        goto fail;
    }
    for (;;) {
        x = 0;
        if (in[i] >= '0' && in[i] <= '9') {
            x = (x << 4) | (in[i] - '0');
            ++i;
        } else if (in[i] >= 'a' && in[i] <= 'f') {
            x = (x << 4) | (10 + (in[i] - 'a'));
            ++i;
        } else if (in[i] >= 'A' && in[i] <= 'F') {
            x = (x << 4) | (10 + (in[i] - 'A'));
            ++i;
        } else {
            goto fail;
        }
        if (in[i] >= '0' && in[i] <= '9') {
            x = (x << 4) | (in[i] - '0');
            ++i;
        } else if (in[i] >= 'a' && in[i] <= 'f') {
            x = (x << 4) | (10 + (in[i] - 'a'));
            ++i;
        } else if (in[i] >= 'A' && in[i] <= 'F') {
            x = (x << 4) | (10 + (in[i] - 'A'));
            ++i;
        } else {
            goto fail;
        }
        out[o++] = x;
        if (i == len) {
            break;
        } else if (in[i] != ':') {
            goto fail;
        } else {
            ++i;
        }
    }
    if (endp != NULL) {
        *endp = (char *)in + i;
    }
    return 0;
 fail:
    if (endp != NULL) {
        *endp = (char *)in + i;
    }
    errno = EINVAL;
    return -1;
}

int
tmstat_parse(const char *in, char **endp, void *out,
             enum tmstat_type type, unsigned size)
{
    if (in == NULL) {
        errno = EINVAL;
        return -1;
    }
    if (size == 0) {
        if (endp != NULL) {
            *endp = (char *)in;
        }
        if (*in == '\0') {
            return 0;
        } else {
            errno = EINVAL;
            return -1;
        }
    }
    if (out == NULL) {
        errno = EINVAL;
        return -1;
    }
    switch (type) {
    case TMSTAT_T_BIN:
        return tmstat_parse_bin(in, endp, out, size);
    case TMSTAT_T_DEC:
        return tmstat_parse_dec(in, endp, out, size);
    case TMSTAT_T_HEX:
        return tmstat_parse_hex(in, endp, out, size);
    default:
        errno = EINVAL;
        return -1;
    }
}

static int
tmstat_print_bin(void *in, char *out, unsigned size)
{
    uint8_t *p = in;
    uint8_t x;
    int i;

    for (i = 0; i < size; ++i) {
        x = p[i];
        *out++ = ((x >> 7) & 1) + '0';
        *out++ = ((x >> 6) & 1) + '0';
        *out++ = ((x >> 5) & 1) + '0';
        *out++ = ((x >> 4) & 1) + '0';
        *out++ = ':';
        *out++ = ((x >> 3) & 1) + '0';
        *out++ = ((x >> 2) & 1) + '0';
        *out++ = ((x >> 1) & 1) + '0';
        *out++ = ((x >> 0) & 1) + '0';
        *out++ = ' ';
    }
    out[-1] = '\0';
    return 0;
}

static int
tmstat_print_dec(void *in, char *out, unsigned size)
{
    uint8_t *p = in;
    uint8_t x;
    int i;

    for (i = 0; i < size; ++i) {
        x = p[i];
        if (x > 99) {
            *out++ = (x / 100) + '0';
        }
        if (x > 9) {
            *out++ = ((x / 10) % 10) + '0';
        }
        *out++ = (x % 10) + '0';
        *out++ = '.';
    }
    out[-1] = '\0';
    return 0;
}

static int
tmstat_print_hex(void *in, char *out, unsigned size)
{
    uint8_t *p = in;
    uint8_t x;
    int i;

    for (i = 0; i < size; ++i) {
        x = p[i] >> 4;
        *out++ = (x < 10) ? (x + '0') : ((x - 10) + 'A');
        x = p[i] & 0x0F;
        *out++ = (x < 10) ? (x + '0') : ((x - 10) + 'A');
        *out++ = ':';
    }
    out[-1] = '\0';
    return 0;
}

int
tmstat_print(void *in, char *out, enum tmstat_type type, unsigned size)
{
    if (size == 0) {
        *out = '\0';
        return 0;
    }
    if (in == NULL) {
        errno = EINVAL;
        return -1;
    }
    switch (type) {
    case TMSTAT_T_BIN:
        return tmstat_print_bin(in, out, size);
    case TMSTAT_T_DEC:
        return tmstat_print_dec(in, out, size);
    case TMSTAT_T_HEX:
        return tmstat_print_hex(in, out, size);
    default:
        errno = EINVAL;
        return -1;
    }
}

unsigned
tmstat_strlen(enum tmstat_type type, unsigned size)
{
    switch (type) {
    case TMSTAT_T_BIN:
        return size * 10;   /* bbbb:bbbb bbbb:bbbb ... bbbb:bbbb\0 */
    case TMSTAT_T_DEC:
        return size * 4;    /* a[b[c]].a[b[c]].....a[b[c]]\0 */
    case TMSTAT_T_HEX:
        return size * 3;    /* xx:xx:...:xx\0 */
    case TMSTAT_T_TEXT:
        return size + 1;    /* +1 in case there's no NUL in the input. */
    case TMSTAT_T_SIGNED:
    case TMSTAT_T_UNSIGNED:
        /* log(10)/log(2) > 3 so for each char we can represent more than
         * three bits.  Add three for rounding error, sign, and NUL. */
        return ((sizeof(intmax_t) * 8) / 3) + 3;
    case TMSTAT_T_HIDDEN:
        return 1;           /* For a single NUL. */
    }
    return 0;
}
