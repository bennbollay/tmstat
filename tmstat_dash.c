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
#include <assert.h>
#include <ctype.h>
#include <curses.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sysexits.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <err.h>

#include "tmstat.h"
#include "tmstat_dash.h"


/*
 * Elements calculated over time.
 *
 * The results of the following equations are retained across
 * multiple samples and are used to calcuate an overall average
 * rate.  This method is used instead of a moving average in
 * an attempt to provide the most accurate results.
 */
struct rate {
    char        *name;          // Entry name.
    char        *what;          // Equation.
    int64_t     val[11];        // Previous values.
    int64_t     rate;           // Current velocity (N / second).
} rate[] = {
    { .name = "crypto.rsa",
      .what="((crypto.mepkcs.done+crypto.mepkcslg.done)+crypto.viper.done)" },
    { .name = "crypto.hs",                  .what="crypto.server.done" },
    { .name = "crypto.record",              .what="crypto.record.done" },
    { .name = "crypto.cipher",              .what="crypto.cipher.done" },
    { .name = "crypto.misc",
      .what="(((crypto.bcm0.done+crypto.bcm1.done)+"
          "(crypto.cn0.done+crypto.cn1.done+crypto.cn2.done+crypto.cn3.done))-"
            "((crypto.mepkcs.done+crypto.mepkcslg.done)+"
             "(crypto.server.done+(crypto.record.done+crypto.cipher.done))))", },
    { .name = "crypto.total",
      .what="((crypto.bcm0.done+crypto.bcm1.done)+"
        "((crypto.cn0.done+crypto.cn1.done+crypto.cn2.done+crypto.cn3.done)"
          "+crypto.viper.done))" },
    { .name = "if[1].bytes_in",   .what="interface_stat[1].counters.bytes_in" },
    { .name = "if[2].bytes_in",   .what="interface_stat[2].counters.bytes_in" },
    { .name = "if[3].bytes_in",   .what="interface_stat[3].counters.bytes_in" },
    { .name = "if[4].bytes_in",   .what="interface_stat[4].counters.bytes_in" },
    { .name = "if[5].bytes_in",   .what="interface_stat[5].counters.bytes_in" },
    { .name = "if[6].bytes_in",   .what="interface_stat[6].counters.bytes_in" },
    { .name = "if[7].bytes_in",   .what="interface_stat[7].counters.bytes_in" },
    { .name = "if[8].bytes_in",   .what="interface_stat[8].counters.bytes_in" },
    { .name = "if[9].bytes_in",   .what="interface_stat[9].counters.bytes_in" },
    { .name = "if[1].bytes_out",  .what="interface_stat[1].counters.bytes_out" },
    { .name = "if[2].bytes_out",  .what="interface_stat[2].counters.bytes_out" },
    { .name = "if[3].bytes_out",  .what="interface_stat[3].counters.bytes_out" },
    { .name = "if[4].bytes_out",  .what="interface_stat[4].counters.bytes_out" },
    { .name = "if[5].bytes_out",  .what="interface_stat[5].counters.bytes_out" },
    { .name = "if[6].bytes_out",  .what="interface_stat[6].counters.bytes_out" },
    { .name = "if[7].bytes_out",  .what="interface_stat[7].counters.bytes_out" },
    { .name = "if[8].bytes_out",  .what="interface_stat[8].counters.bytes_out" },
    { .name = "if[9].bytes_out",  .what="interface_stat[9].counters.bytes_out" },
    { .name = "if[1].pkts_in",    .what="interface_stat[1].counters.pkts_in" },
    { .name = "if[2].pkts_in",    .what="interface_stat[2].counters.pkts_in" },
    { .name = "if[3].pkts_in",    .what="interface_stat[3].counters.pkts_in" },
    { .name = "if[4].pkts_in",    .what="interface_stat[4].counters.pkts_in" },
    { .name = "if[5].pkts_in",    .what="interface_stat[5].counters.pkts_in" },
    { .name = "if[6].pkts_in",    .what="interface_stat[6].counters.pkts_in" },
    { .name = "if[7].pkts_in",    .what="interface_stat[7].counters.pkts_in" },
    { .name = "if[8].pkts_in",    .what="interface_stat[8].counters.pkts_in" },
    { .name = "if[9].pkts_in",    .what="interface_stat[9].counters.pkts_in" },
    { .name = "if[1].pkts_out",   .what="interface_stat[1].counters.pkts_out" },
    { .name = "if[2].pkts_out",   .what="interface_stat[2].counters.pkts_out" },
    { .name = "if[3].pkts_out",   .what="interface_stat[3].counters.pkts_out" },
    { .name = "if[4].pkts_out",   .what="interface_stat[4].counters.pkts_out" },
    { .name = "if[5].pkts_out",   .what="interface_stat[5].counters.pkts_out" },
    { .name = "if[6].pkts_out",   .what="interface_stat[6].counters.pkts_out" },
    { .name = "if[7].pkts_out",   .what="interface_stat[7].counters.pkts_out" },
    { .name = "if[8].pkts_out",   .what="interface_stat[8].counters.pkts_out" },
    { .name = "if[9].pkts_out",   .what="interface_stat[9].counters.pkts_out" },
    { .name = "if[1].drops_in",   .what="interface_stat[1].counters.drops_in" },
    { .name = "if[2].drops_in",   .what="interface_stat[2].counters.drops_in" },
    { .name = "if[3].drops_in",   .what="interface_stat[3].counters.drops_in" },
    { .name = "if[4].drops_in",   .what="interface_stat[4].counters.drops_in" },
    { .name = "if[5].drops_in",   .what="interface_stat[5].counters.drops_in" },
    { .name = "if[6].drops_in",   .what="interface_stat[6].counters.drops_in" },
    { .name = "if[7].drops_in",   .what="interface_stat[7].counters.drops_in" },
    { .name = "if[8].drops_in",   .what="interface_stat[8].counters.drops_in" },
    { .name = "if[9].drops_in",   .what="interface_stat[9].counters.drops_in" },
    { .name = "if[1].drops_out",   .what="interface_stat[1].counters.drops_out" },
    { .name = "if[2].drops_out",   .what="interface_stat[2].counters.drops_out" },
    { .name = "if[3].drops_out",   .what="interface_stat[3].counters.drops_out" },
    { .name = "if[4].drops_out",   .what="interface_stat[4].counters.drops_out" },
    { .name = "if[5].drops_out",   .what="interface_stat[5].counters.drops_out" },
    { .name = "if[6].drops_out",   .what="interface_stat[6].counters.drops_out" },
    { .name = "if[7].drops_out",   .what="interface_stat[7].counters.drops_out" },
    { .name = "if[8].drops_out",   .what="interface_stat[8].counters.drops_out" },
    { .name = "if[9].drops_out",   .what="interface_stat[9].counters.drops_out" },
    { .name = "rnd.entropy",                .what="rnd.entropy.consumed" },
    { .name = "rnd.pseudo",                 .what="rnd.pseudo.consumed" },
    { .name = "rnd.secure",                 .what="rnd.secure.consumed" },
    { .name = "rnd.total",
      .what = "((rnd.entropy.consumed+rnd.pseudo.consumed)+"
               "rnd.secure.consumed)" },
    { .name = "tcp4.accepts",               .what="tcp4.accepts" },
    { .name = "tcp4.connects",              .what="tcp4.connects" },
    { .name = "tcp4.delack",                .what="tcp4.delack" },
    { .name = "tcp4.sndrexmitpack",         .what="tcp4.sndrexmitpack" },
    { .name = "proc_stat.tm_total_cycles",  .what="proc_stat.tm_total_cycles" },
    { .name = "proc_stat.tm_idle_cycles",   .what="proc_stat.tm_idle_cycles" },
    { .name = "proc_stat.newflow_packets",        .what="proc_stat.newflow_packets" },
    { .name = "proc_stat.oldflow_packets",        .what="proc_stat.oldflow_packets" },
    { .name = "proc_stat.polls",                  .what="proc_stat.polls" },
    { .name = "proc_stat.tm_sleep_cycles",  .what="proc_stat.tm_sleep_cycles" },
    { .name = "compress[0].tot_bytes_in", .what="compress[0].tot_bytes_in" },
    { .name = "compress[0].tot_bytes_out",.what="compress[0].tot_bytes_out" },
    { .name = "compress[1].tot_bytes_in", .what="compress[1].tot_bytes_in" },
    { .name = "compress[1].tot_bytes_out",.what="compress[1].tot_bytes_out" },
    { .name = "compress[2].tot_bytes_in", .what="compress[2].tot_bytes_in" },
    { .name = "compress[2].tot_bytes_out",.what="compress[2].tot_bytes_out" },
    { .name = "compress[3].tot_bytes_in", .what="compress[3].tot_bytes_in" },
    { .name = "compress[3].tot_bytes_out",.what="compress[3].tot_bytes_out" },
    { .name = "compress[0].0_tot_ctx",    .what="compress[0].0_tot_ctx" },
    { .name = "compress[0].1_tot_ctx",    .what="compress[0].1_tot_ctx" },
    { .name = "compress[0].2_tot_ctx",    .what="compress[0].2_tot_ctx" },
    { .name = "compress[0].3_tot_ctx",    .what="compress[0].3_tot_ctx" },
    { .name = "compress[0].4_tot_ctx",    .what="compress[0].4_tot_ctx" },
    { .name = "compress[0].5_tot_ctx",    .what="compress[0].5_tot_ctx" },
    { .name = "compress[0].6_tot_ctx",    .what="compress[0].6_tot_ctx" },
    { .name = "compress[0].7_tot_ctx",    .what="compress[0].7_tot_ctx" },
    { .name = "compress[0].8_tot_ctx",    .what="compress[0].8_tot_ctx" },
    { .name = "compress[0].9_tot_ctx",    .what="compress[0].9_tot_ctx" },
    { .name = "compress[1].0_tot_ctx",    .what="compress[1].0_tot_ctx" },
    { .name = "compress[1].1_tot_ctx",    .what="compress[1].1_tot_ctx" },
    { .name = "compress[1].2_tot_ctx",    .what="compress[1].2_tot_ctx" },
    { .name = "compress[1].3_tot_ctx",    .what="compress[1].3_tot_ctx" },
    { .name = "compress[1].4_tot_ctx",    .what="compress[1].4_tot_ctx" },
    { .name = "compress[1].5_tot_ctx",    .what="compress[1].5_tot_ctx" },
    { .name = "compress[1].6_tot_ctx",    .what="compress[1].6_tot_ctx" },
    { .name = "compress[1].7_tot_ctx",    .what="compress[1].7_tot_ctx" },
    { .name = "compress[1].8_tot_ctx",    .what="compress[1].8_tot_ctx" },
    { .name = "compress[1].9_tot_ctx",    .what="compress[1].9_tot_ctx" },
    { .name = "compress[2].0_tot_ctx",    .what="compress[2].0_tot_ctx" },
    { .name = "compress[2].1_tot_ctx",    .what="compress[2].1_tot_ctx" },
    { .name = "compress[2].2_tot_ctx",    .what="compress[2].2_tot_ctx" },
    { .name = "compress[2].3_tot_ctx",    .what="compress[2].3_tot_ctx" },
    { .name = "compress[2].4_tot_ctx",    .what="compress[2].4_tot_ctx" },
    { .name = "compress[2].5_tot_ctx",    .what="compress[2].5_tot_ctx" },
    { .name = "compress[2].6_tot_ctx",    .what="compress[2].6_tot_ctx" },
    { .name = "compress[2].7_tot_ctx",    .what="compress[2].7_tot_ctx" },
    { .name = "compress[2].8_tot_ctx",    .what="compress[2].8_tot_ctx" },
    { .name = "compress[2].9_tot_ctx",    .what="compress[2].9_tot_ctx" },
    { .name = "proc_stat[0].tm_total_cycles", .what="proc_stat[0].tm_total_cycles" },
    { .name = "proc_stat[0].tm_idle_cycles", .what="proc_stat[0].tm_idle_cycles" },
    { .name = "proc_stat[0].tm_sleep_cycles", .what="proc_stat[0].tm_sleep_cycles" },
    { .name = "proc_stat[1].tm_total_cycles", .what="proc_stat[1].tm_total_cycles" },
    { .name = "proc_stat[1].tm_idle_cycles", .what="proc_stat[1].tm_idle_cycles" },
    { .name = "proc_stat[1].tm_sleep_cycles", .what="proc_stat[1].tm_sleep_cycles" },
    { .name = "proc_stat[2].tm_total_cycles", .what="proc_stat[2].tm_total_cycles" },
    { .name = "proc_stat[2].tm_idle_cycles", .what="proc_stat[2].tm_idle_cycles" },
    { .name = "proc_stat[2].tm_sleep_cycles", .what="proc_stat[2].tm_sleep_cycles" },
    { .name = "proc_stat[3].tm_total_cycles", .what="proc_stat[3].tm_total_cycles" },
    { .name = "proc_stat[3].tm_idle_cycles", .what="proc_stat[3].tm_idle_cycles" },
    { .name = "proc_stat[3].tm_sleep_cycles", .what="proc_stat[3].tm_sleep_cycles" },
    { .name = "proc_stat[4].tm_total_cycles", .what="proc_stat[4].tm_total_cycles" },
    { .name = "proc_stat[4].tm_idle_cycles", .what="proc_stat[4].tm_idle_cycles" },
    { .name = "proc_stat[4].tm_sleep_cycles", .what="proc_stat[4].tm_sleep_cycles" },
    { .name = "proc_stat[5].tm_total_cycles", .what="proc_stat[5].tm_total_cycles" },
    { .name = "proc_stat[5].tm_idle_cycles", .what="proc_stat[5].tm_idle_cycles" },
    { .name = "proc_stat[5].tm_sleep_cycles", .what="proc_stat[5].tm_sleep_cycles" },
    { .name = "proc_stat[6].tm_total_cycles", .what="proc_stat[6].tm_total_cycles" },
    { .name = "proc_stat[6].tm_idle_cycles", .what="proc_stat[6].tm_idle_cycles" },
    { .name = "proc_stat[6].tm_sleep_cycles", .what="proc_stat[6].tm_sleep_cycles" },
    { .name = "proc_stat[7].tm_total_cycles", .what="proc_stat[7].tm_total_cycles" },
    { .name = "proc_stat[7].tm_idle_cycles", .what="proc_stat[7].tm_idle_cycles" },
    { .name = "proc_stat[7].tm_sleep_cycles", .what="proc_stat[7].tm_sleep_cycles" },
};

/*
 * Always display.
 *
 * Elements which are always displayed.
 * Add as little here as possible, to reduce clutter.
 */

struct display display_always[] = {
    { .x =   1, .y =   1, .what = DT_LABEL, .s = "Process Status" },
    { .x =  -1, .y =   1, .what = DT_DATE, .align = RIGHT },
    { .what = DT_EOL }
};

/*
 * Display list.
 *
 * Please alphabetize.
 */
struct {
    const char          *name;          /* Display name. */
    struct display      *display;       /* Display definition. */
} display[] = {
       { "compress",  display_compress },
       { "cpu",  display_cpu },
       { "summary",    display_summary },
};

TMSTAT tmstat = NULL;
TMSTAT_EVAL tms_eval = NULL;
char* tms_err_string;
unsigned tms_err_index;

#define IF_ROWS_MAX 32  /* arbitrary */
/* +1 for the &if_list[1] case */
#define IF_STRING_MAX 256
char if_list[IF_ROWS_MAX + 1][IF_STRING_MAX];

#define CPU_LINES 4  /* number of lines per proc on cpu display */

int main(int, char *[]);

void draw(struct display *how, const char *text);
void draw_bar(struct display *how);
void draw_mem(struct display *how);
void draw_smbar(struct display *how);
int64_t expr(const char *expression);
int64_t expr_field(const char *expression);
int64_t expr_field_iface(const char *expression);
int64_t expr_rate(const char *expression);
const char  *itoa(int64_t num);
void print_mem_item(const char* label, uint64_t bytes, uint64_t count,
    uint64_t size, int x, int y);
void rate_update(void);
void show(struct display *display);
void usage(void);

/*
 * Draw element.
 */
void
draw(struct display *how, const char *text)
{
    int len, x, y, mx, my;

    getmaxyx(stdscr, my, mx);
    len = strlen(text);
    /* Calculate column. */
    if (how->x != 0) {
        if (how->x > 0)
            x = how->x;
        else
            x = mx + how->x;
    } else
        x = mx / 2;
    switch (how->align) {
    case LEFT:
        break;
    case RIGHT:
        x -= len;
        break;
    case CENTER:
        x -= len / 2;
    }
    if (x + len > mx)
        x = mx - len;
    if (x < 0)
        x = 0;
    /* Calculate row. */
    if (how->y != 0) {
        if (how->y > 0)
            y = how->y - 1;
        else
            y = my + how->y;
    } else
        y = my / 2;
    /* Display. */
    mvprintw(y, x, text);
}

/*
 * Draw a percentage bar.
 */
void
draw_bar(struct display *how)
{
    char bar[26];
    uint64_t v;

    strcpy(bar, "[                       ]");
    v = expr(how->s);
    switch (how->align) {
    case LEFT:
        for (int x = 0; x < 23; x++)
            bar[x + 1] = ((x+1) <= v) ?
                '=' : "  .  :  .  |  .  :  .  "[x];
        break;
    case RIGHT:
    case CENTER:
        for (int x = 0; x < 23; x++)
            bar[x + 1] = ((23 - x) <= v) ?
                '=' : "  .  :  .  |  .  :  .  "[x];
        break;
    }
    draw(how, bar);
}

/*
 * Draw a list of objects in memory w/ byte totals and counts.
 */
void
print_mem_item(const char* label, uint64_t bytes, uint64_t count, uint64_t size,
    int x, int y)
{
    const char *s;
    int offset = 0;
    if (size == 1) {
        s = "--";
    }
    else {
        if (count > 1000000) {
            offset++;
            s = itoa(count/1000000 + 1);
            mvprintw(y, x - offset, "M");
        }
        else if (count > 1000) {
            offset++;
            s = itoa(count/1000 + 1);
            mvprintw(y, x - offset, "k");
        }
        else {
            s = itoa(count);
        }
    }
    offset += strlen(s);
    mvprintw(y, x-offset, s);
    // start of memory column
    offset = 7;
    if (bytes > 1048576) {
        offset++;
        s = itoa(bytes/1048576 + 1);
        mvprintw(y, x - offset, "MB");
    }
    else if (bytes > 1024) {
        offset++;
        s = itoa(bytes/1024 + 1);
        mvprintw(y, x - offset, "kB");
    }
    else {
        s = itoa(bytes);
        mvprintw(y, x - offset, "B");
    }
    offset += strlen(s);
    mvprintw(y, x - offset, s);
    mvprintw(y, x + 1, label);
}
void
draw_mem(struct display *how)
{
    int x, y, mx, my;
    TMROW *rows;
    unsigned row_count;
    uint64_t total = 0, unseen = 0;

    getmaxyx(stdscr, my, mx);
    my--;
    /* Calculate column. */
    if (how->x != 0) {
        if (how->x > 0)
            x = how->x;
        else
            x = mx + how->x;
    } else {
        x = mx / 2;
    }
    /* Find table. */
    y = how->y;
    if (how->h) {
        if (how->h > 0) {
            if (my > y + how->h)
                my = y + how->h;
        } else
            my += how->h;
    }
    total = 0;
    unseen = 0;
    

    if (0 == tmstat_query(tmstat, "memory_usage_stat", 0, NULL, NULL,
        &rows, &row_count) )
    {
        /* Locate column. */
        uint64_t m[row_count];
        uint64_t c[row_count];
        uint64_t s[row_count];
        const char *l[row_count] ;
        uint64_t max = 0;
        int maxr = 0;
    
        /* Populate list. */
        for (int r = 0; r < row_count; r++) {
            tmstat_row_field(rows[r], "name", &l[r]);
            m[r] = tmstat_row_field_signed(rows[r], "allocated");
            c[r] = tmstat_row_field_signed(rows[r], "cur_allocs");
            s[r] = tmstat_row_field_signed(rows[r], "size");
            if (strncmp("umem_", l[r], 5) == 0) {
                m[r] = c[r] = s[r] = 0;
            }
            else if ((m[r] > max) ||
                ((m[r] == max) && (strcmp(l[r], l[maxr]) < 0))) {
                max = m[r];
                maxr = r;
            }
        }
    
        /* Draw rows, maximal values first. */
        while (max > 0) {
            total += m[maxr];
            if (y < my - 1) {
                /* Draw row. */
                //char label[mx - x];
                //strncpy(label, l[maxr], mx - x - 1);
                print_mem_item(l[maxr], m[maxr], c[maxr], s[maxr], x, y);
                y++;
            } else
                unseen += m[maxr];
            /* Look for next value. */
            m[maxr] = 0;
            max = 0;
            for (int r = 0; r < row_count; r++) {
                if ((m[r] > max) ||
                    ((m[r] == max) &&
                     (strcmp(l[r], l[maxr]) < 0))) {
                    max = m[r];
                    maxr = r;
                }
            }
        }
    } else {
        return;
    }
    for (int i = 0; i < row_count; i++) {
        tmstat_row_drop(rows[i]);
    }
    print_mem_item("(total)", total, 1, 1, x, how->y);
    if (unseen > 0) {
        print_mem_item("(unseen)", unseen, 1, 1, x, my-1);
    }
}

/*
 * Draw a small percentage bar.
 */
void
draw_smbar(struct display *how)
{
    char bar[18];
    uint64_t v;

    strcpy(bar, "[ . : . | . : . ]");
    v = expr(how->s);
    switch (how->align) {
    case LEFT:
        for (int x = 0; x < 15; x++)
            bar[x + 1] = ((x+1) <= v) ?
                '=' : " . : . | . : . "[x];
        break;
    case RIGHT:
    case CENTER:
        for (int x = 0; x < 15; x++)
            bar[x + 1] = ((15 - x) <= v) ?
                '=' : " . : . | . : . "[x];
        break;
    }
    draw(how, bar);
}

/*
 * Parse a field expression.
 *
 * Syntax is:
 *
 * expr     ::= '(' expr [+*-/] expr ')'
 *          |   field
 *          |   '<' rate
 *          |   [1-9][0-9]*
 *          |   0x[0-9a-zA-Z]+
 *          |   0[0-7]+
 *          ;
 *
 *  Note that fields are unsigned values, but this function returns
 *  signed values.  We rely on the rediculously large range of a 63-bit
 *  number to guard against overflows, though they are possible.
 *
 *  Returns -1 upon parsing failure.  Nonexistant fields evaluate to 0.
 */
int64_t
expr(const char *expression)
{
    char s[strlen(expression)];

    strcpy(s, expression);

    /*
     * Match "(" expr oper expr ")".
     */
    if (s[0] == '(') {
        char *ls, *rs;
        char oper;
        int64_t lv, rv;

        /* Skip over leading '('. */
        ls = s + 1;
        /* Scan forward to operator. */
        for (rs = ls; *rs != '\0'; rs++)
            switch (*rs) {
            case '(':
                rs++;
                for (int depth = 1; depth && (*rs != '\0'); )
                    switch (*rs++) {
                    case '(':
                        depth++;
                        break;
                    case ')':
                        depth--;
                        break;
                    }
                rs--;
                break;
            case '+':
            case '-':
            case '*':
            case '/':
                goto oper_found;
            }
        /* Operator not found; return error. */
        return (-1);
oper_found:
        /* Pick up operator. */
        oper = *rs;
        /* Null terminate left expr. */
        *rs++ = '\0';
        /*
         * ls now contains the left expression, rs contains the right
         * expression, and oper contains the operator.  rs is not
         * NUL terminated, but terminated by a ')'.  This is not a
         * problem, as when we recurse to evaluate rs, the trailing
         * ')' will be stripped if need be, ignored otherwise.
         */
        lv = expr(ls);
        rv = expr(rs);
        switch (oper) {
        case '+':
            return (lv + rv);
        case '-':
            return (lv - rv);
        case '*':
            return (lv * rv);
        case '/':
            if (rv == 0)
                return (0);
            return (lv / rv);
        }
    }

    /*
     * Match literal numbers.
     */
    if (isdigit(s[0]))
        return (strtoll(s, NULL, 0));

    /*
     * Anything else must be a field request.
     */
    /* Strip any trailing ')'.
    *  We need to make sure we only strip trailing ')'s
    *  not ones in the middle of the string
    */
    for (char *p = s + strlen(s)-1; *p == ')'; p--) {
        *p = '\0';
    }
    switch (*s) {
    case '<':
        return (expr_rate(s + 1));
    default:
        return (expr_field(s));
    }
}

/*
 * Field expression parser.
 *
 * Syntax is:
 *
 * field    ::= table-name '[' row ']' column-name  // Individual field.
 *          |   table-name '[' ']' column-name      // Column sum.
 *          ;
 */
int64_t
expr_field(const char *expr)
{
    signed long long val = 0;
    tmstat_eval_signed(tms_eval, tmstat, expr, &val,
        &tms_err_string, &tms_err_index);
    return val;
}

/*
 * Rate entry parser.
 *
 * Syntax is:
 *
 * rate     ::= entry-name
 *          ;
 */
int64_t
expr_rate(const char *expr)
{
    unsigned i;

    for (i = 0; i < sizeof(rate) / sizeof(rate[0]); i++)
        if (!strcmp(rate[i].name, expr))
            return (rate[i].rate);
    return (0);
}

/*
 * Convert a number into a string, with commas.
 */
const char *
itoa(int64_t num)
{
    static char s[28];
    char *p;
    int i;
    bool negative;

    negative = (num < 0);
    num = llabs(num);
    p = &s[26];
    *p = 0;
    i = 0;
    do {
        if ((i != 0) && (i % 3 == 0))
            *--p = ',';
        *--p = (num % 10) + '0';
        num /= 10;
        i++;
    } while (num > 0);
    if (negative)
        *--p = '-';
    return (p);
}

/*
 * Process status client.
 */
int
main(int argc, char *argv[])
{
    struct display *d;
    char directory[PATH_MAX] = TMSTAT_DIR_SUBSCRIBE;
    int i;
    float sample_interval = 0.2f;
    float interval = 1.0f;
    float time_elapsed = 0.0f;
    int rc;

    /*
     * Parse options.
     */
    for (int c; (c = getopt(argc, argv, "h")) != -1; )
        switch (c) {
        case 'h':
        default:
            /* Print usage. */
            usage();
            break;
        }
    if (sample_interval > interval) {
        fprintf(stderr, "Error: sample interval cannot be greater than " 
                "display interval.\n");
        usage();
    }

    argc -= optind;
    argv += optind;
    /* Select screen. */
    if (argc >= 1) {
        for (i = 0; i < sizeof(display) / sizeof(*display); i++)
            if (strcmp(argv[0], display[i].name) == 0) {
                /* Select display. */
                d = display[i].display;
                goto selected;
            }
        usage();
    } else {
        /* No display selected; default to summary. */
        d = display_summary;
selected:
        rc = tmstat_eval_create(&tms_eval);
    }

    /*
     * Initialize curses.
     */
    initscr();
    keypad(stdscr, TRUE);
    nonl();
    cbreak();
    noecho();
    halfdelay(sample_interval*10);
    
    /*
     * Display.
     */
    rc = tmstat_subscribe(&tmstat, directory);
    if (rc != 0) {
        err(EXIT_FAILURE, "tmstat_subscribe %s", directory);
    }

    for (bool alive = true; alive; time_elapsed += sample_interval) {
        /* connect to tm_stat */
        rate_update();
        if (time_elapsed >= interval) {
            int maxy, maxx;
            time_elapsed = 0;
            erase();
            show(display_always);
            show(d);
            getmaxyx(stdscr, maxy, maxx);
            move(maxy - 1, 0);
        }
        alive = (tolower(getch()) != 'q');
        usleep(sample_interval*100000);
    } 
    tmstat_eval_destroy(tms_eval);
    tmstat_destroy(tmstat);
    endwin();
    /* clean up tmstat objs */
    
    return (EX_OK);
}

/*
 * Update rate table.
 */
void
rate_update(void)
{
    static uint64_t     prev_ticks[11];
    static unsigned     val = 0;
    unsigned            i, prev;
    signed long long    ticks, t, hz;

    /* get elapsed time in ms */
    tmstat_eval_signed(tms_eval, tmstat, "proc_stat.ticks", &ticks,
        &tms_err_string, &tms_err_index);
    
    tmstat_eval_signed(tms_eval, tmstat, "proc_stat.hz", &hz,
        &tms_err_string, &tms_err_index);
    
    prev = val;
    val = (val + 1) % 11;
    t = ticks - prev_ticks[prev];
    for (i = 0; i < sizeof(rate) / sizeof(rate[0]); i++) {
        rate[i].val[val] = expr(rate[i].what);
        /* only update the rate if time has passed */
        if (t > 0) {
            rate[i].rate = ((rate[i].val[val] - rate[i].val[prev]) * hz / t);
        }
    }
    /* Increment for next update. */
    prev_ticks[val] = ticks;
}

/*
 * Draw display.
 */
void
show(struct display *display)
{
    int i;
    char* txt;
    char date[256];
    time_t t;
    unsigned num_procs;
    tmstat_query(tmstat, "proc_stat", 0, NULL, NULL, NULL, &num_procs);

    for (i = 0; display[i].what != DT_EOL; i++) {
        switch (display[i].what) {
        case DT_MEM:
            draw_mem(&display[i]);
            break;
        case DT_DATE:
            t = time(NULL);
            strcpy(date, ctime(&t));
            attron(A_BOLD);
            draw(&display[i], date);
            standend();
            break;
        case DT_EOL:
            draw(&display[i], "[eol]");
            break;
        case DT_INT:
            draw(&display[i], itoa(expr(display[i].s)));
            break;
        case DT_LABEL:
            attron(A_BOLD);
            draw(&display[i], display[i].s);
            standend();
            break;
        case DT_BAR:
            draw_bar(&display[i]);
            break;
        case DT_STRING:
            if (tmstat_eval(tms_eval, tmstat, display[i].s, &txt,
                    &tms_err_string, &tms_err_index) != -1) {
                draw(&display[i], txt);
                free(txt);
            }
            break;
        case DT_SMBAR:
            draw_smbar(&display[i]);
            break;
        }
        /* CPU screen has a conditional number of lines */
        if (display == display_cpu && (((i + 1) / CPU_LINES) == num_procs))
            break;
    }
}

/*
 * Print usage and exit.
 */
void
usage(void)
{
    int i;

    fprintf(stderr, 
            "usage: tmstat [display]\n"
        "\tdisplays available:");
    for (i = 0; i < sizeof(display) / sizeof(*display); i++) {
        fprintf(stderr, " %s", display[i].name);
    }
    fprintf(stderr, "\n");
    exit(EX_USAGE);
}
