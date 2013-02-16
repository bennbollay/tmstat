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
 * CPU display.
 *
 */
#include <stdint.h>

#include "tmstat.h"
#include "tmstat_dash.h"

struct display display_cpu[] = {
    { .x =   1, .y =   1, .what = DT_LABEL,
        .s = "PROC0 CPU:    %% busy      %% idle     %% sleep" },

    { .x =  14, .y =   1, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(((((<proc_stat[0].tm_total_cycles-(<proc_stat[0].tm_idle_cycles+<proc_stat[0].tm_sleep_cycles))*1000)/<proc_stat[0].tm_total_cycles)+5)/10)" },

    { .x =  26, .y =   1, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[0].tm_idle_cycles*1000)/<proc_stat[0].tm_total_cycles)+5)/10)" },

    { .x =  37, .y =   1, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[0].tm_sleep_cycles*1000)/<proc_stat[0].tm_total_cycles)+5)/10)" },

    { .x =   1, .y =   3, .what = DT_LABEL,
        .s = "PROC1 CPU:    %% busy      %% idle     %% sleep" },

    { .x =  14, .y =   3, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(((((<proc_stat[1].tm_total_cycles-(<proc_stat[1].tm_idle_cycles+<proc_stat[1].tm_sleep_cycles))*1000)/<proc_stat[1].tm_total_cycles)+5)/10)" },

    { .x =  26, .y =   3, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[1].tm_idle_cycles*1000)/<proc_stat[1].tm_total_cycles)+5)/10)" },

    { .x =  37, .y =   3, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[1].tm_sleep_cycles*1000)/<proc_stat[1].tm_total_cycles)+5)/10)" },

    { .x =   1, .y =   5, .what = DT_LABEL,
        .s = "PROC2 CPU:    %% busy      %% idle     %% sleep" },

    { .x =  14, .y =   5, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(((((<proc_stat[2].tm_total_cycles-(<proc_stat[2].tm_idle_cycles+<proc_stat[2].tm_sleep_cycles))*1000)/<proc_stat[2].tm_total_cycles)+5)/10)" },

    { .x =  26, .y =   5, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[2].tm_idle_cycles*1000)/<proc_stat[2].tm_total_cycles)+5)/10)" },

    { .x =  37, .y =   5, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[2].tm_sleep_cycles*1000)/<proc_stat[2].tm_total_cycles)+5)/10)" },

    { .x =   1, .y =   7, .what = DT_LABEL,
        .s = "PROC3 CPU:    %% busy      %% idle     %% sleep" },

    { .x =  14, .y =   7, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(((((<proc_stat[3].tm_total_cycles-(<proc_stat[3].tm_idle_cycles+<proc_stat[3].tm_sleep_cycles))*1000)/<proc_stat[3].tm_total_cycles)+5)/10)" },

    { .x =  26, .y =   7, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[3].tm_idle_cycles*1000)/<proc_stat[3].tm_total_cycles)+5)/10)" },

    { .x =  37, .y =   7, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[3].tm_sleep_cycles*1000)/<proc_stat[3].tm_total_cycles)+5)/10)" },

    { .x =   1, .y =   9, .what = DT_LABEL,
        .s = "PROC4 CPU:    %% busy      %% idle     %% sleep" },

    { .x =  14, .y =   9, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(((((<proc_stat[4].tm_total_cycles-(<proc_stat[4].tm_idle_cycles+<proc_stat[4].tm_sleep_cycles))*1000)/<proc_stat[4].tm_total_cycles)+5)/10)" },

    { .x =  26, .y =   9, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[4].tm_idle_cycles*1000)/<proc_stat[4].tm_total_cycles)+5)/10)" },

    { .x =  37, .y =   9, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[4].tm_sleep_cycles*1000)/<proc_stat[4].tm_total_cycles)+5)/10)" },

    { .x =   1, .y =   11, .what = DT_LABEL,
        .s = "PROC5 CPU:    %% busy      %% idle     %% sleep" },

    { .x =  14, .y =   11, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(((((<proc_stat[5].tm_total_cycles-(<proc_stat[5].tm_idle_cycles+<proc_stat[5].tm_sleep_cycles))*1000)/<proc_stat[5].tm_total_cycles)+5)/10)" },

    { .x =  26, .y =   11, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[5].tm_idle_cycles*1000)/<proc_stat[5].tm_total_cycles)+5)/10)" },

    { .x =  37, .y =   11, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[5].tm_sleep_cycles*1000)/<proc_stat[5].tm_total_cycles)+5)/10)" },

    { .x =   1, .y =   13, .what = DT_LABEL,
        .s = "PROC6 CPU:    %% busy      %% idle     %% sleep" },

    { .x =  14, .y =   13, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(((((<proc_stat[6].tm_total_cycles-(<proc_stat[6].tm_idle_cycles+<proc_stat[6].tm_sleep_cycles))*1000)/<proc_stat[6].tm_total_cycles)+5)/10)" },

    { .x =  26, .y =   13, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[6].tm_idle_cycles*1000)/<proc_stat[6].tm_total_cycles)+5)/10)" },

    { .x =  37, .y =   13, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[6].tm_sleep_cycles*1000)/<proc_stat[6].tm_total_cycles)+5)/10)" },

    { .x =   1, .y =   15, .what = DT_LABEL,
        .s = "PROC7 CPU:    %% busy      %% idle     %% sleep" },

    { .x =  14, .y =   15, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(((((<proc_stat[7].tm_total_cycles-(<proc_stat[7].tm_idle_cycles+<proc_stat[7].tm_sleep_cycles))*1000)/<proc_stat[7].tm_total_cycles)+5)/10)" },

    { .x =  26, .y =   15, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[7].tm_idle_cycles*1000)/<proc_stat[7].tm_total_cycles)+5)/10)" },

    { .x =  37, .y =   15, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat[7].tm_sleep_cycles*1000)/<proc_stat[7].tm_total_cycles)+5)/10)" },

    { .what = DT_EOL }
};
