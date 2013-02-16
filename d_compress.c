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
 * Vcompress display.
 * 
 */
#include <stdint.h>

#include "tmstat.h"
#include "tmstat_dash.h"

struct display display_compress[] = {
    { .x =   1, .y =   1, .what = DT_LABEL,
        .s = "CPU:    %% busy      %% idle     %% sleep" },
    { .x =  9, .y =   1, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(((((<proc_stat.tm_total_cycles-(<proc_stat.tm_idle_cycles+<proc_stat.tm_sleep_cycles))*1000)/<proc_stat.tm_total_cycles)+5)/10)" },
    { .x =  21, .y =   1, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat.tm_idle_cycles*1000)/<proc_stat.tm_total_cycles)+5)/10)" },
    { .x =  32, .y =   1, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "((((<proc_stat.tm_sleep_cycles*1000)/<proc_stat.tm_total_cycles)+5)/10)" },

    /* Memory under exclusive process control. */
    { .x =  15, .y =   3, .what = DT_LABEL, .s = "Memory Allocated",
                                            .align = CENTER },
    { .x =  14, .y =   4, .what = DT_LABEL, .s = "/",
                                            .align = CENTER },
    { .x =  13, .y =   4, .what = DT_INT, .s = "proc_stat.memory_used",
                                            .align = RIGHT },
    { .x =  16, .y =   4, .what = DT_INT, .s = "proc_stat.memory_total",
                                            .align = LEFT },

    /* Misc. */
    { .x = -8, .y = 3, .what = DT_LABEL, .s = "Polls"},
    { .x = -9, .y = 3, .what = DT_INT, .s = "<proc_stat.polls", .align = RIGHT },
    
    /* Compress */
    { .x =  40, .y =  7, .what = DT_LABEL, .s = "Compression", .align = CENTER },
    { .x =  12, .y =  8, .what = DT_LABEL, .s = "Device",      .align = CENTER },
    { .x =  54, .y =  8, .what = DT_LABEL, .s = "Streams",     .align = RIGHT },
    { .x =  -6, .y =  8, .what = DT_LABEL, .s = "Bit Rate",    .align = RIGHT },
                                            
    { .x =  12, .y =  9, .what = DT_STRING,.s = "compress[0].provider", .align = CENTER },
    { .x =  29, .y =  9, .what = DT_INT,   .s = "compress[0].cur_enqueued", .align = RIGHT },       
    { .x =  30, .y =  9, .what = DT_LABEL, .s = "queued" },
    { .x =  29, .y =  10, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(100-((((<compress[0].tot_bytes_out*1000)/<compress[0].tot_bytes_in)+5)/10))" },
    { .x =  30, .y =  10, .what = DT_LABEL,.s = "%% saved", .align = LEFT },
    { .x =  50, .y =  9, .what = DT_INT,   .s = "compress[0].cur_ctx", .align = RIGHT },
    { .x =  51, .y =  9, .what = DT_LABEL, .s = "cur" },
    { .x =  50, .y =  10, .what = DT_INT,  .s = "compress[0].tot_ctx", .align = RIGHT },       
    { .x =  51, .y =  10, .what = DT_LABEL,.s = "tot" },
    { .x =  -6, .y =  9, .what = DT_INT,   .s = "(<compress[0].tot_bytes_out*8)", .align = RIGHT },
    { .x =  -6, .y =  10, .what = DT_INT,  .s = "(<compress[0].tot_bytes_in*8)", .align = RIGHT },
    { .x =  -5, .y =  9, .what = DT_LABEL, .s = "post" },
    { .x =  -5, .y =  10, .what = DT_LABEL,.s = "pre" },

      
    { .x =  12, .y =  12, .what = DT_STRING,.s = "compress[1].provider", .align = CENTER },
    { .x =  29, .y =  12, .what = DT_INT,   .s = "compress[1].cur_enqueued", .align = RIGHT },       
    { .x =  30, .y =  12, .what = DT_LABEL, .s = "queued" },
    { .x =  29, .y =  13, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(100-((((<compress[1].tot_bytes_out*1000)/<compress[1].tot_bytes_in)+5)/10))" },
    { .x =  30, .y =  13, .what = DT_LABEL, .s = "%% saved", .align = LEFT },
    { .x =  50, .y =  12, .what = DT_INT,   .s = "compress[1].cur_ctx", .align = RIGHT },
    { .x =  50, .y =  13, .what = DT_INT,   .s = "compress[1].tot_ctx", .align = RIGHT },       
    { .x =  51, .y =  12, .what = DT_LABEL, .s = "cur" },
    { .x =  51, .y =  13, .what = DT_LABEL, .s = "tot" },
    { .x =  -6, .y =  12, .what = DT_INT,   .s = "(<compress[1].tot_bytes_out*8)", .align = RIGHT },
    { .x =  -6, .y =  13, .what = DT_INT,   .s = "(<compress[1].tot_bytes_in*8)", .align = RIGHT },
    { .x =  -5, .y =  12, .what = DT_LABEL, .s = "post" },
    { .x =  -5, .y =  13, .what = DT_LABEL, .s = "pre" },

    
    { .x =  12, .y =  15, .what = DT_STRING,.s = "compress[2].provider", .align = CENTER },
    { .x =  29, .y =  15, .what = DT_INT,   .s = "compress[2].cur_enqueued", .align = RIGHT },       
    { .x =  30, .y =  15, .what = DT_LABEL, .s = "queued" },
    { .x =  29, .y =  16, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(100-((((<compress[2].tot_bytes_out*1000)/<compress[2].tot_bytes_in)+5)/10))" },
    { .x =  30, .y =  16, .what = DT_LABEL, .s = "%% saved", .align = LEFT },
    { .x =  50, .y =  15, .what = DT_INT,   .s = "compress[2].cur_ctx", .align = RIGHT },
    { .x =  50, .y =  16, .what = DT_INT,   .s = "compress[2].tot_ctx", .align = RIGHT },       
    { .x =  51, .y =  15, .what = DT_LABEL, .s = "cur" },
    { .x =  51, .y =  16, .what = DT_LABEL, .s = "tot" },
    { .x =  -6, .y =  15, .what = DT_INT,   .s = "(<compress[2].tot_bytes_out*8)", .align = RIGHT },
    { .x =  -6, .y =  16, .what = DT_INT,   .s = "(<compress[2].tot_bytes_in*8)", .align = RIGHT },
    { .x =  -5, .y =  15, .what = DT_LABEL, .s = "post" },
    { .x =  -5, .y =  16, .what = DT_LABEL, .s = "pre" },

    
    { .x =  12, .y =  18, .what = DT_STRING,.s = "compress[3].provider", .align = CENTER },
    { .x =  29, .y =  18, .what = DT_INT,   .s = "compress[3].cur_enqueued", .align = RIGHT },       
    { .x =  30, .y =  18, .what = DT_LABEL, .s = "queued" },
    { .x =  29, .y =  19, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(100-((((<compress[3].tot_bytes_out*1000)/<compress[3].tot_bytes_in)+5)/10))" },
    { .x =  30, .y =  19, .what = DT_LABEL, .s = "%% saved", .align = LEFT },
    { .x =  50, .y =  19, .what = DT_INT,   .s = "compress[3].cur_ctx", .align = RIGHT },
    { .x =  50, .y =  19, .what = DT_INT,   .s = "compress[3].tot_ctx", .align = RIGHT },       
    { .x =  51, .y =  18, .what = DT_LABEL, .s = "cur" },
    { .x =  51, .y =  19, .what = DT_LABEL, .s = "tot" },
    { .x =  -6, .y =  18, .what = DT_INT,   .s = "(<compress[3].tot_bytes_out*8)", .align = RIGHT },
    { .x =  -6, .y =  19, .what = DT_INT,   .s = "(<compress[3].tot_bytes_in*8)", .align = RIGHT },
    { .x =  -5, .y =  18, .what = DT_LABEL, .s = "post" },
    { .x =  -5, .y =  19, .what = DT_LABEL, .s = "pre" },

    
    { .x =  12, .y =  21, .what = DT_LABEL, .s = "GRAND TOTAL", .align = CENTER },
    { .x =  29, .y =  21, .what = DT_INT,
      .s = "((compress[0].cur_enqueued+compress[1].cur_enqueued)+(compress[2].cur_enqueued+compress[3].cur_enqueued))", .align = RIGHT },
    { .x =  30, .y =  21, .what = DT_LABEL, .s = "queued" },
    { .x =  29, .y =  22, .what = DT_INT, .w = 3, .align = RIGHT,
        .s = "(100-((((((<compress[0].tot_bytes_out+<compress[1].tot_bytes_out)+(<compress[2].tot_bytes_out+<compress[3].tot_bytes_out))*1000)/((<compress[0].tot_bytes_in+<compress[1].tot_bytes_in)+(<compress[2].tot_bytes_in+<compress[3].tot_bytes_in)))+5)/10))" },
    { .x =  30, .y =  22, .what = DT_LABEL, .s = "%% saved", .align = LEFT },
    { .x =  50, .y =  21, .what = DT_INT,
      .s = "((compress[0].cur_ctx+compress[1].cur_ctx)+(compress[2].cur_ctx+compress[3].cur_ctx))", .align = RIGHT },
    { .x =  50, .y =  22, .what = DT_INT,
      .s = "((compress[0].tot_ctx+compress[1].tot_ctx)+(compress[2].tot_ctx+compress[3].tot_ctx))", .align = RIGHT },
    { .x =  51, .y =  21, .what = DT_LABEL, .s = "cur" },
    { .x =  51, .y =  22, .what = DT_LABEL, .s = "tot" },
    { .x =  -6, .y =  21, .what = DT_INT,
      .s = "(((<compress[0].tot_bytes_out+<compress[1].tot_bytes_out)+(<compress[2].tot_bytes_out+<compress[3].tot_bytes_out))*8)", .align = RIGHT },
    { .x =  -6, .y =  22, .what = DT_INT,
      .s = "(((<compress[0].tot_bytes_in+<compress[1].tot_bytes_in)+(<compress[2].tot_bytes_in+<compress[3].tot_bytes_in))*8)", .align = RIGHT },
    { .x =  -5, .y =  21, .what = DT_LABEL, .s = "post" },
    { .x =  -5, .y =  22, .what = DT_LABEL, .s = "pre" },

    /* Streams per sec by gzip level */
    { .x =  40, .y =  24, .what = DT_LABEL, .s = "New Streams Per Second", .align = CENTER },
    { .x =   1, .y =  25, .what = DT_LABEL, .s =
      "Device        0      1      2      3      4      5      6      7      8      9" },
    { .x =   1, .y =  26, .what = DT_STRING, .s = "compress[0].provider" },
    { .x =   1, .y =  27, .what = DT_STRING, .s = "compress[1].provider" },
    { .x =   1, .y =  28, .what = DT_STRING, .s = "compress[2].provider" },

    { .x =  16, .y =  26, .what = DT_INT, .s = "<compress[0].0_tot_ctx", .align = RIGHT },
    { .x =  23, .y =  26, .what = DT_INT, .s = "<compress[0].1_tot_ctx", .align = RIGHT },
    { .x =  30, .y =  26, .what = DT_INT, .s = "<compress[0].2_tot_ctx", .align = RIGHT },
    { .x =  37, .y =  26, .what = DT_INT, .s = "<compress[0].3_tot_ctx", .align = RIGHT },
    { .x =  44, .y =  26, .what = DT_INT, .s = "<compress[0].4_tot_ctx", .align = RIGHT },
    { .x =  51, .y =  26, .what = DT_INT, .s = "<compress[0].5_tot_ctx", .align = RIGHT },
    { .x =  58, .y =  26, .what = DT_INT, .s = "<compress[0].6_tot_ctx", .align = RIGHT },
    { .x =  65, .y =  26, .what = DT_INT, .s = "<compress[0].7_tot_ctx", .align = RIGHT },
    { .x =  72, .y =  26, .what = DT_INT, .s = "<compress[0].8_tot_ctx", .align = RIGHT },
    { .x =  79, .y =  26, .what = DT_INT, .s = "<compress[0].9_tot_ctx", .align = RIGHT },

    { .x =  16, .y =  27, .what = DT_INT, .s = "<compress[1].0_tot_ctx", .align = RIGHT },
    { .x =  23, .y =  27, .what = DT_INT, .s = "<compress[1].1_tot_ctx", .align = RIGHT },
    { .x =  30, .y =  27, .what = DT_INT, .s = "<compress[1].2_tot_ctx", .align = RIGHT },
    { .x =  37, .y =  27, .what = DT_INT, .s = "<compress[1].3_tot_ctx", .align = RIGHT },
    { .x =  44, .y =  27, .what = DT_INT, .s = "<compress[1].4_tot_ctx", .align = RIGHT },
    { .x =  51, .y =  27, .what = DT_INT, .s = "<compress[1].5_tot_ctx", .align = RIGHT },
    { .x =  58, .y =  27, .what = DT_INT, .s = "<compress[1].6_tot_ctx", .align = RIGHT },
    { .x =  65, .y =  27, .what = DT_INT, .s = "<compress[1].7_tot_ctx", .align = RIGHT },
    { .x =  72, .y =  27, .what = DT_INT, .s = "<compress[1].8_tot_ctx", .align = RIGHT },
    { .x =  79, .y =  27, .what = DT_INT, .s = "<compress[1].9_tot_ctx", .align = RIGHT },

    { .x =  16, .y =  28, .what = DT_INT, .s = "<compress[2].0_tot_ctx", .align = RIGHT },
    { .x =  23, .y =  28, .what = DT_INT, .s = "<compress[2].1_tot_ctx", .align = RIGHT },
    { .x =  30, .y =  28, .what = DT_INT, .s = "<compress[2].2_tot_ctx", .align = RIGHT },
    { .x =  37, .y =  28, .what = DT_INT, .s = "<compress[2].3_tot_ctx", .align = RIGHT },
    { .x =  44, .y =  28, .what = DT_INT, .s = "<compress[2].4_tot_ctx", .align = RIGHT },
    { .x =  51, .y =  28, .what = DT_INT, .s = "<compress[2].5_tot_ctx", .align = RIGHT },
    { .x =  58, .y =  28, .what = DT_INT, .s = "<compress[2].6_tot_ctx", .align = RIGHT },
    { .x =  65, .y =  28, .what = DT_INT, .s = "<compress[2].7_tot_ctx", .align = RIGHT },
    { .x =  72, .y =  28, .what = DT_INT, .s = "<compress[2].8_tot_ctx", .align = RIGHT },
    { .x =  79, .y =  28, .what = DT_INT, .s = "<compress[2].9_tot_ctx", .align = RIGHT },

    { .what = DT_EOL }
};
