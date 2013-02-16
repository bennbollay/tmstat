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
 * Summary display.
 *
 */
#include <stdint.h>

#include "tmstat.h"
#include "tmstat_dash.h"

struct display display_summary[] = {
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
    { .x =  14, .y =   5, .what = DT_BAR,
      .s = "(proc_stat.memory_used/(proc_stat.memory_total/24))", .align = CENTER },

    /* Virtual memory. */
    { .x = 7, .y =  7, .what = DT_LABEL, .s = "Memory",  .align = RIGHT },
    { .x = 13, .y =  7, .what = DT_LABEL, .s = "Count",  .align = RIGHT },
    { .x = 14, .y =  7, .what = DT_LABEL, .s = "Object", .align = LEFT },
    { .x = 13, .y =  8, .what = DT_MEM },

    /* Performance. */
    { .x = -33, .y =   3, .what = DT_LABEL, .s = "Packets", .align = CENTER },
    { .x = -34, .y =   4, .what = DT_LABEL, .s = "New Flows", .align = RIGHT },
    { .x = -34, .y =   5, .what = DT_INT, .s = "<proc_stat.newflow_packets",
                                            .align = RIGHT },
    { .x = -22, .y =   4, .what = DT_LABEL, .s = "Old Flows", .align = RIGHT },
    { .x = -22, .y =   5, .what = DT_INT, .s = "<proc_stat.oldflow_packets",
                                            .align = RIGHT },
                                            
    { .x = -8, .y = 3, .what = DT_LABEL, .s = "Polls"},
    { .x = -9, .y = 3, .what = DT_INT, .s = "<proc_stat.polls", .align = RIGHT },

    /* TCP.
    { .x =  12, .y =   7, .what = DT_LABEL, .s = "Tcp", .align = RIGHT },
    { .x =  13, .y =   8, .what = DT_LABEL, .s = "Open", .align = LEFT },
    { .x =  12, .y =   8, .what = DT_INT, .align = RIGHT,
      .s = "tcp4.used" },
    { .x =  13, .y =   9, .what = DT_LABEL, .s = "Accepts", .align = LEFT },
    { .x =  12, .y =   9, .what = DT_INT, .align = RIGHT,
      .s = "<tcp4.accepts" },
    { .x =  13, .y =  10, .what = DT_LABEL, .s = "Connects", .align = LEFT },
    { .x =  12, .y =  10, .what = DT_INT, .align = RIGHT,
      .s = "<tcp4.connects" },
    { .x =  13, .y =  11, .what = DT_LABEL, .s = "Wait", .align = LEFT },
    { .x =  13, .y =  12, .what = DT_LABEL, .s = "Rtx", .align = LEFT },
    { .x =  12, .y =  12, .what = DT_INT, .align = RIGHT,
      .s = "<tcp4.sndrexmitpack" },
    { .x =  13, .y =  13, .what = DT_LABEL, .s = "Del ACK", .align = LEFT },
    { .x =  12, .y =  13, .what = DT_INT, .align = RIGHT,
      .s = "<tcp4.delack" },
    */

    /* Crypto.
    { .x =  30, .y =   7, .what = DT_LABEL, .s = "Crypto",   .align = RIGHT },
    { .x =  31, .y =   7, .what = DT_LABEL, .s = "Ops", .align = LEFT },
    { .x =  31, .y =   8, .what = DT_LABEL, .s = "(total)", .align = LEFT },
    { .x =  30, .y =   8, .what = DT_INT, .s = "<crypto.total", .align = RIGHT },
    { .x =  31, .y =   9, .what = DT_LABEL, .s = "rsa", .align = LEFT },
    { .x =  30, .y =   9, .what = DT_INT, .s = "<crypto.rsa", .align = RIGHT },
    { .x =  31, .y =  10, .what = DT_LABEL, .s = "full hs", .align = LEFT },
    { .x =  30, .y =  10, .what = DT_INT, .s = "<crypto.hs", .align = RIGHT },
    { .x =  31, .y =  11, .what = DT_LABEL, .s = "record", .align = LEFT },
    { .x =  30, .y =  11, .what = DT_INT, .s = "<crypto.record", .align = RIGHT },
    { .x =  31, .y =  12, .what = DT_LABEL, .s = "cipher", .align = LEFT },
    { .x =  30, .y =  12, .what = DT_INT, .s = "<crypto.cipher", .align = RIGHT },
    { .x =  31, .y =  13, .what = DT_LABEL, .s = "(unseen)", .align = LEFT },
    { .x =  30, .y =  13, .what = DT_INT, .s = "<crypto.misc", .align = RIGHT },
    */
    
    /* Random. 
    { .x =  47, .y =   7, .what = DT_LABEL, .s = "Random",.align = RIGHT },
    { .x =  48, .y =   7, .what = DT_LABEL, .s = "Class", .align = LEFT },
    { .x =  47, .y =   8, .what = DT_INT, .s = "<rnd.total",.align = RIGHT },
    { .x =  48, .y =   8, .what = DT_LABEL, .s = "(total)", .align = LEFT },
    { .x =  47, .y =   9, .what = DT_INT, .s = "<rnd.pseudo",.align = RIGHT },
    { .x =  48, .y =   9, .what = DT_LABEL, .s = "Pseudo", .align = LEFT },
    { .x =  47, .y =  10, .what = DT_INT, .s = "<rnd.entropy",.align = RIGHT },
    { .x =  48, .y =  10, .what = DT_LABEL, .s = "Entropy", .align = LEFT },
    { .x =  47, .y =  11, .what = DT_INT, .s = "<rnd.secure",.align = RIGHT },
    { .x =  48, .y =  11, .what = DT_LABEL, .s = "Secure", .align = LEFT },
    */

    /* Interfaces. */
    
    { .x =  -25, .y =  -21, .what = DT_STRING, .s = "interface_stat[0].name",
      .align = CENTER },
    { .x =  -4, .y =  -20, .what = DT_INT,
      .s = "(<if[0].bytes_in*8)", .align = RIGHT },
    { .x =  -34, .y =  -20, .what = DT_INT,
      .s = "(<if[0].bytes_out*8)", .align = RIGHT },
    { .x =  -4, .y =  -20, .what = DT_LABEL, .s = "b rx" },
    { .x =  -34, .y =  -20, .what = DT_LABEL, .s = "b tx" },
    { .x =  -22, .y =  -20, .what = DT_LABEL, .s = "link" },
    
    { .x =  -33, .y =   -19, .what = DT_SMBAR,
      .s = "((<if[1].bytes_in*(8*16))/interface_stat[1].wire_speed)",
      .align = RIGHT },
    { .x =  -17, .y =   -19, .what = DT_SMBAR,
      .s = "((<if[1].bytes_out*(8*16))/interface_stat[1].wire_speed)",
      .align = LEFT },
    { .x =  -25, .y =  -19, .what = DT_STRING, .s = "interface_stat[1].name",
      .align = CENTER },
    { .x =  -4, .y =  -18, .what = DT_INT,
      .s = "(<if[1].bytes_in*8)", .align = RIGHT },
    { .x =  -34, .y =  -18, .what = DT_INT,
      .s = "(<if[1].bytes_out*8)", .align = RIGHT },
    { .x =  -4, .y =  -18, .what = DT_LABEL, .s = "b rx" },
    { .x =  -34, .y =  -18, .what = DT_LABEL, .s = "b tx" },
    { .x =  -22, .y =  -18, .what = DT_LABEL, .s = "link" },
    { .x =  -23, .y =  -18, .what = DT_INT,
      .s = "(interface_stat[1].wire_speed/1000000)", .align = RIGHT },

    { .x = -33, .y =   -17, .what = DT_SMBAR,
      .s = "((<if[2].bytes_in*(8*16))/interface_stat[2].wire_speed)",
      .align = RIGHT },
    { .x = -17, .y =   -17, .what = DT_SMBAR,
      .s = "((<if[2].bytes_out*(8*16))/interface_stat[2].wire_speed)",
      .align = LEFT },
    { .x = -25, .y =   -17, .what = DT_STRING, .s = "interface_stat[2].name",
      .align = CENTER },
    { .x = -4, .y =   -16, .what = DT_INT,
      .s = "(<if[2].bytes_in*8)", .align = RIGHT },
    { .x = -34, .y =   -16, .what = DT_INT,
      .s = "(<if[2].bytes_out*8)", .align = RIGHT },
    { .x = -4, .y =   -16, .what = DT_LABEL, .s = "b rx" },
    { .x = -34, .y =   -16, .what = DT_LABEL, .s = "b tx" },
    { .x = -22, .y =   -16, .what = DT_LABEL, .s = "link" },
    { .x = -23, .y =   -16, .what = DT_INT,
      .s = "(interface_stat[2].wire_speed/1000000)", .align = RIGHT },

    { .x = -33, .y =   -15, .what = DT_SMBAR,
      .s = "((<if[3].bytes_in*(8*16))/interface_stat[3].wire_speed)",
      .align = RIGHT },
    { .x = -17, .y =   -15, .what = DT_SMBAR,
      .s = "((<if[3].bytes_out*(8*16))/interface_stat[3].wire_speed)",
      .align = LEFT },
    { .x = -25, .y =   -15, .what = DT_STRING, .s = "interface_stat[3].name",
      .align = CENTER },
    { .x = -4, .y =   -14, .what = DT_INT,
      .s = "(<if[3].bytes_in*8)", .align = RIGHT },
    { .x = -34, .y =   -14, .what = DT_INT,
      .s = "(<if[3].bytes_out*8)", .align = RIGHT },
    { .x = -4, .y =   -14, .what = DT_LABEL, .s = "b rx" },
    { .x = -34, .y =   -14, .what = DT_LABEL, .s = "b tx" },
    { .x = -22, .y =   -14, .what = DT_LABEL, .s = "link" },
    { .x = -23, .y =   -14, .what = DT_INT,
      .s = "(interface_stat[3].wire_speed/1000000)", .align = RIGHT },

    { .x = -33, .y =   -13, .what = DT_SMBAR,
      .s = "((<if[4].bytes_in*(8*16))/interface_stat[4].wire_speed)",
      .align = RIGHT },
    { .x = -17, .y =   -13, .what = DT_SMBAR,
      .s = "((<if[4].bytes_out*(8*16))/interface_stat[4].wire_speed)",
      .align = LEFT },
    { .x = -25, .y =   -13, .what = DT_STRING, .s = "interface_stat[4].name",
      .align = CENTER },
    { .x = -4, .y =   -12, .what = DT_INT,
      .s = "(<if[4].bytes_in*8)", .align = RIGHT },
    { .x = -34, .y =   -12, .what = DT_INT,
      .s = "(<if[4].bytes_out*8)", .align = RIGHT },
    { .x = -4, .y =   -12, .what = DT_LABEL, .s = "b rx" },
    { .x = -34, .y =   -12, .what = DT_LABEL, .s = "b tx" },
    { .x = -22, .y =   -12, .what = DT_LABEL, .s = "link" },
    { .x = -23, .y =   -12, .what = DT_INT,
      .s = "(interface_stat[4].wire_speed/1000000)", .align = RIGHT },

    { .x = -33, .y =   -11, .what = DT_SMBAR,
      .s = "((<if[5].bytes_in*(8*16))/interface_stat[5].wire_speed)",
      .align = RIGHT },
    { .x = -17, .y =   -11, .what = DT_SMBAR,
      .s = "((<if[5].bytes_out*(8*16))/interface_stat[5].wire_speed)",
      .align = LEFT },
    { .x = -25, .y =   -11, .what = DT_STRING, .s = "interface_stat[5].name",
      .align = CENTER },
    { .x = -4, .y =   -10, .what = DT_INT,
      .s = "(<if[5].bytes_in*8)", .align = RIGHT },
    { .x = -34, .y =   -10, .what = DT_INT,
      .s = "(<if[5].bytes_out*8)", .align = RIGHT },
    { .x = -4, .y =   -10, .what = DT_LABEL, .s = "b rx" },
    { .x = -34, .y =   -10, .what = DT_LABEL, .s = "b tx" },
    { .x = -22, .y =   -10, .what = DT_LABEL, .s = "link" },
    { .x = -23, .y =   -10, .what = DT_INT,
      .s = "(interface_stat[5].wire_speed/1000000)", .align = RIGHT },

    { .x = -33, .y =   -9, .what = DT_SMBAR,
      .s = "((<if[6].bytes_in*(8*16))/interface_stat[6].wire_speed)",
      .align = RIGHT },
    { .x = -17, .y =   -9, .what = DT_SMBAR,
      .s = "((<if[6].bytes_out*(8*16))/interface_stat[6].wire_speed)",
      .align = LEFT },
    { .x = -25, .y =   -9, .what = DT_STRING, .s = "interface_stat[6].name",
      .align = CENTER },
    { .x = -4, .y =   -8, .what = DT_INT,
      .s = "(<if[6].bytes_in*8)", .align = RIGHT },
    { .x = -34, .y =   -8, .what = DT_INT,
      .s = "(<if[6].bytes_out*8)", .align = RIGHT },
    { .x = -4, .y =   -8, .what = DT_LABEL, .s = "b rx" },
    { .x = -34, .y =   -8, .what = DT_LABEL, .s = "b tx" },
    { .x = -22, .y =   -8, .what = DT_LABEL, .s = "link" },
    { .x = -23, .y =   -8, .what = DT_INT,
      .s = "(interface_stat[6].wire_speed/1000000)", .align = RIGHT },

    { .x = -33, .y =   -7, .what = DT_SMBAR,
      .s = "((<if[7].bytes_in*(8*16))/interface_stat[7].wire_speed)",
      .align = RIGHT },
    { .x = -17, .y =   -7, .what = DT_SMBAR,
      .s = "((<if[7].bytes_out*(8*16))/interface_stat[7].wire_speed)",
      .align = LEFT },
    { .x = -25, .y =   -7, .what = DT_STRING, .s = "interface_stat[7].name",
      .align = CENTER },
    { .x = -4, .y =   -6, .what = DT_INT,
      .s = "(<if[7].bytes_in*8)", .align = RIGHT },
    { .x = -34, .y =   -6, .what = DT_INT,
      .s = "(<if[7].bytes_out*8)", .align = RIGHT },
    { .x = -4, .y =   -6, .what = DT_LABEL, .s = "b rx" },
    { .x = -34, .y =   -6, .what = DT_LABEL, .s = "b tx" },
    { .x = -22, .y =   -6, .what = DT_LABEL, .s = "link" },
    { .x = -23, .y =   -6, .what = DT_INT,
      .s = "(interface_stat[7].wire_speed/1000000)", .align = RIGHT },

    { .x = -33, .y =   -5, .what = DT_SMBAR,
      .s = "((<if[8].bytes_in*(8*16))/interface_stat[8].wire_speed)",
      .align = RIGHT },
    { .x = -17, .y =   -5, .what = DT_SMBAR,
      .s = "((<if[8].bytes_out*(8*16))/interface_stat[8].wire_speed)",
      .align = LEFT },
    { .x = -25, .y =   -5, .what = DT_STRING, .s = "interface_stat[8].name",
      .align = CENTER },
    { .x = -4, .y =   -4, .what = DT_INT,
      .s = "(<if[8].bytes_in*8)", .align = RIGHT },
    { .x = -34, .y =   -4, .what = DT_INT,
      .s = "(<if[8].bytes_out*8)", .align = RIGHT },
    { .x = -4, .y =   -4, .what = DT_LABEL, .s = "b rx" },
    { .x = -34, .y =   -4, .what = DT_LABEL, .s = "b tx" },
    { .x = -22, .y =   -4, .what = DT_LABEL, .s = "link" },
    { .x = -23, .y =   -4, .what = DT_INT,
      .s = "(interface_stat[8].wire_speed/1000000)", .align = RIGHT },

    { .x = -33, .y =   -3, .what = DT_SMBAR,
      .s = "((<if[9].bytes_in*(8*16))/interface_stat[9].wire_speed)",
      .align = RIGHT },
    { .x = -17, .y =   -3, .what = DT_SMBAR,
      .s = "((<if[9].bytes_out*(8*16))/interface_stat[9].wire_speed)",
      .align = LEFT },
    { .x = -25, .y =   -3, .what = DT_STRING, .s = "interface_stat[9].name",
      .align = CENTER },
    { .x = -4, .y =   -2, .what = DT_INT,
      .s = "(<if[9].bytes_in*8)", .align = RIGHT },
    { .x = -34, .y =   -2, .what = DT_INT,
      .s = "(<if[9].bytes_out*8)", .align = RIGHT },
    { .x = -4, .y =   -2, .what = DT_LABEL, .s = "b rx" },
    { .x = -34, .y =   -2, .what = DT_LABEL, .s = "b tx" },
    { .x = -22, .y =   -2, .what = DT_LABEL, .s = "link" },
    { .x = -23, .y =   -2, .what = DT_INT,
      .s = "(interface_stat[9].wire_speed/1000000)", .align = RIGHT },
    { .what = DT_EOL }
};

