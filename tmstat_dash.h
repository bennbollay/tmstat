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
 * Status common definitions.
 *
 */
#ifndef __TMSTAT_H__
#define __TMSTAT_H__

enum align;
enum display_type;
struct display;

/*
 * Alignment direction.
 */
enum align {
    LEFT,           /* Left-justified. */
    RIGHT,          /* Right-justified. */
    CENTER,         /* Center-justified. */
};

/*
 * Display elements.
 */
enum display_type {
    DT_BAR,         /* Percentage bar. */
    DT_DATE,        /* Current time and date. */
    DT_EOL,         /* End of list. */
    DT_INT,         /* Integer field. */
    DT_LABEL,       /* Text label. */
    DT_MEM,        /* Display memory objects in a column. */
    DT_SMBAR,       /* Small percentage bar. */
    DT_STRING,      /* String field. */
};

/*
 * Display element.
 *
 * (x,y)    locations multi-justify: positive left-justifies, negative
 *          right-justifies, and 0 centers.  The same is true for
 *          y values as well (with top, bottom, and center alignment).
 */
struct display {
    int                     x;      /* X location. */
    int                     y;      /* Y location. */
    int                     w;      /* Width. */
    int                     h;      /* Height. */
    enum display_type       what;   /* Display element type. */
    enum align              align;  /* Alignment. */
    const char              *s;     /* Text parameter. */
    const char              *s1;    /* Second text parameter. */
};

extern struct display       display_summary[];  /* Status summary. */
extern struct display       display_compress[]; /* Compress. */
extern struct display       display_cpu[];      /* CPU breakout for CMP */

#endif  /* !__TMSTAT_H__ */
