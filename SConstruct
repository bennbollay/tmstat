# Running unittests is left as an exercise for the reader, through the
# run_tests.sh shellscript.

env = Environment(CFLAGS= ['-falign-functions=16',
        '-D_REENTRANT',
        '-mtune=opteron',
        '-g',
        '-O2',
        '-fPIC',
        '-Wall',
        '-Werror',
        '-Wpointer-arith',
        '-Wreturn-type',
        '-Wswitch',
        '-Wunused',
        '-Wundef',
        '-Wno-uninitialized',
        '-Wno-format',
        '-Wmissing-prototypes',
        '--std=gnu99',
        '-D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE']
    , LIBPATH='.')

env.Library('tmstat', ['libtmstat.c', 'libtmstat_eval.c'])
env.Program('tmstat', ['tmstat_dash.c', 'd_compress.c', 'd_cpu.c', 'd_summary.c'],
    LIBS=['tmstat', 'curses'])
env.Program('tmctl', ['tmctl.c'], LIBS=['tmstat'])
env.Program('tmstat_test', ['tmstat_test.c'], LIBS=['tmstat'])
env.Library('tmstat_tls', ['tmstat_sandbox.c', 'libtmstat.c', 'libtmstat_eval.c'])

