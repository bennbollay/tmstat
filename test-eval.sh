#
# Unit test scripts for tmstat_eval functionality.
#
# $Id$
#
name=$0
obj_dir=$1
tmctl="$obj_dir/tmctl --base=$obj_dir/test_data -d private"
tmstat_test="$obj_dir/tmstat_test --base=$obj_dir/test_data -d private"

#
# Evaluate and verify expression.
#
e() {
	result=$($tmctl --eval "$1" 2>&1)
	if [ x"$result" != x"$2" ]; then
		echo "$name: $1 returned $result, expected $2" >&2
		exit 1;
	elif [ x$VERBOSE != x ]; then
		echo "$name: $1 == $result"
	fi
}

#
# Create source segment.
#
$tmstat_test --test=eval

#
# Test constant expression evaluation.
#

# Literal integer.
e '1234567890' 							'1234567890'
e '-1234567890' 						'-1234567890'
# Truth operators.
e '0 || 0' '0'; e '0 || 1' '1'; e '1 || 0' '1'; e '1 || 1' '1'
e '0 && 0' '0'; e '0 && 1' '0'; e '1 && 0' '0'; e '1 && 1' '1'
e '0 <  0' '0'; e '0 <  1' '1'; e '1 <  0' '0'; e '1 <  1' '0'
e '0 >  0' '0'; e '0 >  1' '0'; e '1 >  0' '1'; e '1 >  1' '0'
e '0 <= 0' '1'; e '0 <= 1' '1'; e '1 <= 0' '0'; e '1 <= 1' '1'
e '0 >= 0' '1'; e '0 >= 1' '0'; e '1 >= 0' '1'; e '1 >= 1' '1'
e '0 != 0' '0'; e '0 != 1' '1'; e '1 != 0' '1'; e '1 != 1' '0'
e '0 == 0' '1'; e '0 == 1' '0'; e '1 == 0' '0'; e '1 == 1' '1'
# Arithmetic operators.
e '1 + 1' 							'2'
e '3 - 1' 							'2'
e '4 * 4' 							'16'
e '8 / 2'							'4'
e '15 % 4' 							'3'
# Parenthesis.
e '(1234567890)' 						'1234567890'
e '((((((((((1)+1)+1)+1)+1)+1)+1)+1)+1)+1)' 			'10'
e '(1+(1+(1+(1+(1+(1+(1+(1+(1+(1))))))))))'			'10'
e '(1 + 1)' 							'2'
e '(1 + 1) * 2' 						'4'
# Operator precedence.
e '(2 * 5 + 1) + (1 + 2 * 5)'					'22'
e '(10 / 2 + 1) + (1 + 10 / 2)'					'12'
e '(15 % 4 + 1) + (1 + 15 % 4)'					'8'
e '1 + 0 || 0 + 1'						'1'
e '1 + 0 && 0 + 1'						'1'
e '2 * 1 || 1 * 2'						'1'
e '2 * 1 && 1 * 2'						'1'
# Left-to-right association for factors (can't tell with other operators).
e '8 * 1 / 2'							'4'

#
# Test integer segment queries.
#

# Single-row access.
e 'test/foo.a'							'1'
e 'test/foo.b'							'2'
e 'test/foo.c'							'3'
e 'test/foo.a + test/foo.b + test/foo.c'			'6'
# Keyed query.
e 'test/taz(id=1).id'						'1'
e 'test/bar(name="row 1").d'					'1'
# Multi-key query.
e 'test/taz(id=1,g=11,h=111,i=1111).id'				'1'
# Query used in simple expression.
e 'test/taz(id=3).g + test/taz(id=3).h + test/taz(id=3).i'	'3699'
# Query keys from simple constant expressions.
e 'test/taz(id=1+1).id'						'2'
e 'test/taz(id=(1+3)).id'					'4'
# Query keys from results of other queries.
e 'test/taz(id=test/foo.a).i'					'1111'
e 'test/taz(id=test/foo.b).i'					'2222'
e 'test/taz(id=test/foo.c).i'					'3333'
e 'test/taz(id=test/taz(id=test/foo.a).id).i'			'1111'
# Query resulting in 0 rows returns column values of 0.
e 'test/taz(id=3,id=4).id'					'0'
# Index multi-row results.
e 'test/bar[1].d'						'1'
e 'test/taz(id=3)[0].id'					'3'
# Invalid row index test
e 'test/bar[33].d'                                              'tmctl: expression error: col 12: row idx too large'

#
# Test text segment queries.
#

# Single-row access.
e 'test/foo.text'						'Hello, World!'
e '(test/foo.text)'						'Hello, World!'
# Left-side text-and-integer should produce 0 result.
e 'test/foo.text + 1'						'1'
e '1 + test/foo.text'						'1'
e '1 * test/foo.text'						'0'
e 'test/foo.text * 1'						'0'
e '1 || test/foo.text'						'1'
e 'test/foo.text || 1'						'1'
# String used as another query's key.
e 'test/bar(name=test/bar[1].name).d'				'1'
e 'test/bar(name=(test/bar[1].name)).d'				'1'

#
# Functions.
#

# MAX(a, ...) - Return smallest argument.
e 'MAX(1, 2, 3)'						'3'
e 'MAX(3, 2, 1)'						'3'
e 'MAX(3, 1, 2)'						'3'
e 'MAX(-1, 0, 1)'						'1'

# MIN(a, ...) - Return largest argument.
e 'MIN(1, 2, 3)'						'1'
e 'MIN(3, 2, 1)'						'1'
e 'MIN(3, 1, 2)'						'1'
e 'MIN(-1, 0, 1)'						'-1'

#
# Binary/decimal/hexadecimal columns.
#
e 'test/zot(bin="0101:1010 1010:0101 0000:0000 1111:1111").n'   '5'
e 'test/zot(dec="90.165.0.255").n'                              '5'
e 'test/zot(hex="5A:A5:00:FF").n'                               '5'
e 'test/zot(n=5).bin'                                           '0101:1010 1010:0101 0000:0000 1111:1111'
e 'test/zot(n=5).dec'                                           '90.165.0.255'
e 'test/zot(n=5).hex'                                           '5A:A5:00:FF'
