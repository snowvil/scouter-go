package logger

import (
	"math"
	"math/big"
	"math/bits"
	"strconv"
	"strings"
)

// This file reproduces the JDK 8 sun.misc.FloatingDecimal binary-to-decimal
// conversion (BinaryToASCIIBuffer.dtoa, Steele & White with a symmetric
// stopping test and the developLongDigits integer path) that Float.toString
// and java.text.DigitList use. JDK 8 output is not always the shortest
// round-trip string (e.g. 1e23 -> 9.999999999999999E22, 3.4e10f ->
// 3.3999999E10, Float.MIN_VALUE -> 1.4E-45), so strconv cannot be used.
// The int/long fast paths keep Java's 32/64-bit wraparound; the FDBigInteger
// path is exact big.Int arithmetic.

const (
	expShift    = 52
	fractHOB    = int64(1) << expShift
	maxSmallBin = 62
	minSmallBin = -(63 / 3)
)

var long5Pow = func() [27]int64 {
	var p [27]int64
	p[0] = 1
	for i := 1; i < len(p); i++ {
		p[i] = p[i-1] * 5
	}
	return p
}()

var n5Bits = [...]int{0, 3, 5, 7, 10, 12, 14, 17, 19, 21, 24, 26, 28, 31, 33, 35, 38, 40, 42, 45, 47, 49, 52, 54, 56, 59, 61}

var insignificantDigitsNumber = [...]int{
	0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3,
	4, 4, 4, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7,
	8, 8, 8, 9, 9, 9, 9, 10, 10, 10, 11, 11, 11,
	12, 12, 12, 12, 13, 13, 13, 14, 14, 14,
	15, 15, 15, 15, 16, 16, 16, 17, 17, 17,
	18, 18, 18, 19,
}

// jdkDecimal is the BinaryToASCIIBuffer state after dtoa: value =
// 0.digits x 10^decExp. digits may end in '0' after a roundup carry.
type jdkDecimal struct {
	digits    []byte
	decExp    int
	roundedUp bool // decimalDigitsRoundedUp
	exact     bool // exactDecimalConversion
}

// jdkFloat64 is getBinaryToASCIIConverter(double) for a finite non-zero v.
func jdkFloat64(v float64) jdkDecimal {
	b := math.Float64bits(v)
	fract := int64(b & (1<<52 - 1))
	binExp := int((b >> 52) & 0x7ff)
	var nSig int
	if binExp == 0 {
		lz := bits.LeadingZeros64(uint64(fract))
		shift := lz - (63 - expShift)
		fract <<= shift
		binExp = 1 - shift
		nSig = 64 - lz
	} else {
		fract |= fractHOB
		nSig = expShift + 1
	}
	return jdkDtoa(binExp-1023, fract, nSig)
}

// jdkFloat32 is getBinaryToASCIIConverter(float) for a finite non-zero f.
func jdkFloat32(f float32) jdkDecimal {
	b := math.Float32bits(f)
	fract := int32(b & (1<<23 - 1))
	binExp := int((b >> 23) & 0xff)
	var nSig int
	if binExp == 0 {
		lz := bits.LeadingZeros32(uint32(fract))
		shift := lz - (31 - 23)
		fract <<= shift
		binExp = 1 - shift
		nSig = 32 - lz
	} else {
		fract |= 1 << 23
		nSig = 24
	}
	return jdkDtoa(binExp-127, int64(fract)<<(expShift-23), nSig)
}

func estimateDecExp(fractBits int64, binExp int) int {
	d2 := math.Float64frombits(uint64(1023)<<expShift | uint64(fractBits)&(1<<52-1))
	d := (d2-1.5)*0.289529654 + 0.176091259 + float64(binExp)*0.301029995663981
	dBits := math.Float64bits(d)
	exponent := int((dBits>>expShift)&0x7ff) - 1023
	neg := dBits>>63 != 0
	switch {
	case exponent >= 0 && exponent < 52:
		mask := uint64(1<<52-1) >> exponent
		r := int(int32(((dBits & (1<<52 - 1)) | 1<<52) >> (expShift - exponent)))
		if !neg {
			return r
		}
		if mask&dBits == 0 {
			return -r
		}
		return -r - 1
	case exponent < 0:
		if dBits&^(1<<63) == 0 || !neg {
			return 0
		}
		return -1
	}
	return int(d)
}

func (r *jdkDecimal) developLongDigits(decExponent int, lvalue int64, insignificant int) {
	if insignificant != 0 {
		pow10 := long5Pow[insignificant] << insignificant
		residue := lvalue % pow10
		lvalue /= pow10
		decExponent += insignificant
		if residue >= pow10>>1 {
			lvalue++
		}
	}
	// Strip trailing zeros, then emit the remaining digits.
	c := lvalue % 10
	lvalue /= 10
	for c == 0 {
		decExponent++
		c = lvalue % 10
		lvalue /= 10
	}
	var rev []byte
	for lvalue != 0 {
		rev = append(rev, byte(c)+'0')
		decExponent++
		c = lvalue % 10
		lvalue /= 10
	}
	rev = append(rev, byte(c)+'0')
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	r.digits = rev
	r.decExp = decExponent + 1
}

func jdkDtoa(binExp int, fractBits int64, nSignificantBits int) jdkDecimal {
	var r jdkDecimal
	tailZeros := bits.TrailingZeros64(uint64(fractBits))
	nFractBits := expShift + 1 - tailZeros
	nTinyBits := max(0, nFractBits-binExp-1)
	if binExp <= maxSmallBin && binExp >= minSmallBin && nTinyBits < len(long5Pow) && nFractBits+n5Bits[nTinyBits] < 64 && nTinyBits == 0 {
		insignificant := 0
		if binExp > nSignificantBits {
			if p2 := binExp - nSignificantBits - 1; p2 > 1 && p2 < len(insignificantDigitsNumber) {
				insignificant = insignificantDigitsNumber[p2]
			}
		}
		if binExp >= expShift {
			fractBits <<= binExp - expShift
		} else {
			fractBits = int64(uint64(fractBits) >> (expShift - binExp))
		}
		r.developLongDigits(0, fractBits, insignificant)
		return r
	}
	decExp := estimateDecExp(fractBits, binExp)
	B5 := max(0, -decExp)
	B2 := B5 + nTinyBits + binExp
	S5 := max(0, decExp)
	S2 := S5 + nTinyBits
	M5 := B5
	M2 := B2 - nSignificantBits
	fractBits = int64(uint64(fractBits) >> tailZeros)
	B2 -= nFractBits - 1
	common := min(B2, S2)
	B2 -= common
	S2 -= common
	M2 -= common
	if nFractBits == 1 {
		M2--
	}
	if M2 < 0 {
		B2 -= M2
		S2 -= M2
		M2 = 0
	}
	bitsOf5 := func(n int) int {
		if n < len(n5Bits) {
			return n5Bits[n]
		}
		return n * 3
	}
	Bbits := nFractBits + B2 + bitsOf5(B5)
	tenSbits := S2 + 1 + bitsOf5(S5+1)
	var digits []byte
	var low, high bool
	var lowDiff int64
	switch {
	case Bbits < 32 && tenSbits < 32:
		b := int32(fractBits) * int32(long5Pow[B5]) << B2
		s := int32(long5Pow[S5]) << S2
		m := int32(long5Pow[M5]) << M2
		tens := s * 10
		q := b / s
		b = 10 * (b % s)
		m *= 10
		low = b < m
		high = b+m > tens
		if q == 0 && !high {
			decExp--
		} else {
			digits = append(digits, byte('0'+q))
		}
		if decExp < -3 || decExp >= 8 {
			low, high = false, false
		}
		for !low && !high {
			q = b / s
			b = 10 * (b % s)
			m *= 10
			if m > 0 {
				low = b < m
				high = b+m > tens
			} else {
				low, high = true, true
			}
			digits = append(digits, byte('0'+q))
		}
		lowDiff = int64(b<<1 - tens)
		r.exact = b == 0
	case Bbits < 64 && tenSbits < 64:
		b := fractBits * long5Pow[B5] << B2
		s := long5Pow[S5] << S2
		m := long5Pow[M5] << M2
		tens := s * 10
		q := b / s
		b = 10 * (b % s)
		m *= 10
		low = b < m
		high = b+m > tens
		if q == 0 && !high {
			decExp--
		} else {
			digits = append(digits, byte('0'+q))
		}
		if decExp < -3 || decExp >= 8 {
			low, high = false, false
		}
		for !low && !high {
			q = b / s
			b = 10 * (b % s)
			m *= 10
			if m > 0 {
				low = b < m
				high = b+m > tens
			} else {
				low, high = true, true
			}
			digits = append(digits, byte('0'+q))
		}
		lowDiff = b<<1 - tens
		r.exact = b == 0
	default:
		pow52 := func(p5, p2 int) *big.Int {
			x := new(big.Int).Exp(big.NewInt(5), big.NewInt(int64(p5)), nil)
			return x.Lsh(x, uint(p2))
		}
		S := pow52(S5, S2)
		B := new(big.Int).Mul(big.NewInt(fractBits), pow52(B5, B2))
		M := pow52(M5+1, M2+1)
		tenS := pow52(S5+1, S2+1)
		ten := big.NewInt(10)
		quoRem := func() int {
			q, rem := new(big.Int).QuoRem(B, S, new(big.Int))
			B = rem.Mul(rem, ten)
			return int(q.Int64())
		}
		cmpHigh := func() bool { return tenS.Cmp(new(big.Int).Add(B, M)) <= 0 }
		q := quoRem()
		low = B.Cmp(M) < 0
		high = cmpHigh()
		if q == 0 && !high {
			decExp--
		} else {
			digits = append(digits, byte('0'+q))
		}
		if decExp < -3 || decExp >= 8 {
			low, high = false, false
		}
		for !low && !high {
			q = quoRem()
			M = new(big.Int).Mul(M, ten)
			low = B.Cmp(M) < 0
			high = cmpHigh()
			digits = append(digits, byte('0'+q))
		}
		if high && low {
			lowDiff = int64(new(big.Int).Lsh(B, 1).Cmp(tenS))
		}
		r.exact = B.Sign() == 0
	}
	r.digits = digits
	r.decExp = decExp + 1
	if high {
		if low {
			if lowDiff == 0 {
				if (digits[len(digits)-1] & 1) != 0 {
					r.roundup()
				}
			} else if lowDiff > 0 {
				r.roundup()
			}
		} else {
			r.roundup()
		}
	}
	return r
}

func (r *jdkDecimal) roundup() {
	i := len(r.digits) - 1
	q := r.digits[i]
	if q == '9' {
		for q == '9' && i > 0 {
			r.digits[i] = '0'
			i--
			q = r.digits[i]
		}
		if q == '9' {
			r.decExp++
			r.digits[0] = '1'
			return
		}
	}
	r.digits[i] = q + 1
	r.roundedUp = true
}

// javaFormatString is BinaryToASCIIBuffer.getChars without the sign.
func (r jdkDecimal) javaFormatString() string {
	d, e := string(r.digits), r.decExp
	var sb strings.Builder
	switch {
	case e > 0 && e < 8:
		n := min(len(d), e)
		sb.WriteString(d[:n])
		if n < e {
			sb.WriteString(strings.Repeat("0", e-n))
			sb.WriteString(".0")
		} else {
			sb.WriteByte('.')
			if n < len(d) {
				sb.WriteString(d[n:])
			} else {
				sb.WriteByte('0')
			}
		}
	case e <= 0 && e > -3:
		sb.WriteString("0.")
		sb.WriteString(strings.Repeat("0", -e))
		sb.WriteString(d)
	default:
		sb.WriteByte(d[0])
		sb.WriteByte('.')
		if len(d) > 1 {
			sb.WriteString(d[1:])
		} else {
			sb.WriteByte('0')
		}
		sb.WriteByte('E')
		sb.WriteString(strconv.Itoa(e - 1))
	}
	return sb.String()
}

// javaFloatString is java.lang.Float.toString (JDK 8).
func javaFloatString(f float32) string {
	switch {
	case f != f:
		return "NaN"
	case math.IsInf(float64(f), 1):
		return "Infinity"
	case math.IsInf(float64(f), -1):
		return "-Infinity"
	case f == 0:
		if math.Signbit(float64(f)) {
			return "-0.0"
		}
		return "0.0"
	}
	s := jdkFloat32(f).javaFormatString()
	if f < 0 {
		return "-" + s
	}
	return s
}

// javaDecimalFormat is new java.text.DecimalFormat("#0.0#################")
// .format(double) on JDK 8 with en_US symbols (DoubleValue, DoubleSummary):
// DigitList.set parses the FloatingDecimal Java-format string, keeps at most
// 18 fraction digits with HALF_EVEN (an exact '5' tie is decided by the
// FloatingDecimal rounded-up/exact flags), and DecimalFormat prints at least
// one integer and one fraction digit without grouping, a '-' for any negative
// input including values that round to zero, NaN as U+FFFD and infinity as
// U+221E.
func javaDecimalFormat(v float64) string {
	const maxFraction = 18
	switch {
	case v != v:
		return "�"
	case math.IsInf(v, 1):
		return "∞"
	case math.IsInf(v, -1):
		return "-∞"
	}
	var sb strings.Builder
	if math.Signbit(v) {
		sb.WriteByte('-')
	}
	var digits []byte
	decimalAt := 0
	if v != 0 {
		digits, decimalAt = digitListFixed(jdkFloat64(math.Abs(v)), maxFraction)
	}
	if len(digits) == 0 {
		sb.WriteString("0.0")
		return sb.String()
	}
	switch {
	case decimalAt <= 0:
		sb.WriteByte('0')
	case len(digits) <= decimalAt:
		sb.Write(digits)
		sb.WriteString(strings.Repeat("0", decimalAt-len(digits)))
	default:
		sb.Write(digits[:decimalAt])
	}
	sb.WriteByte('.')
	switch {
	case decimalAt < 0:
		sb.WriteString(strings.Repeat("0", -decimalAt))
		sb.Write(digits)
	case len(digits) > decimalAt:
		sb.Write(digits[decimalAt:])
	default:
		sb.WriteByte('0')
	}
	return sb.String()
}

// digitListFixed is java.text.DigitList.set(isNegative, double, maxFraction,
// fixedPoint=true): it returns the significant digits (no trailing zeros)
// and decimalAt (digits[0] is at 10^(decimalAt-1)).
func digitListFixed(r jdkDecimal, maxFraction int) ([]byte, int) {
	// DigitList re-parses toJavaFormatString(): leading zeros are skipped and
	// zeros the format adds ("100.0", "5.0E-19") become trailing digits.
	s := r.javaFormatString()
	mant, _, _ := strings.Cut(s, "E")
	var digits []byte
	for i := 0; i < len(mant); i++ {
		c := mant[i]
		if c == '.' || (len(digits) == 0 && c == '0') {
			continue
		}
		digits = append(digits, c)
	}
	decimalAt := r.decExp
	shouldRoundUp := func(at int) bool {
		switch {
		case digits[at] > '5':
			return true
		case digits[at] < '5':
			return false
		case at == len(digits)-1:
			if r.roundedUp {
				return false
			}
			if !r.exact {
				return true
			}
			return at > 0 && (digits[at-1]-'0')%2 != 0
		}
		for _, c := range digits[at+1:] {
			if c != '0' {
				return true
			}
		}
		return false
	}
	switch {
	case -decimalAt > maxFraction:
		return nil, decimalAt
	case -decimalAt == maxFraction:
		if shouldRoundUp(0) {
			return []byte{'1'}, decimalAt + 1
		}
		return nil, decimalAt
	}
	for len(digits) > 1 && digits[len(digits)-1] == '0' {
		digits = digits[:len(digits)-1]
	}
	keep := maxFraction + decimalAt
	if keep >= 0 && keep < len(digits) {
		if shouldRoundUp(keep) {
			for {
				keep--
				if keep < 0 {
					digits[0] = '1'
					decimalAt++
					keep = 0
					break
				}
				digits[keep]++
				if digits[keep] <= '9' {
					break
				}
			}
			keep++
		}
		digits = digits[:keep]
		for len(digits) > 1 && digits[len(digits)-1] == '0' {
			digits = digits[:len(digits)-1]
		}
	}
	return digits, decimalAt
}
