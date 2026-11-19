// Package geoip는 legacy MaxMind GeoIP(.dat, 예: GeoLiteCity.dat) 파일을 읽어 IPv4 주소의 국가 코드·도시를 찾는다.
//
// 원본 Scouter 서버는 com.maxmind.geoip.LookupService(geoip-api 1.3.1)를 GEOIP_MEMORY_CACHE로 열고
// getLocation(InetAddress)를 호출한다. 이 패키지는 그 공개 동작(구조 정보 탐색, binary trie 탐색, City record 해석,
// ISO-8859-1 문자열, 예외 조건)을 형식 지식으로 새로 구현했다. 계약·의도된 차이·상한은 CONTRACT.md에 있다.
//
// DB는 Open 이후 불변이며 Lookup은 여러 goroutine에서 동시에 호출해도 안전하다. 이 패키지는 goroutine을 만들지 않는다.
package geoip

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// MaxFileSize는 Open/FromBytes가 받는 파일 크기 상한(byte)이다. 원본은 파일 전체를 byte[]로 읽으며
// 2 GiB 미만이면 상한이 없다. 실제 GeoLiteCity.dat(legacy)는 수십 MiB이므로 256 MiB로 제한한다.
const MaxFileSize = 256 << 20

// database type(DatabaseInfo 상수). 원본은 구조 정보의 type byte가 106 이상이면 105를 뺀다.
const (
	TypeCountry        = 1
	TypeCityRev1       = 2
	TypeRegionRev1     = 3
	TypeISP            = 4
	TypeOrg            = 5
	TypeCityRev0       = 6
	TypeRegionRev0     = 7
	TypeProxy          = 8
	TypeASNum          = 9
	TypeNetSpeed       = 10
	TypeDomain         = 11
	TypeCountryV6      = 12
	TypeASNumV6        = 21
	TypeISPV6          = 22
	TypeOrgV6          = 23
	TypeDomainV6       = 24
	TypeCityRev1V6     = 30
	TypeCityRev0V6     = 31
	TypeNetSpeedRev1   = 32
	TypeNetSpeedRev1V6 = 33
)

const (
	countryBegin         = 16776960
	stateBeginRev0       = 16700000
	stateBeginRev1       = 16000000
	structureInfoMaxSize = 20
	databaseInfoMaxSize  = 100
	fullRecordLength     = 60
)

// 원본이 같은 입력에서 던지는 예외 class. Error.Java에 들어간다.
const (
	JavaIOException            = "java.io.IOException"
	JavaEOFException           = "java.io.EOFException"
	JavaInvalidDatabase        = "com.maxmind.geoip.InvalidDatabaseException"
	JavaArrayIndexOutOfBounds  = "java.lang.ArrayIndexOutOfBoundsException"
	JavaIndexOutOfBounds       = "java.lang.IndexOutOfBoundsException"
	JavaBufferUnderflow        = "java.nio.BufferUnderflowException"
	JavaNullPointer            = "java.lang.NullPointerException"
	javaInvalidDatabaseInfoMsg = "Error reading database info"
)

// Error는 Open/Lookup 실패다. Java는 같은 입력에서 원본 LookupService가 던지는 예외 class이고,
// Go에만 있는 실패(파일 열기·크기 상한)는 빈 문자열이다.
type Error struct {
	Op   string // "open" 또는 "lookup"
	Java string
	Msg  string
	Err  error
}

func (e *Error) Error() string {
	s := "geoip " + e.Op + ": " + e.Msg
	if e.Java != "" {
		s += " (java: " + e.Java + ")"
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

func (e *Error) Unwrap() error { return e.Err }

func openErr(java, msg string) error   { return &Error{Op: "open", Java: java, Msg: msg} }
func lookupErr(java, msg string) error { return &Error{Op: "lookup", Java: java, Msg: msg} }

// Location은 원본 com.maxmind.geoip.Location의 값이다. 문자열 ""는 원본 null이다
// (원본은 길이 0 문자열을 만들지 않고 null을 돌려준다). countryName은 제공하지 않는다.
type Location struct {
	CountryCode string // countryCode 표(256개) 값. index 0은 "--"
	Region      string
	City        string // ISO-8859-1로 해석한 문자열(UTF-8 byte가 들어 있어도 byte마다 한 문자)
	PostalCode  string
	Latitude    float32
	Longitude   float32
	MetroCode   int32 // City rev1이고 CountryCode == "US"일 때만 설정(dma_code와 같은 값)
	AreaCode    int32
}

// DB는 메모리에 올린 legacy GeoIP 파일이다. 생성 후 불변이다.
type DB struct {
	buf          []byte
	dbType       int8
	segments     int32
	hasSegments  bool // false는 원본 databaseSegments == null(알 수 없는 type)
	recordLength int32
	info         string
}

// Open은 path의 파일 전체를 메모리에 읽어 DB를 만든다(원본 GEOIP_MEMORY_CACHE). 정규 파일만 받고
// MaxFileSize를 넘으면 거절한다. 형식 오류는 원본 생성자가 예외를 던지는 조건과 같을 때 *Error를 돌려준다.
func Open(path string) (*DB, error) {
	// FIFO 등은 open 자체가 대기할 수 있으므로 열기 전에 한 번, 연 뒤 같은 fd로 다시 확인한다.
	if fi, err := os.Stat(path); err != nil {
		return nil, &Error{Op: "open", Msg: "stat file", Err: err}
	} else if !fi.Mode().IsRegular() {
		return nil, &Error{Op: "open", Msg: fmt.Sprintf("not a regular file (mode %s)", fi.Mode())}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, &Error{Op: "open", Msg: "open file", Err: err}
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, &Error{Op: "open", Msg: "stat file", Err: err}
	}
	if !fi.Mode().IsRegular() {
		return nil, &Error{Op: "open", Msg: fmt.Sprintf("not a regular file (mode %s)", fi.Mode())}
	}
	if fi.Size() > MaxFileSize {
		return nil, &Error{Op: "open", Msg: fmt.Sprintf("file size %d exceeds limit %d", fi.Size(), MaxFileSize)}
	}
	// 원본처럼 열 때의 길이만큼 정확히 읽는다(readFully). 그 사이 파일이 줄면 EOF 오류, 늘어난 부분은 읽지 않는다.
	buf := make([]byte, fi.Size())
	if _, err := io.ReadFull(f, buf); err != nil {
		java := JavaIOException
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			java = JavaEOFException
		}
		return nil, &Error{Op: "open", Java: java, Msg: "read file", Err: err}
	}
	return FromBytes(buf)
}

// FromBytes는 이미 메모리에 있는 .dat 내용으로 DB를 만든다. b의 소유권은 DB로 넘어가며 호출자는 이후 b를 수정하면 안 된다.
func FromBytes(b []byte) (*DB, error) {
	if len(b) > MaxFileSize {
		return nil, &Error{Op: "open", Msg: fmt.Sprintf("size %d exceeds limit %d", len(b), MaxFileSize)}
	}
	db := &DB{buf: b, dbType: TypeCountry}
	n := len(b)
	// 원본 init(): 파일 끝 3 byte부터 1 byte씩 앞으로 이동하며 FF FF FF를 최대 20번 찾는다.
	// 불일치마다 seek(pos-1)을 하므로 pos가 0에서 불일치하면 마지막 시도여도 음수 seek 예외다.
	p := n - 3
	if p < 0 {
		return nil, openErr(JavaIOException, "negative seek offset: file shorter than 3 bytes")
	}
	found := false
	for i := 0; i < structureInfoMaxSize; i++ {
		if b[p] == 0xff && b[p+1] == 0xff && b[p+2] == 0xff {
			found = true
			if p+3 >= n {
				return nil, openErr(JavaEOFException, "database type byte missing after structure delimiter")
			}
			t := int8(b[p+3])
			if t >= 106 {
				t -= 105
			}
			db.dbType = t
			switch t {
			case TypeRegionRev0:
				db.segments, db.hasSegments, db.recordLength = stateBeginRev0, true, 3
			case TypeRegionRev1:
				db.segments, db.hasSegments, db.recordLength = stateBeginRev1, true, 3
			case TypeCityRev0, TypeCityRev1, TypeOrg, TypeOrgV6, TypeISP, TypeISPV6, TypeDomain, TypeDomainV6,
				TypeASNum, TypeASNumV6, TypeNetSpeedRev1, TypeNetSpeedRev1V6, TypeCityRev0V6, TypeCityRev1V6:
				db.recordLength = 3
				switch t {
				case TypeOrg, TypeOrgV6, TypeISP, TypeISPV6, TypeDomain, TypeDomainV6:
					db.recordLength = 4
				}
				if p+7 > n {
					return nil, openErr(JavaEOFException, "segment bytes missing after database type")
				}
				db.segments = int32(b[p+4]) | int32(b[p+5])<<8 | int32(b[p+6])<<16
				db.hasSegments = true
			}
			break
		}
		p--
		if p < 0 {
			return nil, openErr(JavaIOException, "negative seek offset while searching structure info")
		}
	}
	switch db.dbType {
	case TypeCountry, TypeCountryV6, TypeProxy, TypeNetSpeed:
		db.segments, db.hasSegments, db.recordLength = countryBegin, true, 3
	}
	info, err := databaseInfo(b, found, p)
	if err != nil {
		return nil, err
	}
	db.info = info
	return db, nil
}

// databaseInfo는 원본 getDatabaseInfo()(GEOIP_MEMORY_CACHE 생성자에서 호출)를 따른다. delimiter가 있으면 그 3 byte 앞,
// 없으면 파일 끝 3 byte에서 시작해 00 00 00을 최대 100번 뒤로 찾고, 찾으면 그 뒤 i byte를 ISO-8859-1로 읽는다.
// 음수 seek는 IOException을 감싼 InvalidDatabaseException이다.
func databaseInfo(b []byte, found bool, delim int) (string, error) {
	start := len(b) - 3
	if found {
		start = delim - 3
	}
	if start < 0 {
		return "", openErr(JavaInvalidDatabase, javaInvalidDatabaseInfoMsg+": negative seek offset")
	}
	for i := 0; i < databaseInfoMaxSize; i++ {
		q := start - i
		if b[q] == 0 && b[q+1] == 0 && b[q+2] == 0 {
			return latin1(b[q+3 : q+3+i]), nil
		}
		if q-1 < 0 {
			return "", openErr(JavaInvalidDatabase, javaInvalidDatabaseInfoMsg+": negative seek offset")
		}
	}
	return "", nil
}

// Type은 105 보정 후 database type이다(원본 databaseType byte, 음수 가능).
func (db *DB) Type() int { return int(db.dbType) }

// Info는 원본 DatabaseInfo 문자열(ISO-8859-1)이다. 찾지 못하면 "".
func (db *DB) Info() string { return db.info }

// Size는 메모리에 올린 byte 수다.
func (db *DB) Size() int { return len(db.buf) }

// Lookup은 원본 getLocation(InetAddress)와 같다. found=false, err=nil은 원본 null(미등록)이다.
// err != nil은 원본이 예외를 던지는 경우이며 *Error.Java에 그 class가 있다. Lookup은 할당 외 부작용이 없다.
func (db *DB) Lookup(ip [4]byte) (Location, bool, error) {
	x, err := db.seek(binary.BigEndian.Uint32(ip[:]))
	if err != nil {
		return Location{}, false, err
	}
	return db.readCityRecord(x)
}

// seek는 원본 seekCountry(long)이다. 모든 산술은 Java int(32bit, wrap)와 같게 한다.
func (db *DB) seek(ipnum uint32) (int32, error) {
	rl := db.recordLength
	var off int32
	for depth := 31; depth >= 0; depth-- {
		// readNode: System.arraycopy(dbbuffer, 2*rl*off, buf, 0, 2*rl)
		src := 2 * rl * off
		end := int64(src) + int64(2*rl)
		if src < 0 || end > int64(len(db.buf)) {
			return 0, lookupErr(JavaArrayIndexOutOfBounds, fmt.Sprintf("trie node offset %d outside database", off))
		}
		if !db.hasSegments {
			return 0, lookupErr(JavaNullPointer, fmt.Sprintf("database type %d has no segments", db.dbType))
		}
		node := db.buf[src:end]
		var x int32
		base := int32(0)
		if ipnum>>uint(depth)&1 == 1 {
			base = rl
		}
		for j := int32(0); j < rl; j++ {
			x += int32(node[base+j]) << (8 * j)
		}
		if x >= db.segments {
			return x, nil
		}
		off = x
	}
	return 0, lookupErr(JavaInvalidDatabase, fmt.Sprintf("Error seeking country while searching for %d", ipnum))
}

// readCityRecord는 원본 readCityRecord(int)이다. record 위치는 x + (2*rl-1)*segments이고 최대 60 byte를 본다.
func (db *DB) readCityRecord(x int32) (Location, bool, error) {
	if x == db.segments {
		return Location{}, false, nil
	}
	ptr := x + (2*db.recordLength-1)*db.segments
	n := len(db.buf)
	if ptr < 0 || int64(ptr) > int64(n) {
		return Location{}, false, lookupErr(JavaIndexOutOfBounds, fmt.Sprintf("record pointer %d outside database", ptr))
	}
	rec := db.buf[ptr:min(n, int(ptr)+fullRecordLength)]
	if len(rec) == 0 {
		return Location{}, false, lookupErr(JavaBufferUnderflow, "empty record at end of database")
	}
	var loc Location
	loc.CountryCode = countryCodes[rec[0]]
	pos := 1
	loc.Region, pos = readString(rec, pos)
	loc.City, pos = readString(rec, pos)
	loc.PostalCode, pos = readString(rec, pos)
	var v int32
	var err error
	if v, pos, err = read3(rec, pos, "angle"); err != nil {
		return Location{}, false, err
	}
	loc.Latitude = toAngle(v)
	if v, pos, err = read3(rec, pos, "angle"); err != nil {
		return Location{}, false, err
	}
	loc.Longitude = toAngle(v)
	if db.dbType == TypeCityRev1 && loc.CountryCode == "US" {
		if v, _, err = read3(rec, pos, "metro area"); err != nil {
			return Location{}, false, err
		}
		loc.MetroCode = v / 1000
		loc.AreaCode = v % 1000
	}
	return loc, true, nil
}

// readString은 원본 readString(ByteBuffer)이다. NUL(소비됨)이나 buffer 끝까지 읽고, 끝까지 NUL이 없으면
// 마지막 byte를 버린다(원본 position-1 계산). 길이 0이면 null("")이다.
func readString(rec []byte, pos int) (string, int) {
	i := pos
	for i < len(rec) {
		c := rec[i]
		i++
		if c == 0 {
			break
		}
	}
	end := i - 1
	if end > pos {
		return latin1(rec[pos:end]), end + 1
	}
	return "", end + 1
}

func read3(rec []byte, pos int, what string) (int32, int, error) {
	if len(rec)-pos < 3 {
		return 0, pos, lookupErr(JavaInvalidDatabase, "Unexpected end of data record when reading "+what)
	}
	return int32(rec[pos]) | int32(rec[pos+1])<<8 | int32(rec[pos+2])<<16, pos + 3, nil
}

// toAngle은 원본 readAngle: (float)(double)v / 10000f - 180f. 명시적 float32 변환으로 단계마다 단정도 반올림한다.
func toAngle(v int32) float32 {
	q := float32(v) / float32(10000)
	return float32(q - float32(180))
}

// latin1은 ISO-8859-1 decode다(byte b -> U+00bb). 원본 charset은 ISO-8859-1 고정이다.
func latin1(b []byte) string {
	ascii := true
	for _, c := range b {
		if c >= 0x80 {
			ascii = false
			break
		}
	}
	if ascii {
		return string(b)
	}
	r := make([]rune, len(b))
	for i, c := range b {
		r[i] = rune(c)
	}
	return string(r)
}
