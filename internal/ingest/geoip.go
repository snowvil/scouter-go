package ingest

import (
	"container/list"
	"encoding/binary"
	"log"

	"github.com/snowvil/scouter-go/internal/geoip"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

// GeoIPLookup is the GeoIP city database as seen by the XLog path
// (GISDataUtil.getGeoIPInfos); *geoip.DB implements it. found=false with a
// nil error is a Java null Location (address not in the database). An empty
// city is replaced by "unknown". Lookup is called under the engine lock and
// must only read the in-memory database.
type GeoIPLookup interface {
	Lookup(ip [4]byte) (geoip.Location, bool, error)
}

const (
	geoCacheMax = 10000  // locationCache and citySet setMax(10000)
	textCity    = "city" // TextTypes.CITY
	// geoErrorLogInterval is Logger.println(id, 10, ...): one line per 10s.
	geoErrorLogInterval = 10000
)

type geoLocation struct {
	ip            int32
	country, city string
}

// geoState is GeoIpUtil's two FIFO caches: IntKeyLinkedMap/IntLinkedSet with
// setMax put only absent keys at the tail, never reorder on get, and drop the
// head on overflow.
type geoState struct {
	locations     map[int32]*list.Element
	locationOrder list.List
	cities        map[int32]*list.Element
	cityOrder     list.List
	// putText is the TextPermWR.add target (Engine.putGeoText); tests
	// observe it.
	putText      func(date string, p *protocol.TextPack) error
	nextErrorLog int64
}

func newGeoState(putText func(string, *protocol.TextPack) error) *geoState {
	return &geoState{locations: map[int32]*list.Element{}, cities: map[int32]*list.Element{}, putText: putText}
}

// applyGeoIPLocked is XLogCore.calc's "if (conf.geoip_enabled)
// GeoIpUtil.setNationAndCity(m)": after XLogGroupUtil.process and before
// XLogGroupPerf.add, the plugin hook and the store write. A nil lookup is
// GISDataUtil.isOk() == false (no database loaded): nothing happens.
//
// It never fails the XLog: a lookup error or a failed city text write skips
// only the enrichment (country/city stay as received, nothing is cached) and
// is counted and logged at most once per 10s (CONTRACT.md).
func (e *Engine) applyGeoIPLocked(p *protocol.XLogPack, date string) {
	if !e.settings().GeoIP {
		return
	}
	lookup := e.config.GeoIP
	if len(p.IPAddress) != 4 || lookup == nil || p.CountryCode != "" || privateIP(p.IPAddress) {
		return
	}
	ipInt := int32(binary.BigEndian.Uint32(p.IPAddress)) // DataInputX.toInt(ipaddr, 0)
	if ipInt == 0 {
		return
	}
	g := e.geo
	var loc *geoLocation
	if el := g.locations[ipInt]; el != nil {
		loc = el.Value.(*geoLocation)
	} else {
		found, ok, err := lookup.Lookup([4]byte(p.IPAddress))
		if err != nil {
			// Java: the RuntimeException leaves calc and the XLogCore loop
			// drops the whole XLog. Go keeps the XLog without the GeoIP step.
			e.geoFailed("geoip lookup failed", err)
			return
		}
		if !ok {
			return
		}
		city := found.City
		if city == "" {
			city = "unknown"
		}
		loc = &geoLocation{ip: ipInt, country: found.CountryCode, city: city}
		cityHash := storage.CounterHash(city)
		if g.cities[cityHash] == nil {
			// TextPermWR.add(TextTypes.CITY, ...). Java queues the write and
			// caches first; Go queues first so a refused write is retried by
			// the next miss instead of being hidden behind the caches. The
			// XLog is kept unenriched so no stored XLog carries a city hash
			// whose text was refused. A full text queue is waited for after
			// the engine lock (putGeoText); a drop there is the text queue's,
			// as before.
			if err := g.putText(date, &protocol.TextPack{XType: textCity, Hash: cityHash, Text: city}); err != nil {
				e.geoTextErrors.Add(1)
				e.geoFailed("geoip city text write failed", err)
				return
			}
		}
		g.putLocation(loc)
		g.putCity(cityHash)
	}
	p.CountryCode = loc.country
	p.City = storage.CounterHash(loc.city)
	if p.TailVersion < 1 {
		// Java always writes every XLog field; the Go codec keeps the received
		// append groups, and countryCode/city live in group 1.
		p.TailVersion = 1
	}
}

// putGeoText is the city text write (geoState.putText): validated now, a
// refused text failing the enrichment, and queued like the other texts of
// the collection path. A write submitted after the engine lock that fails
// (closed store) is counted as GeoIPErrors and GeoIPTextErrors only: the XLog
// already carries the city.
func (e *Engine) putGeoText(date string, p *protocol.TextPack) error {
	return e.putTextLocked(date, p, e.geoTextSubmitFailed)
}

func (e *Engine) geoTextSubmitFailed(error) {
	e.geoErrors.Add(1)
	e.geoTextErrors.Add(1)
}

// geoFailed counts a skipped enrichment and logs at most one line per 10s
// (Java Logger.println(id, 10, ...)); the IP is never logged.
func (e *Engine) geoFailed(what string, err error) {
	n := e.geoErrors.Add(1)
	g := e.geo
	if now := e.config.Clock().UnixMilli(); now >= g.nextErrorLog {
		g.nextErrorLog = now + geoErrorLogInterval
		log.Printf("%s (XLog kept without country/city, %d GeoIP errors so far): %v", what, n, err)
	}
}

func (g *geoState) putLocation(loc *geoLocation) {
	if g.locations[loc.ip] != nil {
		return
	}
	if len(g.locations) >= geoCacheMax {
		head := g.locationOrder.Front()
		delete(g.locations, head.Value.(*geoLocation).ip)
		g.locationOrder.Remove(head)
	}
	g.locations[loc.ip] = g.locationOrder.PushBack(loc)
}

func (g *geoState) putCity(hash int32) {
	if g.cities[hash] != nil {
		return
	}
	if len(g.cities) >= geoCacheMax {
		head := g.cityOrder.Front()
		delete(g.cities, head.Value.(int32))
		g.cityOrder.Remove(head)
	}
	g.cities[hash] = g.cityOrder.PushBack(hash)
}

// privateIP is GeoIpUtil.isPrivateIp. The first octet is masked (& 0xff), the
// second is compared as a Java signed byte: 172.16-31 matches, while
// "ip(1) == 168" can never hold for a byte (-128..127), so 192.168.x.x is NOT
// treated as private and is looked up.
func privateIP(ip []byte) bool {
	second := javaByte(ip[1])
	switch ip[0] {
	case 127, 10:
		return true
	case 172:
		return second >= 16 && second <= 31
	case 192:
		return second == 168
	}
	return false
}

func javaByte(b byte) int { return int(int8(b)) }

// SetGeoIP replaces the GeoIP database (GISDataUtil.load after a
// geoip_data_city_file change). Unlike Java, the IP->Location cache is
// cleared so the new file answers every address; the city text set is kept
// because it only suppresses duplicate dictionary writes. nil disables the
// step (isOk false).
func (e *Engine) SetGeoIP(lookup GeoIPLookup) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.config.GeoIP = lookup
	clear(e.geo.locations)
	e.geo.locationOrder.Init()
}
