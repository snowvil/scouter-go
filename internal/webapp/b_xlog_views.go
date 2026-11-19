package webapp

import (
	"context"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// bSXLog is model/scouter/SXLog. Jackson orders field-backed properties first;
// the Lombok getter getXType() maps to property "xtype", which no field backs,
// so it is serialized last. Hashes are raw (not dictionary-resolved).
type bSXLog struct {
	EndTime            int64   `json:"endTime"`
	ObjHash            int32   `json:"objHash"`
	Service            int32   `json:"service"`
	Txid               int64   `json:"txid"`
	ThreadNameHash     int32   `json:"threadNameHash"`
	Caller             int64   `json:"caller"`
	Gxid               int64   `json:"gxid"`
	Elapsed            int32   `json:"elapsed"`
	Error              int32   `json:"error"`
	CPU                int32   `json:"cpu"`
	SQLCount           int32   `json:"sqlCount"`
	SQLTime            int32   `json:"sqlTime"`
	Ipaddr             string  `json:"ipaddr"`
	Kbytes             int32   `json:"kbytes"`
	Userid             int64   `json:"userid"`
	UserAgent          int32   `json:"userAgent"`
	Referrer           int32   `json:"referrer"`
	Group              int32   `json:"group"`
	ApicallCount       int32   `json:"apicallCount"`
	ApicallTime        int32   `json:"apicallTime"`
	CountryCode        *string `json:"countryCode"`
	City               int32   `json:"city"`
	Login              int32   `json:"login"`
	Desc               int32   `json:"desc"`
	HasDump            int8    `json:"hasDump"`
	Text1              *string `json:"text1"`
	Text2              *string `json:"text2"`
	Text3              *string `json:"text3"`
	Text4              *string `json:"text4"`
	Text5              *string `json:"text5"`
	QueuingHostHash    int32   `json:"queuingHostHash"`
	QueuingTime        int32   `json:"queuingTime"`
	Queuing2ndHostHash int32   `json:"queuing2ndHostHash"`
	Queuing2ndTime     int32   `json:"queuing2ndTime"`
	ProfileCount       int32   `json:"profileCount"`
	ProfileSize        int32   `json:"profileSize"`
	XType              int32   `json:"xtype"`
}

// bXLogText returns the Java XLogPack String field: null when the append
// group carrying it was absent on the wire (TailVersion below minTail).
func bXLogText(p *protocol.XLogPack, minTail byte, v string) *string {
	if p.TailVersion < minTail {
		return nil
	}
	return &v
}

func bToSXLog(p *protocol.XLogPack) bSXLog {
	return bSXLog{
		EndTime: p.EndTime, ObjHash: p.ObjHash, Service: p.Service, Txid: p.TxID,
		ThreadNameHash: p.ThreadNameHash, Caller: p.Caller, Gxid: p.GxID, Elapsed: p.Elapsed,
		Error: p.Error, CPU: p.CPU, SQLCount: p.SQLCount, SQLTime: p.SQLTime,
		Ipaddr: bIPString(p.IPAddress), Kbytes: p.KBytes, Userid: p.UserID,
		UserAgent: p.UserAgent, Referrer: p.Referer, Group: p.Group,
		ApicallCount: p.APICallCount, ApicallTime: p.APICallTime,
		CountryCode: bXLogText(p, 1, p.CountryCode), City: p.City,
		Login: p.Login, Desc: p.Desc, HasDump: int8(p.HasDump),
		Text1: bXLogText(p, 7, p.Text1), Text2: bXLogText(p, 7, p.Text2),
		Text3: bXLogText(p, 9, p.Text3), Text4: bXLogText(p, 9, p.Text4), Text5: bXLogText(p, 9, p.Text5),
		QueuingHostHash: p.QueuingHostHash, QueuingTime: p.QueuingTime,
		Queuing2ndHostHash: p.Queuing2ndHostHash, Queuing2ndTime: p.Queuing2ndTime,
		ProfileCount: p.ProfileCount, ProfileSize: p.ProfileSize, XType: int32(p.XType),
	}
}

// bXLogData is model/XLogData; dictionary hashes are resolved to text.
// getXLogType() maps to property "xlogType" (no backing field) -> last.
type bXLogData struct {
	EndTime         int64   `json:"endTime"`
	ObjHash         int32   `json:"objHash"`
	Service         string  `json:"service"`
	Txid            int64   `json:"txid"`
	ThreadName      string  `json:"threadName"`
	Caller          int64   `json:"caller"`
	Gxid            int64   `json:"gxid"`
	Elapsed         int32   `json:"elapsed"`
	Error           string  `json:"error"`
	CPU             int32   `json:"cpu"`
	SQLCount        int32   `json:"sqlCount"`
	SQLTime         int32   `json:"sqlTime"`
	IPAddr          string  `json:"ipAddr"`
	AllocatedMemory int32   `json:"allocatedMemory"`
	InternalID      int64   `json:"internalId"`
	UserAgent       string  `json:"userAgent"`
	Referrer        string  `json:"referrer"`
	Group           string  `json:"group"`
	ApicallCount    int32   `json:"apicallCount"`
	ApicallTime     int32   `json:"apicallTime"`
	CountryCode     *string `json:"countryCode"`
	City            string  `json:"city"`
	Login           string  `json:"login"`
	Desc            string  `json:"desc"`
	HasDump         int8    `json:"hasDump"`
	Text1           *string `json:"text1"`
	Text2           *string `json:"text2"`
	Text3           *string `json:"text3"`
	Text4           *string `json:"text4"`
	Text5           *string `json:"text5"`
	QueuingHost     string  `json:"queuingHost"`
	QueuingTime     int32   `json:"queuingTime"`
	Queuing2ndHost  string  `json:"queuing2ndHost"`
	Queuing2ndTime  int32   `json:"queuing2ndTime"`
	XLogType        string  `json:"xlogType"`
}

// bXLogTypeName is XLogTypes.Type.of(xType).name().
func bXLogTypeName(x byte) string {
	switch x {
	case 0:
		return "WEB_SERVICE"
	case 1:
		return "APP_SERVICE"
	case 2:
		return "BACK_THREAD"
	case 3:
		return "ASYNCSERVLET_DISPATCHED_SERVICE"
	case 4:
		return "BACK_THREAD2"
	case 5:
		return "ZIPKIN_SPAN"
	}
	return "UNKNOWN"
}

// bToXLogData mirrors XLogData.of. Java pre-loads the hashes with
// GET_TEXT_ANY_TYPE (no date), so the lookups pass an empty date.
func (s *Server) bToXLogData(ctx context.Context, p *protocol.XLogPack) bXLogData {
	t := func(typ string, h int32) string { return s.text(ctx, "", typ, h) }
	return bXLogData{
		EndTime: p.EndTime, ObjHash: p.ObjHash, Service: t("service", p.Service), Txid: p.TxID,
		ThreadName: t("hmsg", p.ThreadNameHash), Caller: p.Caller, Gxid: p.GxID, Elapsed: p.Elapsed,
		Error: t("error", p.Error), CPU: p.CPU, SQLCount: p.SQLCount, SQLTime: p.SQLTime,
		IPAddr: bIPString(p.IPAddress), AllocatedMemory: p.KBytes, InternalID: p.UserID,
		UserAgent: t("ua", p.UserAgent), Referrer: t("referer", p.Referer), Group: t("group", p.Group),
		ApicallCount: p.APICallCount, ApicallTime: p.APICallTime,
		CountryCode: bXLogText(p, 1, p.CountryCode), City: t("city", p.City),
		Login: t("login", p.Login), Desc: t("desc", p.Desc), HasDump: int8(p.HasDump),
		Text1: bXLogText(p, 7, p.Text1), Text2: bXLogText(p, 7, p.Text2),
		Text3: bXLogText(p, 9, p.Text3), Text4: bXLogText(p, 9, p.Text4), Text5: bXLogText(p, 9, p.Text5),
		QueuingHost: t("hmsg", p.QueuingHostHash), QueuingTime: p.QueuingTime,
		Queuing2ndHost: t("hmsg", p.Queuing2ndHostHash), Queuing2ndTime: p.Queuing2ndTime,
		XLogType: bXLogTypeName(p.XType),
	}
}
