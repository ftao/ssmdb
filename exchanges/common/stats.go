package common

import (
	"expvar"
	"time"
)

func GetUinxMillisecond() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

type StatsVars struct {
	LastRecv     *expvar.Int
	LastSend     *expvar.Int
	SendCount    *expvar.Int
	RecvCount    *expvar.Int
	CountByTopic *expvar.Map
}

func NewStatsVars(prefix string) *StatsVars {
	countByTopic := expvar.NewMap(prefix + ".count_by_topic")
	countByTopic.Init()
	return &StatsVars{
		LastRecv:     expvar.NewInt(prefix + ".last_recv"),
		LastSend:     expvar.NewInt(prefix + ".last_send"),
		SendCount:    expvar.NewInt(prefix + ".send_count"),
		RecvCount:    expvar.NewInt(prefix + ".recv_count"),
		CountByTopic: countByTopic,
	}
}

func (st *StatsVars) UpdateOnRecv(topic string) {
	st.LastRecv.Set(GetUinxMillisecond())
	st.RecvCount.Add(1)
	st.CountByTopic.Add(topic, 1)
}

func (st *StatsVars) UpdateOnSend() {
	st.LastSend.Set(GetUinxMillisecond())
	st.SendCount.Add(1)
}
