package ethdb

import "github.com/ethereum/go-ethereum/metrics"

var (
	// for perf performance
	PerfDBGetTimer        = metrics.NewRegisteredTimer("perf/db/get/time", nil)
	PerfDBPutTimer        = metrics.NewRegisteredTimer("perf/db/put/time", nil)
	PerfDBBatchWriteTimer = metrics.NewRegisteredTimer("perf/db/batch/write/time", nil)
)
