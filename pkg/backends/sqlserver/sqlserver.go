package sqlserver

// PSEUDO CODE
// newclientfromviper
// -> calls newsqlserverclient
// -> check configs, establish connection
// -> check existence of tables, maybe validate with some same queries (SET PARSEONLY ON)
// ->
import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/util"
	"github.com/atlassian/gostatsd/pkg/transport"
)

const (
	// BackendName is the name of this backend.
	BackendName = "sqlserver"
	// DefaultAddress is the default address of Graphite server.
	DefaultAddress  = "localhost"
	DefaultPort     = 1433
	DefaultUser     = ""
	DefaultPassword = ""
	DefaultDatabase = ""
	// DefaultDialTimeout is the default net.Dial timeout.
	DefaultDialTimeout = 5 * time.Second
	// DefaultWriteTimeout is the default socket write timeout.
	DefaultWriteTimeout = 30 * time.Second
	// DefaultGlobalPrefix is the default global prefix.
	DefaultGlobalPrefix = "stats"
	// DefaultPrefixCounter is the default counters prefix.
	DefaultPrefixCounter = "counters"
	// DefaultPrefixTimer is the default timers prefix.
	DefaultPrefixTimer = "timers"
	// DefaultPrefixGauge is the default gauges prefix.
	DefaultPrefixGauge = "gauges"
	// DefaultPrefixSet is the default sets prefix.
	DefaultPrefixSet = "sets"
	// DefaultGlobalSuffix is the default global suffix.
	DefaultGlobalSuffix = ""
	// DefaultMode controls whether to use legacy namespace, no tags, or tags
	DefaultMode = "tags"
)

var creationScriptsByTable = map[string]string{
	"counters_statistics": `
		DROP TABLE IF EXISTS counters_statistics;
		CREATE TABLE counters_statistics (
			"timestamp" BIGINT NOT NULL,
			"name" varchar(255) NOT NULL,
			value BIGINT NOT NULL,
			CONSTRAINT counters_statistics_pkey PRIMARY KEY ("timestamp", name)
		)`,
	"gauges_statistics": `
		DROP TABLE IF EXISTS gauges_statistics;
		CREATE TABLE gauges_statistics (
			"timestamp" BIGINT NOT NULL,
			"name" varchar(255) NOT NULL,
			value BIGINT NOT NULL,
			CONSTRAINT gauges_statistics_pkey PRIMARY KEY ("timestamp", name)
		)`,
	"sets_statistics": `
		DROP TABLE IF EXISTS sets_statistics;
		CREATE TABLE sets_statistics (
			"timestamp" BIGINT NOT NULL,
			"name" varchar(255) NOT NULL,
			value BIGINT NOT NULL,
			CONSTRAINT sets_statistics_pkey PRIMARY KEY ("timestamp", name)
		)`,
	"timers_statistics": `
		DROP TABLE IF EXISTS timers_statistics;

		CREATE TABLE timers_statistics (
			id BIGINT NOT NULL IDENTITY,
			"timestamp" BIGINT NOT NULL,
			"name" varchar(255) NOT NULL,
			value BIGINT NOT NULL,
			CONSTRAINT timers_statistics_pkey PRIMARY KEY (id)
		)`,
}

// Client is an object that is used to send messages to a Graphite server's TCP interface.
type Client struct {
	dbHandle         sql.DB
	dbName           string
	disabledSubtypes gostatsd.TimerSubtypes
}

func (client *Client) Run(ctx context.Context) {
	// client.sender.Run(ctx)
}

func executeStatement(stmt *sql.Stmt) int64 {
	result, err := stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	rowCount, _ := result.RowsAffected()
	return rowCount
}

// SendMetricsAsync flushes the metrics to the Graphite server, preparing payload synchronously but doing the send asynchronously.
func (client *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	fmt.Printf("calling send metrics async\n")
	fmt.Print(metrics)

	txn, err := client.dbHandle.Begin()
	if err != nil {
		log.Fatal(err.Error())
	}

	stmt, err := txn.Prepare(mssql.CopyIn("counters", mssql.BulkOptions{}, "timestamp", "name", "value"))
	if err != nil {
		log.Fatal(err.Error())
	}

	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		_, err = stmt.Exec(counter.Timestamp, counter.Tags.String(), counter.Value)
		if err != nil {
			log.Fatal(err.Error())
		}
	})

	executeStatement(stmt)

	stmt, err = txn.Prepare(mssql.CopyIn("gauges", mssql.BulkOptions{}, "timestamp", "name", "value"))
	if err != nil {
		log.Fatal(err.Error())
	}

	metrics.Gauges.Each(func(key, tagsKey string, gauge gostatsd.Gauge) {
		_, err = stmt.Exec(gauge.Timestamp, gauge.Tags.String(), gauge.Value)
		if err != nil {
			log.Fatal(err.Error())
		}
	})

	executeStatement(stmt)

	stmt, err = txn.Prepare(mssql.CopyIn("sets", mssql.BulkOptions{}, "timestamp", "name", "value"))
	if err != nil {
		log.Fatal(err.Error())
	}

	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		for k := range set.Values {
			_, err = stmt.Exec(set.Timestamp, set.Tags.String(), k)
			if err != nil {
				log.Fatal(err.Error())
			}
		}
	})

	executeStatement(stmt)

	// TODO: decide how to store timer metrics
	// metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
	// 	_, err = stmt.Exec(timer.Timestamp, timer.Tags.String(), timer.Values)
	// 	if err != nil {
	// 		log.Fatal(err.Error())
	// 	}
	// })
	// buf := client.preparePayload(metrics, time.Now())
	// sink := make(chan *bytes.Buffer, 1)
	// sink <- buf
	// close(sink)
	// select {
	// case <-ctx.Done():
	// 	client.sender.PutBuffer(buf)
	// 	cb([]error{ctx.Err()})
	// case client.sender.Sink <- sender.Stream{Ctx: ctx, Cb: cb, Buf: sink}:
	// }

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

func setupDatabase(conn *sql.DB) {
	for table, script := range creationScriptsByTable {
		rows, err := conn.Query(fmt.Sprintf("SELECT * FROM statsd.dbo.%s", table))
		if err != nil { // table does not exist
			_, err = conn.Exec(script)
			if err != nil {
				log.Fatal(fmt.Sprintf("Failed to create table %s: ", table), err.Error())
			}
		} else {
			rows.Close()
		}
	}
}

func (client *Client) preparePayload(metrics *gostatsd.MetricMap, ts time.Time) *bytes.Buffer {
	// buf := client.sender.GetBuffer()
	// now := ts.Unix()
	// if client.legacyNamespace {
	// 	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
	// 		_, _ = fmt.Fprintf(buf, "%s %d %d\n", client.prepareName("stats_counts", key, "", counter.Source, counter.Tags), counter.Value, now)
	// 		_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.counterNamespace, key, "", counter.Source, counter.Tags), counter.PerSecond, now)
	// 	})
	// } else {
	// 	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
	// 		_, _ = fmt.Fprintf(buf, "%s %d %d\n", client.prepareName(client.counterNamespace, key, "count", counter.Source, counter.Tags), counter.Value, now)
	// 		_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.counterNamespace, key, "rate", counter.Source, counter.Tags), counter.PerSecond, now)
	// 	})
	// }
	// metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
	// 	if timer.Histogram != nil {
	// 		for histogramThreshold, count := range timer.Histogram {
	// 			bucketTag := "le:+Inf"
	// 			if !math.IsInf(float64(histogramThreshold), 1) {
	// 				bucketTag = "le:" + strconv.FormatFloat(float64(histogramThreshold), 'f', -1, 64)
	// 			}
	// 			newTags := timer.Tags.Concat(gostatsd.Tags{bucketTag})
	// 			_, _ = fmt.Fprintf(buf, "%s %d %d\n", client.prepareName(client.counterNamespace, key, "histogram", timer.Source, newTags), count, now)
	// 		}
	// 	} else {
	// 		if !client.disabledSubtypes.Lower {
	// 			_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "lower", timer.Source, timer.Tags), timer.Min, now)
	// 		}
	// 		if !client.disabledSubtypes.Upper {
	// 			_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "upper", timer.Source, timer.Tags), timer.Max, now)
	// 		}
	// 		if !client.disabledSubtypes.Count {
	// 			_, _ = fmt.Fprintf(buf, "%s %d %d\n", client.prepareName(client.timerNamespace, key, "count", timer.Source, timer.Tags), timer.Count, now)
	// 		}
	// 		if !client.disabledSubtypes.CountPerSecond {
	// 			_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "count_ps", timer.Source, timer.Tags), timer.PerSecond, now)
	// 		}
	// 		if !client.disabledSubtypes.Mean {
	// 			_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "mean", timer.Source, timer.Tags), timer.Mean, now)
	// 		}
	// 		if !client.disabledSubtypes.Median {
	// 			_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "median", timer.Source, timer.Tags), timer.Median, now)
	// 		}
	// 		if !client.disabledSubtypes.StdDev {
	// 			_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "std", timer.Source, timer.Tags), timer.StdDev, now)
	// 		}
	// 		if !client.disabledSubtypes.Sum {
	// 			_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "sum", timer.Source, timer.Tags), timer.Sum, now)
	// 		}
	// 		if !client.disabledSubtypes.SumSquares {
	// 			_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, "sum_squares", timer.Source, timer.Tags), timer.SumSquares, now)
	// 		}
	// 		for _, pct := range timer.Percentiles {
	// 			_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.timerNamespace, key, pct.Str, timer.Source, timer.Tags), pct.Float, now)
	// 		}
	// 	}
	// })
	// metrics.Gauges.Each(func(key, tagsKey string, gauge gostatsd.Gauge) {
	// 	_, _ = fmt.Fprintf(buf, "%s %f %d\n", client.prepareName(client.gaugesNamespace, key, "", gauge.Source, gauge.Tags), gauge.Value, now)
	// })
	// metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
	// 	_, _ = fmt.Fprintf(buf, "%s %d %d\n", client.prepareName(client.setsNamespace, key, "", set.Source, set.Tags), len(set.Values), now)
	// })
	// return buf
	return nil
}

// SendEvent discards events.
func (client *Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	fmt.Printf("calling send event\n")
	fmt.Print(e)
	return nil
}

// Name returns the name of the backend.
func (client *Client) Name() string {
	return BackendName
}

// NewClientFromViper constructs a Client object using configuration provided by Viper
func NewClientFromViper(v *viper.Viper, logger logrus.FieldLogger, pool *transport.TransportPool) (gostatsd.Backend, error) {
	g := util.GetSubViper(v, "sqlserver")

	g.SetDefault("address", DefaultAddress)
	g.SetDefault("port", DefaultPort)
	g.SetDefault("user", DefaultUser)
	g.SetDefault("password", DefaultPassword)
	g.SetDefault("database", DefaultDatabase)
	g.SetDefault("dial_timeout", DefaultDialTimeout)
	g.SetDefault("write_timeout", DefaultWriteTimeout)

	return NewClient(
		g.GetString("address"),
		g.GetInt("port"),
		g.GetString("user"),
		g.GetString("password"),
		g.GetString("database"),
		g.GetDuration("dial_timeout"),
		g.GetDuration("write_timeout"),
		gostatsd.DisabledSubMetrics(v),
		logger,
	)
}

// NewClient constructs a Graphite backend object.
func NewClient(
	address string,
	port int,
	user string,
	password string,
	database string,
	dialTimeout time.Duration,
	writeTimeout time.Duration,
	disabled gostatsd.TimerSubtypes,
	logger logrus.FieldLogger,
) (*Client, error) {
	if address == "" {
		return nil, fmt.Errorf("[%s] address is required", BackendName)
	}
	if user == "" {
		return nil, fmt.Errorf("[%s] user is required", BackendName)
	}
	if password == "" {
		return nil, fmt.Errorf("[%s] password is required", BackendName)
	}
	if database == "" {
		return nil, fmt.Errorf("[%s] database is required", BackendName)
	}
	if dialTimeout <= 0 {
		return nil, fmt.Errorf("[%s] dialTimeout should be positive", BackendName)
	}
	if writeTimeout < 0 {
		return nil, fmt.Errorf("[%s] writeTimeout should be non-negative", BackendName)
	}

	logger.WithFields(logrus.Fields{
		"address":       address,
		"dial-timeout":  dialTimeout,
		"write-timeout": writeTimeout,
	}).Info("created backend")

	fmt.Printf(" server: %s\n", address)
	fmt.Printf(" user: %s\n", user)
	fmt.Printf(" password: %s\n", password)
	fmt.Printf(" port: %d\n", port)
	fmt.Printf(" database: %s\n", database)

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s", address, user, password, port, database)
	fmt.Printf("connString:%s\n", connString)
	conn, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}

	txn, err := conn.Begin() // IS THIS NEEDED?

	setupDatabase(conn)
	fmt.Printf("txn: %v\n", txn)
	if err != nil {
		fmt.Printf("wtf")
		log.Fatal(err)
	}

	return &Client{
		dbHandle: *conn,
		dbName:   database,
	}, nil
}

// func combine(prefix, suffix string) string {
// 	prefix = strings.Trim(prefix, ".")
// 	suffix = strings.Trim(suffix, ".")
// 	if prefix != "" && suffix != "" {
// 		return prefix + "." + suffix
// 	}
// 	if prefix != "" {
// 		return prefix
// 	}
// 	return suffix
// }
