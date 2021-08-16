package sqlserver

// PSEUDO CODE
// newclientfromviper
// -> calls newsqlserverclient
// -> check configs, establish connection
// -> check existence of tables, maybe validate with some same queries (SET PARSEONLY ON)
// ->
import (
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
	// DefaultAddress is the default address of SQL Server instance.
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
			id BIGINT NOT NULL IDENTITY,
			"timestamp" BIGINT NOT NULL,
			"name" VARCHAR(255) NOT NULL,
			value BIGINT NOT NULL,
			CONSTRAINT counters_statistics_pkey PRIMARY KEY (id)
		);
		CREATE INDEX counters_statistics_timestamp_IDX ON counters_statistics ([timestamp], name)`,
	"gauges_statistics": `
		DROP TABLE IF EXISTS gauges_statistics;
		CREATE TABLE gauges_statistics (
			id BIGINT NOT NULL IDENTITY,
			"timestamp" BIGINT NOT NULL,
			"name" VARCHAR(255) NOT NULL,
			value DECIMAL NOT NULL,
			CONSTRAINT gauges_statistics_pkey PRIMARY KEY (id)
		);
		CREATE INDEX gauges_statistics_timestamp_IDX ON gauges_statistics ([timestamp], name)`,
	"sets_statistics": `
		DROP TABLE IF EXISTS sets_statistics;
		CREATE TABLE sets_statistics (
			id BIGINT NOT NULL IDENTITY,
			"timestamp" BIGINT NOT NULL,
			"name" VARCHAR(255) NOT NULL,
			value BIGINT NOT NULL,
			CONSTRAINT sets_statistics_pkey PRIMARY KEY (id)
		);
		CREATE INDEX sets_statistics_timestamp_IDX ON sets_statistics ([timestamp], name)`,
	"timers_statistics": `
		DROP TABLE IF EXISTS timers_statistics;
		CREATE TABLE timers_statistics (
			id BIGINT NOT NULL IDENTITY,
			"timestamp" DECIMAL NOT NULL,
			"name" VARCHAR(255) NOT NULL,
			value BIGINT NOT NULL,
			CONSTRAINT timers_statistics_pkey PRIMARY KEY (id)
		);
		CREATE INDEX timers_statistics_timestamp_IDX ON timers_statistics ([timestamp], name)`,
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

func executeStatement(stmt *sql.Stmt, errs *[]error) {
	_, err := stmt.Exec()
	if err != nil {
		*errs = append(*errs, err)
	}

	err2 := stmt.Close()
	if err2 != nil {
		*errs = append(*errs, err2)
	}
}

// SendMetricsAsync flushes the metrics to the Graphite server, preparing payload synchronously but doing the send asynchronously.
func (client *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	fmt.Printf("calling send metrics async\n")
	fmt.Print(metrics)

	tryInsertMetrics := func() []error {
		errs := make([]error, 0)
		txn, err := client.dbHandle.Begin()
		if err != nil {
			errs = append(errs, err)
			return errs
		}

		stmt, err := txn.Prepare(mssql.CopyIn("counters_statistics", mssql.BulkOptions{}, "timestamp", "name", "value"))
		if err != nil {
			errs = append(errs, err)
		} else {
			metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
				_, err = stmt.Exec(counter.Timestamp, key, counter.Value)
				if err != nil {
					errs = append(errs, err)
				}
			})

			executeStatement(stmt, &errs)
		}

		// stmt, err = txn.Prepare(mssql.CopyIn("gauges_statistics", mssql.BulkOptions{}, "timestamp", "name", "value"))
		// if err != nil {
		// 	errs = append(errs, err)
		// } else {
		// 	metrics.Gauges.Each(func(key, tagsKey string, gauge gostatsd.Gauge) {
		// 		_, err = stmt.Exec(gauge.Timestamp, gauge.Tags.String(), gauge.Value)
		// 		if err != nil {
		// 			errs = append(errs, err)
		// 		}
		// 	})

		// 	executeStatement(stmt, &errs)
		// }

		stmt, err = txn.Prepare(mssql.CopyIn("sets_statistics", mssql.BulkOptions{}, "timestamp", "name", "value"))
		if err != nil {
			errs = append(errs, err)
		} else {
			metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
				for k := range set.Values {
					_, err = stmt.Exec(set.Timestamp, set.Tags.String(), k)
					if err != nil {
						errs = append(errs, err)
					}
				}
			})

			executeStatement(stmt, &errs)
		}

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
		return errs
	}

	errs := tryInsertMetrics()

	cb(errs)
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

// NewClient constructs a SQL Server backend object.
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
