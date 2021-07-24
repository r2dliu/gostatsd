DROP TABLE IF EXISTS counters_statistics;

CREATE TABLE counters_statistics (
	"timestamp" BIGINT NOT NULL,
	"name" varchar(255) NOT NULL,
	value BIGINT NOT NULL,
	CONSTRAINT counters_statistics_pkey PRIMARY KEY ("timestamp", name)
);
