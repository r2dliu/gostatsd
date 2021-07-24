DROP TABLE IF EXISTS gauges_statistics;

CREATE TABLE gauges_statistics (
	"timestamp" BIGINT NOT NULL,
	"name" varchar(255) NOT NULL,
	value BIGINT NOT NULL,
	CONSTRAINT gauges_statistics_pkey PRIMARY KEY ("timestamp", name)
);
