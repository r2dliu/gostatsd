DROP TABLE IF EXISTS timers_statistics;

CREATE TABLE timers_statistics (
	id BIGINT NOT NULL IDENTITY,
	"timestamp" BIGINT NOT NULL,
	"name" varchar(255) NOT NULL,
	value BIGINT NOT NULL,
	CONSTRAINT timers_statistics_pkey PRIMARY KEY (id)
);
