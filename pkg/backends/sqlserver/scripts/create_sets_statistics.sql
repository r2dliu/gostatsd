DROP TABLE IF EXISTS sets_statistics;

CREATE TABLE sets_statistics (
	"timestamp" BIGINT NOT NULL,
	"name" varchar(255) NOT NULL,
	value BIGINT NOT NULL,
	CONSTRAINT sets_statistics_pkey PRIMARY KEY ("timestamp", name)
);
