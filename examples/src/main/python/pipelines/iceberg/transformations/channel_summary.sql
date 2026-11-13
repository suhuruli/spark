-- Materialized view defined in SQL with Iceberg partitioning and table properties.
CREATE MATERIALIZED VIEW channel_summary
PARTITIONED BY (channel)
TBLPROPERTIES ('team' = 'analytics')
AS SELECT channel, COUNT(*) AS orders, SUM(amount) AS revenue
FROM large_orders
GROUP BY channel;
