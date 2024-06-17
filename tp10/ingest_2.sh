## running ingest pipeline with scoop

/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://postgres/northwind \
--username postgres \
--m 1 \
--password-file /sqoop/password_file.txt \
--target-dir /sqoop/ingest/envios \
--as-parquetfile \
--query "select order_id, shipped_date, c.company_name, c.phone from orders as o
	inner join customers as c on c.customer_id = o.customer_id where \$CONDITIONS;" \
--delete-target-dir

/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://postgres/northwind \
--username postgres \
--m 1 \
--password-file /sqoop/password_file.txt \
--target-dir /sqoop/ingest/order_details \
--as-parquetfile \
--query "select order_id, unit_price, quantity, discount from order_details as od where \$CONDITIONS;" \
--delete-target-dir