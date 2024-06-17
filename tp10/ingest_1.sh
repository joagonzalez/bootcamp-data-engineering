## running ingest pipeline with scoop

/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://postgres/northwind \
--username postgres \
--m 1 \
--password-file /sqoop/password_file.txt \
--target-dir /sqoop/ingest/clientes \
--as-parquetfile \
--query "select c.customer_id, c.company_name, orders_joined.quantity 
            from customers as c inner join
                (select od.quantity, o.customer_id from order_details as od inner join orders as o on od.order_id = o.order_id) 
            as orders_joined on c.customer_id = orders_joined.customer_id where \$CONDITIONS
            order by orders_joined.quantity desc;" \
--delete-target-dir
