### Queries using Windows Functions

1) Obtener el promedio de precios por cada categoría de producto. La cláusula OVER(PARTITION BY CategoryID) específica que se debe calcular el promedio de precios por cada valor único de CategoryID en la tabla
 ```sql
select  c.category_name, 
		p.product_name, 
		p.unit_price,
		AVG(p.unit_price) 
			over (partition by c.category_id) as avg_price_by_category
from products as p inner join
	categories as c on c.category_id = p.category_id;
 ```

 2) Obtener el promedio de venta de cada cliente
 ```sql
 select  
		AVG(od.unit_price * od.quantity) over (partition by c.customer_id) as avg_order_amount,
		o.order_id,
		c.customer_id,
		od.unit_price,
		od.quantity,
		(od.unit_price * od.quantity) as amount
	from customers as c 
	inner join orders as o
		on c.customer_id = o.customer_id
	inner join order_details as od
		on o.order_id = od.order_id 
 ```

 3) Obtener el promedio de cantidad de productos vendidos por categoría (product_name, quantity_per_unit, unit_price, quantity, avgquantity) y ordenarlo por nombre de la categoría y nombre del producto
 ```sql
 select  		
		p.product_name,
		cat.category_name,
		p.quantity_per_unit,
		p.unit_price,
		od.quantity,
		AVG(od.quantity) over (partition by cat.category_id) as avg_order_amount
	from customers as c 
	inner join orders as o
		on c.customer_id = o.customer_id
	inner join order_details as od
		on o.order_id = od.order_id 
	inner join products as p
		on p.product_id = od.product_id 
	inner join categories as cat
		on cat.category_id = p.category_id 
	order by cat.category_name, p.product_name 
 ```

 4) Selecciona el ID del cliente, la fecha de la orden y la fecha más antigua de la orden para cada cliente de la tabla 'Orders'.
 ```sql
 ```
