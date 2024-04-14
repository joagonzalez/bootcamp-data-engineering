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
 select 
	customer_id,
	order_date,
	min(order_date) over (partition by customer_id) as earliest_date
from orders as o
order by customer_id, earliest_date;
``` 

5) Seleccione el id de producto, el nombre de producto, el precio unitario, el id de
categoría y el precio unitario máximo para cada categoría de la tabla Products.
```sql
select 
	product_id,
	product_name,
	unit_price,
	category_id,
	max(unit_price) over (partition by category_id) as max_unit_price
from products as p 
```

6) Obtener el ranking de los productos más vendidos
```sql
select 
	ROW_NUMBER() OVER (ORDER BY sum desc) AS ranking,
	product_name,
	sum
from 
	(select 
		p.product_name,
		sum(od.quantity) as sum
	from order_details as od inner join products as p
		on od.product_id = p.product_id 
	group by p.product_name 
	order by sum desc) as qty
```

7) Asignar numeros de fila para cada cliente, ordenados por customer_id
```sql
select 
	ROW_NUMBER() OVER (ORDER BY customer_id) AS ranking,
	customer_id,
	company_name ,
	contact_name ,
	contact_title ,
	address ,
	city,
	region, country ,
	phone ,
	fax
from customers as c 
```

8) Obtener el ranking de los empleados más jóvenes () ranking, nombre y apellido del empleado, fecha de nacimiento)
```sql
select 
	ROW_NUMBER() OVER (ORDER BY birth_date desc) AS ranking,
	concat(first_name, ' ', last_name),
	birth_date 
from employees as e 
```

9) Obtener la suma de venta de cada cliente
```sql
select 
	sum(od.unit_price*od.quantity) over (partition by customer_id) as sum_order_amount,
	o.order_id ,
	o.customer_id ,
	o.employee_id ,
	o.order_date ,
	o.required_date,
	od.unit_price ,
	od.quantity 
from orders as o inner join order_details as od
	on o.order_id = od.order_id 
```

10) Obtener la suma total de ventas por categoría de producto
```sql
select 
	c.category_name ,
	p.product_name ,
	od.unit_price ,
	od.quantity ,
	sum(od.unit_price*od.quantity) over (partition by c.category_name) as total_sales
from order_details as od inner join products as p
	on od.product_id = p.product_id inner join categories as c
	on c.category_id = p.category_id 
order by c.category_name  , p.product_name 
```

11) Calcular la suma total de gastos de envío por país de destino, luego ordenarlo por país
y por orden de manera ascendente
```sql
select 
	o.ship_country ,
	o.order_id ,
	o.shipped_date ,
	o.freight ,
	sum(o.freight) over (partition by o.ship_country) as total_shipping_cost
from orders o 
order by o.ship_country, total_shipping_cost asc
```

12) Ranking de ventas por cliente
```sql
select 
	o.customer_id ,
	c.company_name,  
	sum(od.unit_price * od.quantity) as total_sales,
	RANK() OVER (ORDER BY sum(od.unit_price * od.quantity) desc) sales_rank
from orders as o inner join order_details as od
	on o.order_id = od.order_id inner join customers as c
	on o.customer_id = c.customer_id 
group by o.customer_id , c.company_name 
```

13) Ranking de empleados por fecha de contratacion
```sql
select 
	employee_id ,
	first_name ,
	last_name ,
	hire_date ,
	RANK() OVER (ORDER BY hire_date) hire_rank
from employees e 
```

14) Ranking de productos por precio unitario
```sql
select 
	product_id ,
	product_name ,
	unit_price ,
	RANK() OVER (ORDER BY unit_price desc) price_rank
from products p 
```

15) Mostrar por cada producto de una orden, la cantidad vendida y la cantidad
vendida del producto previo
```sql
select 
	order_id,
	product_id ,
	quantity,
    LAG(quantity , 1) OVER (
	  ORDER BY order_id  ASC) AS prev_qty
from order_details od 
```

16) Obtener un listado de ordenes mostrando el id de la orden, fecha de orden, id del cliente
y última fecha de orden
```sql
select 
	order_id ,
	order_date ,
	customer_id ,
	LAG(order_date, 1) over (order by customer_id, order_date) as prev_order_date
from orders as o 
```

17) Obtener un listado de productos que contengan: id de producto, nombre del producto,
precio unitario, precio del producto anterior, diferencia entre el precio del producto y
precio del producto anterior
```sql
select 
	product_id ,
	product_name ,
	unit_price ,
	LAG(unit_price , 1) over (order by product_id) as prev_price,
	(unit_price - LAG(unit_price , 1) over (order by product_id)) as difference_price
from products p 
```

18) Obtener un listado que muestra el precio de un producto junto con el precio del producto
siguiente
```sql
select 
	product_name ,
	unit_price ,
    LEAD(unit_price , 1) OVER(
    	ORDER BY product_name 
  	) AS next_sales
from products p 
```

19) Obtener un listado que muestra el total de ventas por categoría de producto junto con el
total de ventas de la categoría siguiente
```sql
select 
	c.category_name ,
	sum(od.unit_price*od.quantity) as suma,
	LEAD(sum(od.unit_price*od.quantity), 1) OVER(
    	ORDER BY c.category_name  
  	) AS next_suma
from order_details as od inner join products as p
	on p.product_id  = od.product_id inner join categories as c
	on c.category_id = p.category_id 
group by c.category_name 
```