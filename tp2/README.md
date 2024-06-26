### Queries

1) Obtener una lista de todas las categorías distintas
 ```sql
 select distinct category_name  from categories; 
 ```

2) Obtener una lista de todas las regiones distintas para los clientes
```sql
 select distinct region from customers;
```

3) Obtener una lista de todos los títulos de contacto distintos
```sql
 select distinct contact_title from customers;
```

4) Obtener una lista de todos los clientes, ordenados por país
```sql
 select * from customers order by country;
```

5) Obtener una lista de todos los pedidos, ordenados por id del empleado y fecha del pedido
```sql
 select * from orders order by employee_id, order_date; 
```

6) Insertar un nuevo cliente en la tabla Customers:
```sql
 insert into public.customers
    (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax)
 values
    ('TSLA', 'Tesla', 'Elon Musk', 'CEO', '', 'California', '', '12209', 'United States', '08008881111', '2136-123');
```

7) Insertar una nueva región en la tabla region
```sql
insert into public.region (region_id, region_description) values (5, 'Center'); 
```

8) Obtener todos los clientes de la tabla Customers donde el campo Región es NULL
```sql
select * from customers where region is null;
```

9) Obtener Product_Name y Unit_Price de la tabla Products, y si Unit_Price es NULL, use el precio estándar de $10 en su lugar
```sql
select product_name, coalesce(unit_price , 10) from products;
```

10) Obtener el nombre de la empresa, el nombre del contacto y la fecha del pedido de todos los pedidos
```sql
select c.company_name, c.contact_name, o.order_date
	from customers as c inner join orders as o 
		on c.customer_id = o.customer_id;
```

11) Obtener la identificación del pedido, el nombre del producto y el descuento de todos los detalles del pedido y productos
```sql
select p.product_name, od.order_id , od.discount
	from products as p inner join order_details as od 
		on p.product_id = od.product_id ;
```

12) Obtener el identificador del cliente, el nombre de la compañía, el identificador y la fecha de la orden de todas las órdenes y aquellos clientes que hagan match
```sql
select c.customer_id, c.company_name, o.order_id, o.order_date
	from customers as c left join orders as o
		on c.customer_id = o.customer_id;
```

13) Obtener el identificador del empleados, apellido, identificador de territorio y descripción del territorio de todos los empleados y aquellos que hagan match en territorios
```sql
select e.employee_id, e.last_name, t.territory_id, t.territory_description
	from employees as e 
		inner join employee_territories as et
			on e.employee_id = et.employee_id 
		inner join territories as t
			on t.territory_id  = et.territory_id;
```

14) Obtener el identificador de la orden y el nombre de la empresa de todos las órdenes y aquellos clientes que hagan match
```sql
select o.order_id, c.company_name
	from orders as o left join customers as c
		on o.customer_id = c.customer_id ;
```

15) Obtener el identificador de la orden, y el nombre de la compañía de todas las órdenes y aquellos clientes que hagan match
```sql
select o.order_id, c.company_name
	from orders as o right join customers as c
		on o.customer_id = c.customer_id ;
```

16) Obtener el nombre de la compañía, y la fecha de la orden de todas las órdenes y aquellos transportistas que hagan match. Solamente para aquellas ordenes del año 1996
```sql
select s.company_name , o.order_date
	from shippers as s right join orders as o
		on s.shipper_id = o.ship_via 
	WHERE extract ('Year' from o.order_date) = 1996;
```

17) Obtener nombre y apellido del empleados y el identificador de territorio, de todos los empleados y aquellos que hagan match o no de employee_territories
```sql
select e.first_name, e.last_name, t.territory_id
	from employees as e full outer join employee_territories as et
		on e.employee_id = et.employee_id 
	full outer join territories as t
		on et.territory_id = t.territory_id;
```

18) Obtener el identificador de la orden, precio unitario, cantidad y total de todas las órdenes y aquellas órdenes detalles que hagan match o no
```sql
select o.order_id, od.unit_price, od.quantity, (od.unit_price * od.quantity) as total
	from orders as o full outer join order_details as od
		on o.order_id = od.order_id ;
```

19) Obtener la lista de todos los nombres de los clientes y los nombres de los proveedores
```sql
select company_name from suppliers
	union
select company_name from customers;
```

20) Obtener la lista de los nombres de todos los empleados y los nombres de los gerentes de departamento
```sql
select e.first_name as name from employees e
	union
select e2.first_name from employees e2 
	inner join employees e3 on e2.reports_to = e3.employee_id
order by name;
```

21) Obtener los productos del stock que han sido vendidos
```sqlS
select product_name, product_id 
	from products p 
		where product_id in (select distinct  product_id  from order_details od)
```

22)
```sql
select company_name from customers c 
	where c.customer_id in
		(select distinct customer_id  from orders o where ship_country = 'Argentina');
```

23) Obtener el nombre de los productos que nunca han sido pedidos por clientes de
Francia

```sql
select distinct product_name  from products p where product_id in
(select product_id  from order_details od where order_id in
(select distinct order_id from orders where 
	customer_id in (select distinct customer_id  from customers c  where country != 'France')));
```

24) Obtener la cantidad de productos vendidos por identificador de orden
```sql
select order_id, sum(quantity)as sum from order_details od  group by order_id;
```

25) Obtener el promedio de productos en stock por producto
```sql
select product_name , avg(units_in_stock) from products p group by product_name 
```

26) Cantidad de productos en stock por producto, donde haya más de 100 productos en stock
```sql
select product_name , sum(units_in_stock) from products p group by product_name 
	having sum(units_in_stock) > 100
```

27) Obtener el promedio de pedidos por cada compañía y solo mostrar aquellas con un promedio de pedidos superior a 10
```sql
select company_name, avg(o.order_id) from customers as c 
	inner join orders o 
		on c.customer_id = o.customer_id group by c.company_name 
```

28) Obtener el nombre del producto y su categoría, pero muestre "Discontinued" en lugar del nombre de la categoría si el producto ha sido descontinuado
```sql
select  p.product_name,
	case 
		when p.discontinued = 0 then c.category_name else 'Discontinued' 
	end as NewCategory
from products p inner join categories c on
	p.category_id = c.category_id;
```

29) Obtener el nombre del empleado y su título, pero muestre "Gerente de Ventas" en lugar del título si el empleado es un gerente de ventas (Sales Manager)
```sql
select first_name , last_name ,
	case 
		when title != 'Sales Manager' then title else 'Gerente de Ventas' 
	end as NewTitle
from employees e 
```