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

7) 
```sql
 select distinct category_name  from categories; 
```

8) 
```sql
 select distinct category_name  from categories; 
```