# Final: Ejercicio 1 

### Problema
La Administración Nacional de Aviación Civil necesita una serie de informes para elevar al ministerio de transporte acerca de los aterrizajes y despegues en todo el territorio Argentino, como puede ser: cuales aviones son los que más volaron, cuántos pasajeros volaron, ciudades de partidas y aterrizajes entre fechas determinadas, etc.
Usted como data engineer deberá realizar un pipeline con esta información, automatizarlo y realizar los análisis de datos solicitados que permita responder las preguntas de negocio, y hacer sus recomendaciones con respecto al estado actual

Listado de vuelos realizados: 
- https://datos.gob.ar/lv/dataset/transporte-aterrizajes-despegues-procesados-por-administracion-nacional-aviacion-civil-anac

Listado de detalles de aeropuertos de Argentina:
- https://datos.transporte.gob.ar/dataset/lista-aeropuertos

### Solución

1) Hacer ingest de los siguientes files relacionados con transporte aéreo de Argentina.

```bash
# ingest.sh
## download flights and airports datasets
wget -nc -O /home/hadoop/landing/vuelos_2021.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv
wget -nc -O /home/hadoop/landing/vuelos_2022.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv
wget -nc -O /home/hadoop/landing/aeropuertos.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv
```

2) Crear 2 tablas en el datawarehouse, una para los vuelos realizados en 2021 y 2022 (2021-informe-ministerio.csv y 202206-informe-ministerio) y otra tabla para el detalle de los aeropuertos (aeropuertos_detalle.csv)

```sql
CREATE DATABASE vuelos;

CREATE EXTERNAL TABLE IF NOT EXISTS vuelos.aeropuerto_tabla( 
  fecha date,
  horaUTC string,
  clase_de_vuelo string,
  clasificacion_de_vuelo string,
  tipo_de_movimiento string,
  aeropuerto string,
  origen_destino string,
  aerolinea_nombre string,
  aeronave string,
  pasajeros int
)
LOCATION 'hdfs://etl:9000/user/hive/warehouse/tables/vuelos';

CREATE EXTERNAL TABLE IF NOT EXISTS vuelos.aeropuerto_detalles_tabla( 
  aeropuerto string,
  oac string,
  iata string,
  tipo string,
  denominacion string,
  coordenadas string,
  latitud string,
  longitud string,
  elev float,
  uom_elev string,
  ref string,
  distancia_ref float,
  direccion_ref string,
  condicion string,
  control string,
  region string,
  uso string,
  trafico string,
  sna string,
  concesionado string,
  provincia string
)
LOCATION 'hdfs://etl:9000/user/hive/warehouse/tables/vuelos';
```

3) Realizar un proceso automático orquestado por airflow que ingeste los archivos previamente mencionados entre las fechas 01/01/2021 y 30/06/2022 en las dos tablas creadas.

Los archivos 202206-informe-ministerio.csv y 202206-informe-ministerio.csv → en la 
tabla aeropuerto_tabla 

El archivo aeropuertos_detalle.csv → en la tabla aeropuerto_detalles_tabla

4) 

```python

```

```bash
/home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transformation.py
```