# ETL for obtaining products from MeLi API
This is an example of an ETL script that communicates with a public API from Mercado Libre (https://developers.mercadolibre.com.ar/es_ar/api-docs-es), in order to
obtain different attributes from a certain product sold by different Mercado Libre's costumers (sellers). For example, we 
can obtain the price, sold quantity, warranty, etc. of the product `Iphone 11` that is offered by sellers that publish their 
products on Mercado Libre. The relevant information about items and sellers are stored then in a local MySQL database. 
The workflow also does a time profiling for measuring: request times, database insert times and processes times (extraction, transformation and loading)

_________________
### Deploy local MySQL database
You can use the `docker-compose.yml` and `.env` file, where is defined the `MYSQL_ROOT_PASSWORD` environment variable. 
Execute: `docker-compose up` (wait a moment until the database has initialized)

________________
### Script execution
You can execute the process in two different ways

#### 1. Local virtual environment
* In your Python environment, install the dependencies by executing `pip install -r requirements.txt`.
* Export the environ variable `DATABASE_URL`: `export DATABASE_URL=mysql+pymysql://root:<MYSQL_ROOT_PASSWORD>@localhost:3306/db`
* In your Python environment, execute `python best_seller.py ` (include -h to see the optional arguments)
* Otional arguments:

`--query`: request products filtered by name. For example: 'Iphone 11'

`--max_items`:   maximum amount of products to be requested.

`--exclude_seller_id`:   This is the client seller id. Products published by the client will be omitted.

`--new_db`:     If true, all current data stored in the local database will be deleted.

#### 2. Docker container
* Open a terminal and execute `docker build --tag=ml-sellers .`; this will generate a docker
image with tag `ml-sellers:latest`
* Execute: 
```
docker run --name=ml-sellers-etl --net=host \
           -it --rm \
           -e DATABASE_URL=mysql+pymysql://root:<MYSQL_ROOT_PASSWORD>@localhost:3306/db \
           ml-sellers:latest python /app/best_seller.py <args>
```
________________
### Database
Every data related to products, sellers and processes metrics will be stored in the local MySQL. By default,
 the database name will be `bd`. In it, the following tables will be found:

Relevant information about the selected item product
* **items**: Relevant information about the selected item product (seller_id, title, sold_quantity, price, warranty).
* **item_shipping**: Shipping methods per item.
* **sellers**: Information about product sellers (seller_id, completed_sales).
* **request_metrics**: Request times of each Mercado Libre's API services.  
* **database_metrics**: Database insertion times.
* **process_metrics**: Total elapsed times for each process (data extraction, transformation and loading in database) 

