# Data Dictionary for The Project Data Model
<br>

## df_fact_immi
<br>

| df_fact_immi       | Type      | Description                                            |
|--------------------|-----------|--------------------------------------------------------|
| immigration_id     | INT       | Primary Key                                            |
| cicid              | BIGINT    | CIC ID                                                 |
| year               | INT       | 4 digit year                                           |
| month              | INT       | Numeric month                                          |
| country_code_cit   | INT       | Three numbers correspondant for country of citizenship |
| country_code_res   | INT       | Three numbers correspondant for country of origin      |
| city_code          | CHAR(3)   | Three characters abbreviation for USA city             |
| state_code         | CHAR(2)   | Two characters abbreviation for USA state              |
| arrival_date       | TIMESTAMP | Arrive date                                            |
| departure_date     | TIMESTAMP | Leaving date                                           |
| transportation     | INT       | Traffic method                                         |
| visa               | INT       | Visa category                                          |
| airline            | INT       | Airline used to arrive in the US                       |
<br>
## df_dim_ident
<br>

| df_dim_ident      | Type    | Description                     |
|-------------------|---------|---------------------------------|
| ident_id          | INT     | Primary Key                     |
| cicid             | BIGINT  | CIC ID                          |
| citizen_country   | INT     | Country of citizenship (i94cit) |
| residence_country | INT     | Country of residence (i94res)   |
| birth_year        | INT     | Birth year                      |
| gender            | CHAR(1) | Gender                          |
<br>

## df_dim_flight
<br>

| df_dim_flight | Type    | Description                                     |
|---------------|---------|-------------------------------------------------|
| flight_id     | INT     | Primary Key                                     |
| cicid         | BIGINT  | CIC ID                                          |
| airline       | VARCHAR | Airline used to arrive in U.S.                  |
| admin_num     | BIGINT  | Admission Number                                |
| flight_number | VARCHAR | Flight number of Airline used to arrive in U.S. |
| visa_type     | CHAR(2) | Class of legal immigration admission            |
<br>

## df_dim_city_pop
<br>

| df_dim_city_pop   | Type    | Description                                |
|-------------------|---------|--------------------------------------------|
| city_pop_id       | INT     | Primary Key                                |
| city              | CHAR(3) | Three characters abbreviation for USA city |
| state             | CHAR(2) | Two characters abbreviation for USA state  |
| male_population   | INT     | City male population                       |
| famale_population | INT     | City female population                     |
| num_veterans      | INT     | Number of veterans                         |
| foreign_born      | INT     | Number of foreign born baby                |
| race              | VARCHAR | Race of majority                           |
<br>

## df_dim_city_stats
<br>

| df_dim_city_stats  | Type    | Description                                |
|--------------------|---------|--------------------------------------------|
| city_stats_id      | INT     | Primary Key                                |
| city               | CHAR(3) | Three characters abbreviation for USA city |
| state              | CHAR(2) | Two characters abbreviation for USA state  |
| median_age         | INT     | City median age                            |
| avg_household_size | FLOAT   | City average household size                |
<br>

## df_country_code
<br>

| df_country_code | Type    | Description  |
|-----------------|---------|--------------|
| code            | INT     | Country ID   |
| country         | VARCHAR | Country name |
<br>

## df_city_code
<br>

| df_city_code | Type    | Description                                |
|--------------|---------|--------------------------------------------|
| code         | CHAR(2) | Three characters abbreviation for USA city |
| city         | VARCHAR | Full name of USA city                      |
<br>

## df_state_code
<br>

| df_state_code | Type    | Description                               |
|---------------|---------|-------------------------------------------|
| code          | CHAR(3) | Two characters abbreviation for USA state |
| state         | VARCHAR | Full name of USA state                    |
<br>

## df_visa_code
<br>

| df_visa_code | Type    | Description  |
|--------------|---------|--------------|
| code         | INT     | Visa type ID |
| type         | VARCHAR | Visa type    |
<br>

## df_transportation
<br>

| df_transportation | Type    | Description           |
|-------------------|---------|-----------------------|
| code              | INT     | Transportaion mode ID |
| mode              | VARCHAR | Transportation mode   |
