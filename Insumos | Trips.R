#### Insumo | Trips
### Librerias
rm(list = ls());gc();library(data.table);library(openxlsx);library(jsonlite);library(tidyverse);library(lubridate);library(readxl);library(sqldf);library(writexl);library(googlesheets4);library(googledrive);library(processx);library(RPostgres);options(sqldf.driver = "SQLite")
### Extraccion
## CSV
ruta_json <- "/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/still-smithy-407213-cbfd93ea82d1.json"
gs4_auth(email = NULL,path = ruta_json,scopes = "https://www.googleapis.com/auth/spreadsheets",cache = gargle::gargle_oauth_cache(),use_oob = gargle::gargle_oob_default(),token = NULL)
calendario <- data.frame(read_sheet('https://docs.google.com/spreadsheets/d/1Ew9ntxmLREZfEhCrsX438xOTVVYyyEQKpqr4uc54Pg8/', sheet = "archivos_trips"))
fechas <- calendario[,"fechas"]
nombre <- calendario[,"nombre"]
for (i in seq_along(fechas)) {archivo <- paste0("/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/Uber/trip_activity/", fechas[i], "-trip_activity-BRUNO_OCAMPO_GONZALEZ.csv");data <- data.frame(read_csv(archivo));assign(nombre[i], data)}
### Transformacion 
rm(archivo,data,fechas,i,nombre)
dataframe_names <- ls()[sapply(ls(), function(x) is.data.frame(get(x)) && x != "calendario" && x != "archivo" && x != "data" && x != "fechas" && x != "i" && x != "nombre" && x != "ruta_json")]
dataframes_list <- mget(dataframe_names)
trips_1 <- bind_rows(dataframes_list)
trips_2 <- distinct_all(trips_1)
trips <- trips_2
### Carga
# RData
save(trips,file = "/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/Uber/trip_activity/trips.RData")
## PostgreSQL
REPORTE_PROD = dbConnect(Postgres(),user = 'voltopdatauser',password = 'OtC6Vd2BtUOFc7gj',dbname = 'postgres',host = 'psql-voltopdata.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
dbGetQuery(REPORTE_PROD,statement="truncate rawdata_uber_trips");
dbWriteTable(REPORTE_PROD,name = "rawdata_uber_trips",value = trips,overwrite = F,append = T,row.names = NA)
dbDisconnect(REPORTE_PROD)






