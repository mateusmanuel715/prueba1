#### Insumo | Driver Activity
### Librerias
rm(list = ls());gc();library(data.table);library(openxlsx);library(jsonlite);library(tidyverse);library(lubridate);library(readxl);library(sqldf);library(writexl);library(googlesheets4);library(googledrive);library(processx);library(RPostgres);options(sqldf.driver = "SQLite")
### Extraccion
## CSV
ruta_json <- "/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/still-smithy-407213-cbfd93ea82d1.json"
gs4_auth(email = NULL,path = ruta_json,scopes = "https://www.googleapis.com/auth/spreadsheets",cache = gargle::gargle_oauth_cache(),use_oob = gargle::gargle_oob_default(),token = NULL)
calendario <- data.frame(read_sheet('https://docs.google.com/spreadsheets/d/1Ew9ntxmLREZfEhCrsX438xOTVVYyyEQKpqr4uc54Pg8/', sheet = "archivos_dri_activity"))
fechas <- calendario[,"fechas"]
nombre <- calendario[,"nombre"]
for (i in seq_along(fechas)) {archivo <- paste0("/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/Uber/driver_activity/", fechas[i], "-driver_activity-BRUNO_OCAMPO_GONZALEZ.csv");data <- data.frame(read_csv(archivo));data[,"fecha"] <- fechas[i];assign(nombre[i], data)}
### Transformacion
rm(archivo,data,fechas,i,nombre)
dataframe_names <- ls()[sapply(ls(), function(x) is.data.frame(get(x)) && x != "calendario")]
dataframes_list <- mget(dataframe_names)
driver_activity_1 <- bind_rows(dataframes_list)
driver_activity_2 <- distinct_all(driver_activity_1)
driver_activity_2[,"fecha"] <- as.Date(substr(driver_activity_2[,"fecha"], 1, 8), format = "%Y%m%d")
driver_activity <- data.frame(driver_activity_2)
### Carga
# RData
save(driver_activity,file = "/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/Uber/driver_activity/driver_activity.RData")
## PostgreSQL
REPORTE_PROD = dbConnect(Postgres(),user = 'voltopdatauser',password = 'OtC6Vd2BtUOFc7gj',dbname = 'postgres',host = 'psql-voltopdata.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
dbGetQuery(REPORTE_PROD,statement="truncate rawdata_uber_driver_activity");
dbWriteTable(REPORTE_PROD,name = "rawdata_uber_driver_activity",value = driver_activity,overwrite = F,append = T,row.names = NA)
dbDisconnect(REPORTE_PROD)

