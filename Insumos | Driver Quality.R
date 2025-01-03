#### Insumo | Driver Quality
### Librerias
rm(list = ls());gc();library(data.table);library(openxlsx);library(jsonlite);library(tidyverse);library(lubridate);library(readxl);library(sqldf);library(writexl);library(googlesheets4);library(googledrive);library(processx);library(RPostgres);options(sqldf.driver = "SQLite")
### Extraccion
## CSV
ruta_json <- "/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/still-smithy-407213-cbfd93ea82d1.json"
gs4_auth(email = NULL,path = ruta_json,scopes = "https://www.googleapis.com/auth/spreadsheets",cache = gargle::gargle_oauth_cache(),use_oob = gargle::gargle_oob_default(),token = NULL)
calendario <- data.frame(read_sheet('https://docs.google.com/spreadsheets/d/1Ew9ntxmLREZfEhCrsX438xOTVVYyyEQKpqr4uc54Pg8/', sheet = "archivos_dri_quality"))
fechas <- calendario[,"fechas"]
nombre <- calendario[,"nombre"]
for (i in seq_along(fechas)) {archivo <- paste0("/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/Uber/driver_quality/", fechas[i], "-driver_quality-BRUNO_OCAMPO_GONZALEZ.csv");data <- data.frame(read_csv(archivo));data[,"fecha"] <- fechas[i];assign(nombre[i], data)}
### Transformacion
rm(archivo,data,fechas,i,nombre)
dataframe_names <- ls()[sapply(ls(), function(x) is.data.frame(get(x)) && x != "calendario")]
dataframes_list <- mget(dataframe_names)
driver_quality_1 <- bind_rows(dataframes_list)
driver_quality_2 <- distinct_all(driver_quality_1)
driver_quality_2[,"fecha"] <- as.Date(substr(driver_quality_2[,"fecha"], 1, 8), format = "%Y%m%d")
driver_quality <- data.frame(driver_quality_2)
colnames(driver_quality) <- c("driver_id","driver_firt_name","driver_last_name","viajes_completados","tasa_aceptacion","tasa_cancelacion","tasa_viajes_completados","calificacion_ultimas_4_semanas","calificacion_500_viajes_previos","tasa_aceptacion_app_socios","tasa_cancelacion_app_socios","fecha")
### Carga
# RData
save(driver_quality,file = "/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/Uber/driver_quality/driver_quality.RData")
## PostgreSQL
REPORTE_PROD = dbConnect(Postgres(),user = 'voltopdatauser',password = 'OtC6Vd2BtUOFc7gj',dbname = 'postgres',host = 'psql-voltopdata.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
dbGetQuery(REPORTE_PROD,statement="truncate rawdata_uber_driver_quality");
dbWriteTable(REPORTE_PROD,name = "rawdata_uber_driver_quality",value = driver_quality,overwrite = F,append = T,row.names = NA)
dbDisconnect(REPORTE_PROD)

