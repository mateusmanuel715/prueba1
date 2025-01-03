#### Insumo | Distancia recorrida
### Librerias
rm(list = ls());gc();library(data.table);library(openxlsx);library(jsonlite);library(tidyverse);library(lubridate);library(readxl);library(sqldf);library(writexl);library(googlesheets4);library(googledrive);library(processx);library(RPostgres);options(sqldf.driver = "SQLite")
### Extraccion
carpeta <- "/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/Hunter/Distancia recorrida/"
distancia_recorrida_1 <- list.files(path = carpeta, pattern = "\\.xlsx$", full.names = TRUE)
distancia_recorrida_2 <- data.frame(distancia_recorrida_1 %>% lapply(read_excel) %>% bind_rows())
### Transformacion
colnames(distancia_recorrida_2) <- c("Activo","placa","Fecha_inicial","Hora_inicial","Fecha_final","Hora_final","Distancia_recorrida")
distancia_recorrida_3 <- subset(distancia_recorrida_2, subset = {Fecha_inicial != "Total"})
distancia_recorrida_3[,"Distancia_recorrida_1"] <- as.numeric(gsub(" km","",distancia_recorrida_3[,"Distancia_recorrida"]))
distancia_recorrida_3[,"dia"] <- as.Date(distancia_recorrida_3[,"Fecha_inicial"], format = "%Y-%m-%d")
distancia_recorrida_4 <- distinct_all(distancia_recorrida_3)
distancia_recorrida <- distancia_recorrida_4
### Carga
# RData
save(distancia_recorrida,file = "/Users/manuelmateus/Ambiente_Virtual/Voltop/REPORTES/OPERACIONES/reporte_distancia_recorrida.RData")
## PostgreSQL
REPORTE_PROD = dbConnect(Postgres(), user = 'voltopdatauser',password = 'OtC6Vd2BtUOFc7gj',dbname = 'postgres',host = 'psql-voltopdata.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
dbGetQuery(REPORTE_PROD,statement="truncate rawdata_hunter_car_distance")
dbWriteTable(REPORTE_PROD,name = "rawdata_hunter_car_distance",value = distancia_recorrida,overwrite = F,append = T,row.names = NA)
dbDisconnect(REPORTE_PROD)









