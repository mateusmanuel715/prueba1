#### Reporte Check-in y Check-out de conductores
### Librerias
rm(list = ls());gc();
library(openxlsx);library(jsonlite);library(tidyverse);library(lubridate);library(readxl);library(sqldf);library(writexl);library(googlesheets4);library(googledrive);library(processx);library(RPostgres);options(sqldf.driver = "SQLite");Sys.setlocale("LC_TIME", "es_ES.UTF-8")
### Extraccion
## PostgreSQL
OPERATIONAL_PROD = dbConnect(Postgres(), user = 'voltopuser',password = 'PaFqtjD4asat5ObF',dbname = 'postgres',host = 'psql-voltop.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
driver_check_ins_outs <- data.frame(dbGetQuery(OPERATIONAL_PROD, "SELECT * FROM driver_check_ins_outs"))
dbDisconnect(OPERATIONAL_PROD)
REPORTE_PROD = dbConnect(Postgres(),user = 'voltopdatauser',password = 'OtC6Vd2BtUOFc7gj',dbname = 'postgres',host = 'psql-voltopdata.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
payments_order <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM rawdata_uber_payments_order"))
trips <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM rawdata_uber_trips"))
asociados <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM report_drivers"))
dbDisconnect(REPORTE_PROD)
### Transformacion
colnames(trips) <- c("trip_id","driver_id","driver_first_name","driver_last_name","car_id","car_plate","type_service","created_datetime","finished_datetime","start_address","end_address","total_distance","status")
trips_1 <- subset(trips, subset = {status == "completed"})
trips_2 <- sqldf("select * from trips_1 group by trip_id")
asociados_1 <- sqldf("select * from asociados group by external_driver_id")
placa <- sqldf("select car_plate,city from asociados group by car_plate")
viajes_asociados <- subset(payments_order, subset = {description == "trip completed order"})
viajes_asociados[,"transaction_datetime"] <- as.character(viajes_asociados[,"transaction_datetime"])
viajes_asociados[,"dia"] <- as.Date(substr(viajes_asociados[,"transaction_datetime"], 1, 10), format = "%Y-%m-%d")
viajes_asociados[,"mes_numero"] <- month(viajes_asociados[,"dia"])
viajes_asociados[,"mes_texto"] <- format(viajes_asociados[,"dia"], "%B")
viajes_asociados[,"mes_texto"] <- str_to_title(viajes_asociados[,"mes_texto"])
viajes_asociados[,"mes"] <- paste0(viajes_asociados[,"mes_numero"],". ",viajes_asociados[,"mes_texto"])
viajes_asociados[,"ano"] <- year(viajes_asociados[,"dia"])
viajes_asociados[,"trimestre"] <- paste0("Q", quarter(viajes_asociados[,"dia"]), " ",viajes_asociados[,"ano"])
viajes_asociados_1 <- sqldf("select a.*,b.car_plate as placa from viajes_asociados as a left join trips_2 as b on a.trip_id = b.trip_id")
viajes_asociados_1[,"placa"][is.na(viajes_asociados_1[,"placa"])] <- 0
viajes_asociados_1 <- viajes_asociados_1 %>% rename(external_driver_id = driver_id)
viajes_asociados_2 <- sqldf("select a.*,b.driver_name as conductor,b.driver_id from viajes_asociados_1 as a left join asociados_1 as b on a.external_driver_id = b.external_driver_id")
viajes_asociados_2[,"driver_name"] <- paste0(viajes_asociados_2[,"driver_first_name"]," ",viajes_asociados_2[,"driver_last_name"])
viajes_asociados_2[,"driver_name"] <- toupper(viajes_asociados_2[,"driver_name"])
viajes_asociados_2[,"conductor"][is.na(viajes_asociados_2[,"conductor"])] <- 0
viajes_asociados_2[,"placa"][is.na(viajes_asociados_2[,"placa"])] <- 0
viajes_asociados_2[,"conductor"] <- ifelse(viajes_asociados_2[,"conductor"] == 0,viajes_asociados_2[,"driver_name"],viajes_asociados_2[,"conductor"])
viajes_asociados_3 <- sqldf("select a.*,b.city as ciudad from viajes_asociados_2 as a left join placa as b on a.placa = b.car_plate")
viajes_asociados_4 <- sqldf("select ciudad,driver_id,conductor,placa,ano,trimestre,mes,dia from viajes_asociados_3 group by ciudad,driver_id,conductor,placa,ano,trimestre,mes,dia")
viajes_asociados_4[,"llave"] <- paste0(viajes_asociados_4[,"driver_id"],viajes_asociados_4[,"dia"])
driver_check_ins_outs[,"dia"] <- as.Date(driver_check_ins_outs[,"created_at"])
driver_check_in <- subset(driver_check_ins_outs, subset = {is_check_in == 'TRUE'})
driver_check_in_1 <- sqldf("select driver_id,dia from driver_check_in group by driver_id,dia")
driver_check_in_1[,"llave"] <- paste0(driver_check_in_1[,"driver_id"],driver_check_in_1[,"dia"])
driver_check_in_2 <- sqldf("select * from driver_check_in_1 group by llave")
checks_conductores_1 <- sqldf("select a.*, b.llave as check_in from viajes_asociados_4 as a left join driver_check_in_1 as b on a.llave = b.llave")
checks_conductores_1[,"check_in"][is.na(checks_conductores_1[,"check_in"])] <- 0
checks_conductores_1[,"check_in"] <- ifelse(checks_conductores_1[,"check_in"] == 0,"No","Si")
driver_check_out <- subset(driver_check_ins_outs, subset = {is_check_in == 'FALSE'})
driver_check_out_1 <- sqldf("select driver_id,dia from driver_check_ins_outs group by driver_id,dia")
driver_check_out_1[,"llave"] <- paste0(driver_check_out_1[,"driver_id"],driver_check_out_1[,"dia"])
driver_check_out_2 <- sqldf("select * from driver_check_out_1 group by llave")
checks_conductores_2 <- sqldf("select a.*, b.llave as check_out from checks_conductores_1 as a left join driver_check_out_1 as b on a.llave = b.llave")
checks_conductores_2[,"check_out"][is.na(checks_conductores_2[,"check_out"])] <- 0
checks_conductores_2[,"check_out"] <- ifelse(checks_conductores_2[,"check_out"] == 0,"No","Si")
checks_conductores_3 <- subset(checks_conductores_2, subset = {dia != Sys.Date()})
checks_conductores_3[,"ciudad"][checks_conductores_3[,"ciudad"] == 0] <- "Sin ciudad"
checks_conductores_3[,"driver_id"][is.na(checks_conductores_3[,"driver_id"])] <- 0
checks_conductores_4 <- subset(checks_conductores_3, subset = {driver_id != 0})
checks_conductores_5 <- subset(checks_conductores_4, subset = {placa != 0})
checks_conductores_6 <- subset(checks_conductores_5, subset = {dia >= "2024-04-02"})
checks_conductores_7 <- subset(checks_conductores_6, subset = {ciudad != "Sin ciudad"})
checks_conductores <- checks_conductores_7
# RData
save(checks_conductores,file = "/Users/manuelmateus/Ambiente_Virtual/Voltop/REPORTES/OPERACIONES/reporte_checks_conductores.RData")
## PostgreSQL
REPORTE_PROD = dbConnect(Postgres(),user = 'voltopdatauser',password = 'OtC6Vd2BtUOFc7gj',dbname = 'postgres',host = 'psql-voltopdata.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
dbGetQuery(REPORTE_PROD,statement="truncate report_driver_checks");
dbWriteTable(REPORTE_PROD,name = "report_driver_checks",value = checks_conductores,overwrite = F,append = T,row.names = NA)
dbDisconnect(REPORTE_PROD)





