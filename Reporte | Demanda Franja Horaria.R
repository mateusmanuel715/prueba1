#### Reporte Demana por Franja Horaria
### Librerias
rm(list = ls());gc();
library(openxlsx);library(jsonlite);library(tidyverse);library(lubridate);library(readxl);library(sqldf);library(writexl);library(googlesheets4);library(googledrive);library(processx);library(RPostgres);options(sqldf.driver = "SQLite")
### Extraccion
## PostgreSQL
REPORTE_PROD = dbConnect(Postgres(),user = 'voltopdatauser',password = 'OtC6Vd2BtUOFc7gj',dbname = 'postgres',host = 'psql-voltopdata.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
payments_order <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM rawdata_uber_payments_order"))
trips <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM rawdata_uber_trips"))
asociados <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM report_drivers"))
dbDisconnect(REPORTE_PROD)
# Google Sheet
ruta_json <- "/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/still-smithy-407213-cbfd93ea82d1.json"
gs4_auth(email = NULL,path = ruta_json,scopes = "https://www.googleapis.com/auth/spreadsheets",cache = gargle::gargle_oauth_cache(),use_oob = gargle::gargle_oob_default(),token = NULL)
calendario <- data.frame(read_sheet('https://docs.google.com/spreadsheets/d/1Ew9ntxmLREZfEhCrsX438xOTVVYyyEQKpqr4uc54Pg8/', sheet = "semanas"))
### Transformacion
colnames(trips) <- c("trip_id","driver_id","driver_first_name","driver_last_name","car_id","car_plate","type_service","created_datetime","finished_datetime","start_address","end_address","total_distance","status")
trips_1 <- subset(trips, subset = {status == "completed"})
trips_2 <- sqldf("select * from trips group by trip_id")
asociados_1 <- sqldf("select * from asociados group by external_driver_id")
franja_horaria_1 <- subset(payments_order, subset = {description == "trip completed order"})
franja_horaria_1[,"day"] <- as.Date(substr(franja_horaria_1[,"transaction_datetime"], 1, 10), format = "%Y-%m-%d")
franja_horaria_1[,"mes_numero"] <- month(franja_horaria_1[,"day"])
franja_horaria_1[,"mes_texto"] <- format(franja_horaria_1[,"day"], "%B")
franja_horaria_1[,"mes_texto"] <- str_to_title(franja_horaria_1[,"mes_texto"])
franja_horaria_1[,"mes"] <- paste0(franja_horaria_1[,"mes_numero"],". ",franja_horaria_1[,"mes_texto"])
franja_horaria_1[,"ano"] <- year(franja_horaria_1[,"day"])
franja_horaria_1[,"trimestre"] <- paste0("Q", quarter(franja_horaria_1[,"day"])," ",franja_horaria_1[,"ano"])
franja_horaria_2 <- sqldf("select a.*,b.car_plate from franja_horaria_1 as a left join trips_2 as b on a.trip_id = b.trip_id")
franja_horaria_2[,"hora"]  <- as.numeric(format(franja_horaria_2$transaction_datetime, "%H"))
franja_horaria_2[,"franja_horaria"] <- cut(franja_horaria_2[,"hora"],breaks = c(-1, 2, 5, 11, 15, 21, 24),labels = c("0h a 2h", "2h a 5h", "5h a 11h", "11h a 15h", "15 a 21h", "21h a 24h"),right = FALSE)
franja_horaria_3 <- sqldf("select ano,trimestre,mes,day as dia,driver_id,car_plate as placa,count(distinct trip_id) as valor from franja_horaria_2 group by ano,trimestre,mes,day,driver_id,car_plate")
franja_horaria_3[,"dia"] <- as.Date(franja_horaria_3[,"dia"], origin = "1970-01-01")
franja_horaria_3[,"dia_letra"] <- weekdays(franja_horaria_3[,"dia"])
franja_horaria <- franja_horaria_3
franja_horaria_hora_3 <- sqldf("select ano,trimestre,mes,day as dia,franja_horaria,driver_id,car_plate as placa,count(distinct trip_id) as valor from franja_horaria_2 group by ano,trimestre,mes,day,franja_horaria,driver_id,car_plate")
franja_horaria_hora_3[,"dia"] <- as.Date(franja_horaria_hora_3[,"dia"], origin = "1970-01-01")
franja_horaria_hora_3[,"dia_letra"] <- weekdays(franja_horaria_hora_3[,"dia"])
franja_horaria_hora <- franja_horaria_hora_3
### Carga
# Excel
save(franja_horaria,file = "/Users/manuelmateus/Ambiente_Virtual/Voltop/REPORTES/OPERACIONES/reporte_franja_horaria.RData")
save(franja_horaria_hora,file = "/Users/manuelmateus/Ambiente_Virtual/Voltop/REPORTES/OPERACIONES/franja_horaria_hora.RData")
## PostgreSQL
REPORTE_PROD = dbConnect(Postgres(),user = 'voltopdatauser',password = 'OtC6Vd2BtUOFc7gj',dbname = 'postgres',host = 'psql-voltopdata.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
dbGetQuery(REPORTE_PROD,statement="truncate report_time_frame");
dbGetQuery(REPORTE_PROD,statement="truncate report_time_frame_hour");
dbWriteTable(REPORTE_PROD,name = "report_time_frame",value = franja_horaria,overwrite = F,append = T,row.names = NA)
dbWriteTable(REPORTE_PROD,name = "report_time_frame_hour",value = franja_horaria_hora,overwrite = F,append = T,row.names = NA)
dbDisconnect(REPORTE_PROD)









