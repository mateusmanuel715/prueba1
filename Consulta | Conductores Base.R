#### Reporte Conductores Base
### Librerias
#rm(list = ls());gc();
library(openxlsx);library(jsonlite);library(tidyverse);library(lubridate);library(readxl);library(sqldf);library(writexl);library(googlesheets4);library(googledrive);library(processx);library(RPostgres);options(sqldf.driver = "SQLite")
### Extraccion
## PostgreSQL
REPORTE_PROD = dbConnect(Postgres(),user = 'voltopdatauser',password = 'OtC6Vd2BtUOFc7gj',dbname = 'postgres',host = 'psql-voltopdata.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
#payments_order <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM rawdata_uber_payments_order"))
#trips <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM rawdata_uber_trips"))
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
conductores_baseA <- subset(payments_order, subset = {transaction_datetime >= "2024-09-02" & transaction_datetime <= "2024-12-01"})
conductores_base <- subset(conductores_baseA, subset = {variable == "net_earning"})
conductores_base[,"transaction_datetime"] <- as.character(conductores_base[,"transaction_datetime"])
conductores_base[,"dia"] <- as.Date(substr(conductores_base[,"transaction_datetime"], 1, 10), format = "%Y-%m-%d")
conductores_base[,"mes_numero"] <- month(conductores_base[,"dia"])
conductores_base[,"mes_texto"] <- format(conductores_base[,"dia"], "%B")
conductores_base[,"mes_texto"] <- str_to_title(conductores_base[,"mes_texto"])
conductores_base[,"mes"] <- paste0(conductores_base[,"mes_numero"],". ",conductores_base[,"mes_texto"])
conductores_base[,"ano"] <- year(conductores_base[,"dia"])
conductores_base[,"trimestre"] <- paste0("Q", quarter(conductores_base[,"dia"])," ",conductores_base[,"ano"])
conductores_baseA <- subset(conductores_base, subset = {transaction_id != "b26357cf-e979-3377-b15a-fd2b5bfb6bdb"})
conductores_base_1 <- sqldf("select a.*,b.car_plate as placa from conductores_baseA as a left join trips_2 as b on a.trip_id = b.trip_id")
conductores_base_2 <- sqldf("select ano,trimestre,mes,dia,driver_id,placa,sum(value) as valor from conductores_base_1 group by ano,trimestre,mes,dia,driver_id,placa")
conductores_base_3 <- sqldf("select a.*,b.driver_name as conductor,b.city as ciudad from conductores_base_2 as a left join asociados_1 as b on a.driver_id = b.external_driver_id")
conductores_base_3[,"placa"][is.na(conductores_base_3[,"placa"])] <- 0
con_placa <- subset(conductores_base_3, subset = {placa != 0})
con_placa_1 <- sqldf("select dia,driver_id,placa from con_placa group by dia,driver_id")
con_placa_1[,"llave"] <- paste0(con_placa_1[,"dia"],con_placa_1[,"driver_id"])
sin_placa <- subset(conductores_base_3, subset = {placa == 0})
sin_placa[,"llave"] <- paste0(sin_placa[,"dia"],sin_placa[,"driver_id"])
sin_placa_1 <- sqldf("select a.*,b.placa as placa1 from sin_placa as a left join con_placa_1 as b on a.llave = b.llave")
sin_placa_1[,"placa1"][is.na(sin_placa_1[,"placa1"])] <- 0
sin_placa_1[,"placa1"][sin_placa_1[,"driver_id"] == "2cb61a90-04b1-4b22-8fdc-aa32c0f0d562" & sin_placa_1[,"dia"] == "2024-11-05"] <- "KXQ156"
sin_placa_1[,"placa1"][sin_placa_1[,"driver_id"] == "ed817f20-8f43-424e-ab8c-dd70026c667b" & sin_placa_1[,"dia"] == "2024-10-07"] <- "KXN689"
sin_placa_1[,"placa"] <- sin_placa_1[,"placa1"]
sin_placa_2 <- sin_placa_1 %>% select(-placa1,-llave)
conductores_base_4 <- rbind(con_placa,sin_placa_2)
dias <- sqldf("select dia from conductores_base_4 group by dia")
calendario[,"fecha_inicio"] <- as.Date(calendario[,"fecha_inicio"], format = "%Y%m%d")
calendario[,"fecha_fin"] <- as.Date(calendario[,"fecha_fin"], format = "%Y%m%d")
dias[,"semana"] <- 0; for (i in 1:dim(dias)[1]) {for(j in 1:dim(calendario)[1]){dias[i,"semana"][dias[i,"dia"] >= calendario[j,"fecha_inicio"] & dias[i,"dia"] <= calendario[j,"fecha_fin"]] <- calendario[j,"semana"]}}
conductores_base_5 <- sqldf("select a.*,b.semana from conductores_base_4 as a left join dias as b on a.dia = b.dia")
conductores_base_5A <- sqldf("select a.*,b.mes1 from conductores_base_5 as a left join calendario as b on a.semana = b.semana")
conductores_base_6 <- sqldf("select ano,trimestre,mes1 as mes,semana,driver_id,conductor,sum(valor) as facturacion_semanal from conductores_base_5A group by ano,trimestre,mes1,semana,driver_id")
conductores_base_6A <- sqldf("select ano,trimestre,mes,semana,driver_id,conductor,count() as semanas_facturadas from conductores_base_5A group by ano,trimestre,mes,semana,driver_id")
conductores_base_7 <- sqldf("select a.*,b.semanas_facturadas from conductores_base_6 as a left join conductores_base_6A as b on a.driver_id = b.driver_id")
conductores_base_8 <- conductores_base_7 %>% arrange(desc(semanas_facturadas),desc(driver_id),desc(semana))
conductores_base <- conductores_base_8
### Carga
# RData
write_xlsx(conductores_base,"/Users/manuelmateus/Ambiente_Virtual/Voltop/REPORTES/OPERACIONES/conductores_base.xlsx")




