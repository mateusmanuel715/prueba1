#### Reporte Indicadores Operacionales y Financieros
### Librerias
rm(list = ls());gc();
library(openxlsx);library(jsonlite);library(tidyverse);library(lubridate);library(readxl);library(sqldf);library(writexl);library(googlesheets4);library(googledrive);library(processx);library(RPostgres);options(sqldf.driver = "SQLite")
### Extraccion
## PostgreSQL
OPERATIONAL_PROD = dbConnect(Postgres(), user = 'voltopuser',password = 'PaFqtjD4asat5ObF',dbname = 'postgres',host = 'psql-voltop.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
rider_payments_1 <- data.frame(dbGetQuery(OPERATIONAL_PROD, "SELECT * FROM rider_payments"))
driver_work_shifts <- data.frame(dbGetQuery(OPERATIONAL_PROD, "SELECT * FROM driver_work_shifts"))
work_shift_metrics <- data.frame(dbGetQuery(OPERATIONAL_PROD, "SELECT * FROM work_shift_metrics"))
dbDisconnect(OPERATIONAL_PROD)
REPORTE_PROD = dbConnect(Postgres(),user = 'voltopdatauser',password = 'OtC6Vd2BtUOFc7gj',dbname = 'postgres',host = 'psql-voltopdata.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
payments_order <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM rawdata_uber_payments_order"))
trips <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM rawdata_uber_trips"))
driver_activity <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM rawdata_uber_driver_activity"))
driver_quality <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM rawdata_uber_driver_quality"))
asociados <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM report_drivers"))
rider_payments_2 <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM rawdata_uber_rider_payments"))
distancia_recorrida <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM rawdata_hunter_car_distance"))
energia <- data.frame(dbGetQuery(REPORTE_PROD, "SELECT * FROM report_energy"))
dbDisconnect(REPORTE_PROD)
# Google Sheet
ruta_json <- "/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/still-smithy-407213-cbfd93ea82d1.json"
gs4_auth(email = NULL,path = ruta_json,scopes = "https://www.googleapis.com/auth/spreadsheets",cache = gargle::gargle_oauth_cache(),use_oob = gargle::gargle_oob_default(),token = NULL)
flota <- data.frame(read_sheet('https://docs.google.com/spreadsheets/d/1YHOEpscP78YaPZRZ6mZrhg20sGlDs7lSQX8xfyeO9uE/edit?gid=0#gid=0',col_types = "ccD"))
calendario <- data.frame(read_sheet('https://docs.google.com/spreadsheets/d/1Ew9ntxmLREZfEhCrsX438xOTVVYyyEQKpqr4uc54Pg8/', sheet = "semanas"))
# Excel 
### Transformacion
# Flota
flotaA <- subset(flota, subset = {Fecha.inicio.operación >= "2023-01-01"})
flota_1 <- flotaA %>% select(Placa,Fecha_inicio_operacion = Fecha.inicio.operación)
fecha_limite <- Sys.Date()
flota_2 <- flota_1 %>% filter(!is.na(Fecha_inicio_operacion)) %>% rowwise() %>% mutate(Dias = list(seq(Fecha_inicio_operacion, fecha_limite, by = "day"))) %>% unnest(Dias) %>% rename(Dia = Dias)
flota_2 <- data.frame(flota_2)
flota_2[,"state"] <- "Flota"
flota_2[,"status"] <- "Flota"
flota_2[,"Dia"] <- as.Date(flota_2[,"Dia"])
flota_2[,"mes_numero"] <- month(flota_2[,"Dia"])
flota_2[,"mes_texto"] <- format(flota_2[,"Dia"], "%B")
flota_2[,"mes_texto"] <- str_to_title(flota_2[,"mes_texto"])
flota_2[,"mes"] <- paste0(flota_2[,"mes_numero"],". ",flota_2[,"mes_texto"])
flota_2[,"ano"] <- year(flota_2[,"Dia"])
flota_2[,"trimestre"] <- paste0("Q", quarter(flota_2[,"Dia"])," ",flota_2[,"ano"])
flota_2[,"driver_id"] <- NA
flota_3 <- sqldf("select ano,trimestre,mes,Dia as dia,driver_id,Placa as placa,state,status from flota_2 group by ano,trimestre,mes,dia,driver_id,Placa,state,status")
flota_3[,"valor"] <- 0
flota_3[,"conductor"] <- NA
placa_ciudad <- sqldf("select car_plate as placa,city as ciudad from asociados group by car_plate")
flota_4 <- sqldf("select a.*,b.ciudad from flota_3 as a left join placa_ciudad as b on a.placa = b.placa")
flota_4[,"dia"] <- as.Date(flota_4[,"dia"], origin = "1970-01-01")
# Vehiculos en operacion
colnames(trips) <- c("trip_id","driver_id","driver_first_name","driver_last_name","car_id","car_plate","type_service","created_datetime","finished_datetime","start_address","end_address","total_distance","status")
trips_1 <- subset(trips, subset = {status == "completed"})
trips_2 <- sqldf("select * from trips group by trip_id")
asociados_1 <- sqldf("select * from asociados group by external_driver_id")
veh_operaciones <- subset(payments_order, subset = {description == "trip completed order"})
veh_operaciones[,"state"] <- "Vehículos en operacion"
veh_operaciones[,"status"] <- "Vehículos en operacion"
veh_operaciones[,"transaction_datetime"] <- as.character(veh_operaciones[,"transaction_datetime"])
veh_operaciones[,"day"] <- as.Date(substr(veh_operaciones[,"transaction_datetime"], 1, 10), format = "%Y-%m-%d")
veh_operaciones[,"mes_numero"] <- month(veh_operaciones[,"day"])
veh_operaciones[,"mes_texto"] <- format(veh_operaciones[,"day"], "%B")
veh_operaciones[,"mes_texto"] <- str_to_title(veh_operaciones[,"mes_texto"])
veh_operaciones[,"mes"] <- paste0(veh_operaciones[,"mes_numero"],". ",veh_operaciones[,"mes_texto"])
veh_operaciones[,"ano"] <- year(veh_operaciones[,"day"])
veh_operaciones[,"trimestre"] <- paste0("Q", quarter(veh_operaciones[,"day"])," ",veh_operaciones[,"ano"])
veh_operaciones_1 <- sqldf("select a.*,b.car_plate from veh_operaciones as a left join trips_2 as b on a.trip_id = b.trip_id")
veh_operaciones_1[,"driver_id"] <- NA
veh_operaciones_2 <- sqldf("select ano,trimestre,mes,day as dia,driver_id,car_plate as placa,state,status from veh_operaciones_1 group by ano,trimestre,mes,dia,driver_id,car_plate,state,status")
veh_operaciones_2[,"valor"] <- 0
veh_operaciones_2[,"conductor"] <- NA
veh_ciudad <- sqldf("select car_plate as placa,city as ciudad from asociados group by car_plate")
veh_operaciones_3 <- sqldf("select a.*,b.ciudad from veh_operaciones_2 as a left join veh_ciudad as b on a.placa = b.placa")
# Asociados en operacion
colnames(trips) <- c("trip_id","driver_id","driver_first_name","driver_last_name","car_id","car_plate","type_service","created_datetime","finished_datetime","start_address","end_address","total_distance","status")
trips_1 <- subset(trips, subset = {status == "completed"})
trips_2 <- sqldf("select * from trips group by trip_id")
asociados_1 <- sqldf("select * from asociados group by driver_id")
aso_operaciones <- subset(payments_order, subset = {description == "trip completed order"})
aso_operaciones[,"state"] <- "Asociados en operacion"
aso_operaciones[,"status"] <- "Asociados en operacion"
aso_operaciones[,"transaction_datetime"] <- as.character(aso_operaciones[,"transaction_datetime"])
aso_operaciones[,"day"] <- as.Date(substr(aso_operaciones[,"transaction_datetime"], 1, 10), format = "%Y-%m-%d")
aso_operaciones[,"mes_numero"] <- month(aso_operaciones[,"day"])
aso_operaciones[,"mes_texto"] <- format(aso_operaciones[,"day"], "%B")
aso_operaciones[,"mes_texto"] <- str_to_title(aso_operaciones[,"mes_texto"])
aso_operaciones[,"mes"] <- paste0(aso_operaciones[,"mes_numero"],". ",aso_operaciones[,"mes_texto"])
aso_operaciones[,"ano"] <- year(aso_operaciones[,"day"])
aso_operaciones[,"trimestre"] <- paste0("Q", quarter(aso_operaciones[,"day"])," ",aso_operaciones[,"ano"])
aso_operaciones_1 <- sqldf("select a.*,b.car_plate from aso_operaciones as a left join trips_2 as b on a.trip_id = b.trip_id")
aso_operaciones_2 <- sqldf("select ano,trimestre,mes,day as dia,state,status,driver_id,car_plate as placa from aso_operaciones_1 group by ano,trimestre,mes,dia,state,status,driver_id,car_plate")
aso_operaciones_2[,"valor"] <- 0
aso_operaciones_3 <- sqldf("select a.*,b.driver_name as conductor,b.city as ciudad from aso_operaciones_2 as a left join asociados_1 as b on a.driver_id = b.external_driver_id")
aso_operaciones_4 <- aso_operaciones_3 %>% select(ano,trimestre,mes,dia,driver_id,placa,state,status,valor,conductor,ciudad)
# Viajes completos
viajes_asociados <- subset(payments_order, subset = {description == "trip completed order"})
viajes_asociados[,"state"] <- "Viajes finalizados"
viajes_asociados[,"status"] <- "Viajes finalizados"
viajes_asociados[,"transaction_datetime"] <- as.character(viajes_asociados[,"transaction_datetime"])
viajes_asociados[,"day"] <- as.Date(substr(viajes_asociados[,"transaction_datetime"], 1, 10), format = "%Y-%m-%d")
viajes_asociados[,"mes_numero"] <- month(viajes_asociados[,"day"])
viajes_asociados[,"mes_texto"] <- format(viajes_asociados[,"day"], "%B")
viajes_asociados[,"mes_texto"] <- str_to_title(viajes_asociados[,"mes_texto"])
viajes_asociados[,"mes"] <- paste0(viajes_asociados[,"mes_numero"],". ",viajes_asociados[,"mes_texto"])
viajes_asociados[,"ano"] <- year(viajes_asociados[,"day"])
viajes_asociados[,"trimestre"] <- paste0("Q", quarter(viajes_asociados[,"day"])," ",viajes_asociados[,"ano"])
viajes_asociados_1 <- sqldf("select a.*,b.car_plate from viajes_asociados as a left join trips_2 as b on a.trip_id = b.trip_id")
viajes_asociados_2 <- sqldf("select ano,trimestre,mes,day as dia,driver_id,car_plate as placa,state,status,count(distinct trip_id) as valor from viajes_asociados_1 group by ano,trimestre,mes,day,driver_id,car_plate,state,status")
viajes_asociados_3 <- sqldf("select a.*,b.driver_name as conductor,b.city as ciudad from viajes_asociados_2 as a left join asociados_1 as b on a.driver_id = b.external_driver_id")
viajes_asociados_3[,"conductor"][is.na(viajes_asociados_3[,"conductor"])] <- 0
viajes_asociados_3[,"ciudad"][is.na(viajes_asociados_3[,"ciudad"])] <- 0
viajes_asociados_3[,"mes"][is.na(viajes_asociados_3[,"mes"])] <- 0
viajes_asociados_4 <- subset(viajes_asociados_3, subset = {mes != 0})
viajes_asociados_5 <- subset(viajes_asociados_4, subset = {mes != "00000000-0000-0000-0000-000000000000"})
# Viajes cancelados
viajes_cancel_1 <- subset(trips, subset = {!(status == "completed" | status == "fare_split" | status == "delivery_failed")})
viajes_cancel_1[,"state"] <- "Viajes cancelados"
viajes_cancel_1[,"transaction_datetime"] <- as.character(viajes_cancel_1[,"created_datetime"])
viajes_cancel_1[,"day"] <- as.Date(substr(viajes_cancel_1[,"transaction_datetime"], 1, 10), format = "%Y-%m-%d")
viajes_cancel_1[,"mes_numero"] <- month(viajes_cancel_1[,"day"])
viajes_cancel_1[,"mes_texto"] <- format(viajes_cancel_1[,"day"], "%B")
viajes_cancel_1[,"mes_texto"] <- str_to_title(viajes_cancel_1[,"mes_texto"])
viajes_cancel_1[,"mes"] <- paste0(viajes_cancel_1[,"mes_numero"],". ",viajes_cancel_1[,"mes_texto"])
viajes_cancel_1[,"ano"] <- year(viajes_cancel_1[,"day"])
viajes_cancel_1[,"trimestre"] <- paste0("Q", quarter(viajes_cancel_1[,"day"])," ",viajes_cancel_1[,"ano"])
viajes_cancel_2 <- sqldf("select ano,trimestre,mes,day as dia,driver_id,car_plate as placa,state,status,count(distinct trip_id) as valor from viajes_cancel_1 group by ano,trimestre,mes,dia,driver_id,car_plate,state,status")
viajes_cancel_3 <- sqldf("select a.*,b.driver_name as conductor,b.city as ciudad from viajes_cancel_2 as a left join asociados_1 as b on a.driver_id = b.external_driver_id")
viajes_cancel_3[,"status"][viajes_cancel_3[,"status"] == "driver_cancelled"] <- "Cancelado por conductor"
viajes_cancel_3[,"status"][viajes_cancel_3[,"status"] == "rider_cancelled"] <- "Cancelado por usuario"
# Km recorridos
kmA <- subset(payments_order, subset = {description == "trip completed order"})
km <- sqldf("select * from kmA group by trip_id")
km[,"state"] <- "Km"
km[,"status"] <- "Km Operacion"
km[,"transaction_datetime"] <- as.character(km[,"transaction_datetime"])
km[,"day"] <- as.Date(substr(km[,"transaction_datetime"], 1, 10), format = "%Y-%m-%d")
km[,"dia"] <- as.Date(km[,"day"])
km[,"mes_numero"] <- month(km[,"dia"])
km[,"mes_texto"] <- format(km[,"dia"], "%B")
km[,"mes_texto"] <- str_to_title(km[,"mes_texto"])
km[,"mes"] <- paste0(km[,"mes_numero"],". ",km[,"mes_texto"])
km[,"ano"] <- year(km[,"dia"])
km[,"trimestre"] <- paste0("Q", quarter(km[,"dia"])," ",km[,"ano"])
km_1 <- sqldf("select a.*,b.car_plate,b.total_distance from km as a left join trips_2 as b on a.trip_id = b.trip_id")
km_2 <- sqldf("select ano,trimestre,mes,dia,driver_id,car_plate as placa,state,status,sum(total_distance) as valor from km_1 group by ano,trimestre,mes,dia,driver_id,car_plate,state,status")
km_3 <- sqldf("select a.*,b.driver_name as conductor,b.city as ciudad from km_2 as a left join asociados_1 as b on a.driver_id = b.external_driver_id")
### Km vehiculo
km_vehiculo <- distancia_recorrida
km_vehiculo[,"state"] <- "Km"
km_vehiculo[,"status"] <- "Km Vehiculo"
km_vehiculo[,"mes_numero"] <- month(km_vehiculo[,"dia"])
km_vehiculo[,"mes_texto"] <- format(km_vehiculo[,"dia"], "%B")
km_vehiculo[,"mes_texto"] <- str_to_title(km_vehiculo[,"mes_texto"])
km_vehiculo[,"mes"] <- paste0(km_vehiculo[,"mes_numero"],". ",km_vehiculo[,"mes_texto"])
km_vehiculo[,"ano"] <- year(km_vehiculo[,"dia"])
km_vehiculo[,"trimestre"] <- paste0("Q", quarter(km_vehiculo[,"dia"])," ",km_vehiculo[,"ano"])
km_vehiculo[,"driver_id"] <- NA
km_vehiculo_1 <- sqldf("select ano,trimestre,mes,dia,driver_id,placa,state,status,sum(Distancia_recorrida_1) as valor from km_vehiculo group by ano,trimestre,mes,dia,driver_id,placa,state,status")
km_vehiculo_1[,"conductor"] <- NA
km_vehiculo_2 <- sqldf("select a.*,b.ciudad from km_vehiculo_1 as a left join placa_ciudad as b on a.placa = b.placa")
# Horas en operacion
horas_operacionA <- subset(payments_order, subset = {description == "trip completed order"})
horas_operacion <- sqldf("select * from horas_operacionA group by trip_id")
horas_operacion[,"state"] <- "Horas en operación"
horas_operacion[,"status"] <- "Horas en operación"
horas_operacion[,"transaction_datetime"] <- as.character(horas_operacion[,"transaction_datetime"])
horas_operacion[,"dia"] <- as.Date(substr(horas_operacion[,"transaction_datetime"], 1, 10), format = "%Y-%m-%d")
horas_operacion[,"mes_numero"] <- month(horas_operacion[,"dia"])
horas_operacion[,"mes_texto"] <- format(horas_operacion[,"dia"], "%B")
horas_operacion[,"mes_texto"] <- str_to_title(horas_operacion[,"mes_texto"])
horas_operacion[,"mes"] <- paste0(horas_operacion[,"mes_numero"],". ",horas_operacion[,"mes_texto"])
horas_operacion[,"ano"] <- year(horas_operacion[,"dia"])
horas_operacion[,"trimestre"] <- paste0("Q", quarter(horas_operacion[,"dia"])," ",horas_operacion[,"ano"])
horas_operacion_1 <- sqldf("select a.*,b.car_plate,b.created_datetime,b.finished_datetime from horas_operacion as a left join trips_2 as b on a.trip_id = b.trip_id")
horas_operacion_1[,"horas_operacion"] = as.numeric(difftime(horas_operacion_1[,"finished_datetime"], horas_operacion_1[,"created_datetime"],units = "hours"))
horas_operacion_2 <- sqldf("select ano,trimestre,mes,dia,driver_id,car_plate as placa,state,status,sum(horas_operacion) as valor from horas_operacion_1 group by ano,trimestre,mes,dia,driver_id,car_plate,state,status")
horas_operacion_3 <- sqldf("select a.*,b.driver_name as conductor,b.city as ciudad from horas_operacion_2 as a left join asociados_1 as b on a.driver_id = b.external_driver_id")
# Horas disponibles vehiculo
horas_disponibles_flota_1 <- flota_4
horas_disponibles_flota_1[,"state"] <- "Horas disponibles"
horas_disponibles_flota_1[,"status"] <- "Horas disponibles"
horas_disponibles_flota_1[,"valor"] <- 24
# Horas disponibles asociado
horas_disponibles_aso_1 <- aso_operaciones_4
horas_disponibles_aso_1[,"state"] <- "Horas disponibles"
horas_disponibles_aso_1[,"status"] <- "Horas disponibles asociado"
horas_disponibles_aso_1[,"valor"] <- 12
# Horas conectadas
colnames(driver_activity) <- c("driver_id","driver_first_name","driver_last_name","viajes_completados","tiempo_conexion","tiempo_viaje","fecha")
horas_conectadas_1 <- driver_activity %>% select(driver_id,fecha,tiempo_conexion)
horas_conectadas_1[,"tiempo_conexion"] <- as.numeric(horas_conectadas_1[,"tiempo_conexion"]) / 60
horas_conectadas_1[,"state"] <- "Horas conectadas"
horas_conectadas_1[,"status"] <- "Horas conectadas"
horas_conectadas_1[,"dia"] <- as.Date(horas_conectadas_1[,"fecha"])
horas_conectadas_1[,"mes_numero"] <- month(horas_conectadas_1[,"dia"])
horas_conectadas_1[,"mes_texto"] <- format(horas_conectadas_1[,"dia"], "%B")
horas_conectadas_1[,"mes_texto"] <- str_to_title(horas_conectadas_1[,"mes_texto"])
horas_conectadas_1[,"mes"] <- paste0(horas_conectadas_1[,"mes_numero"],". ",horas_conectadas_1[,"mes_texto"])
horas_conectadas_1[,"ano"] <- year(horas_conectadas_1[,"dia"])
horas_conectadas_1[,"trimestre"] <- paste0("Q", quarter(horas_conectadas_1[,"dia"])," ",horas_conectadas_1[,"ano"])
horas_conectadas_1[,"llave"] <- paste0(horas_conectadas_1[,"driver_id"],horas_conectadas_1[,"dia"])
trips_2[,"dia"] <- as.Date(trips_2[,"created_datetime"])
trips_2[,"llave"] <- paste0(trips_2[,"driver_id"],trips_2[,"dia"])
trips_3 <- sqldf("select llave,car_plate from trips_2 group by llave")
horas_conectadas_2 <- sqldf("select a.*,b.car_plate from horas_conectadas_1 as a left join trips_3 as b on a.llave = b.llave")
horas_conectadas_3 <- sqldf("select ano,trimestre,mes,dia,driver_id,car_plate as placa,state,status,sum(tiempo_conexion) as valor from horas_conectadas_2 group by ano,trimestre,mes,dia,driver_id,car_plate,state,status")
horas_conectadas_4 <- sqldf("select a.*,b.driver_name as conductor,b.city as ciudad from horas_conectadas_3 as a left join asociados_1 as b on a.driver_id = b.external_driver_id")
# Ingresos brutos
rider_payments <- rbind(rider_payments_1,rider_payments_2)
ingresos_brutos_1 <- rider_payments %>% select(trip_id,driver_id,day,rider_pays_local)
ingresos_brutos_1[,"state"] <- "Ingresos"
ingresos_brutos_1[,"status"] <- "GMV"
ingresos_brutos_1[,"dia"] <- as.Date(ingresos_brutos_1[,"day"])
ingresos_brutos_1[,"mes_numero"] <- month(ingresos_brutos_1[,"dia"])
ingresos_brutos_1[,"mes_texto"] <- format(ingresos_brutos_1[,"dia"], "%B")
ingresos_brutos_1[,"mes_texto"] <- str_to_title(ingresos_brutos_1[,"mes_texto"])
ingresos_brutos_1[,"mes"] <- paste0(ingresos_brutos_1[,"mes_numero"],". ",ingresos_brutos_1[,"mes_texto"])
ingresos_brutos_1[,"ano"] <- year(ingresos_brutos_1[,"dia"])
ingresos_brutos_1[,"trimestre"] <- paste0("Q", quarter(ingresos_brutos_1[,"dia"])," ",ingresos_brutos_1[,"ano"])
ingresos_brutos_1[,"rider_pays_local"] <- ingresos_brutos_1[,"rider_pays_local"]*(-1)
ingresos_brutos_2 <- sqldf("select a.*,b.car_plate as placa from ingresos_brutos_1 as a left join trips_2 as b on a.trip_id = b.trip_id")
ingresos_brutos_3 <- sqldf("select ano,trimestre,mes,dia,driver_id,placa,state,status,sum(rider_pays_local) as valor from ingresos_brutos_2 group by ano,trimestre,mes,dia,driver_id,placa,state,status")
ingresos_brutos_4 <- sqldf("select a.*,b.driver_name as conductor,b.city as ciudad from ingresos_brutos_3 as a left join asociados_1 as b on a.driver_id = b.external_driver_id")
# Ganancias asociados
ganancias_asociados <- subset(payments_order, subset = {variable == "net_earning"})
ganancias_asociados[,"state"] <- "Ingresos"
ganancias_asociados[,"status"] <- "Ganancias asociados"
ganancias_asociados[,"transaction_datetime"] <- as.character(ganancias_asociados[,"transaction_datetime"])
ganancias_asociados[,"dia"] <- as.Date(substr(ganancias_asociados[,"transaction_datetime"], 1, 10), format = "%Y-%m-%d")
ganancias_asociados[,"mes_numero"] <- month(ganancias_asociados[,"dia"])
ganancias_asociados[,"mes_texto"] <- format(ganancias_asociados[,"dia"], "%B")
ganancias_asociados[,"mes_texto"] <- str_to_title(ganancias_asociados[,"mes_texto"])
ganancias_asociados[,"mes"] <- paste0(ganancias_asociados[,"mes_numero"],". ",ganancias_asociados[,"mes_texto"])
ganancias_asociados[,"ano"] <- year(ganancias_asociados[,"dia"])
ganancias_asociados[,"trimestre"] <- paste0("Q", quarter(ganancias_asociados[,"dia"])," ",ganancias_asociados[,"ano"])
ganancias_asociadosA <- subset(ganancias_asociados, subset = {transaction_id != "b26357cf-e979-3377-b15a-fd2b5bfb6bdb"})
ganancias_asociados_1 <- sqldf("select a.*,b.car_plate as placa from ganancias_asociadosA as a left join trips_2 as b on a.trip_id = b.trip_id")
ganancias_asociados_2 <- sqldf("select ano,trimestre,mes,dia,driver_id,placa,state,status,sum(value) as valor from ganancias_asociados_1 group by ano,trimestre,mes,dia,driver_id,placa,state,status")
ganancias_asociados_3 <- sqldf("select a.*,b.driver_name as conductor,b.city as ciudad from ganancias_asociados_2 as a left join asociados_1 as b on a.driver_id = b.external_driver_id")
ganancias_asociados_3[,"placa"][is.na(ganancias_asociados_3[,"placa"])] <- 0
con_placa <- subset(ganancias_asociados_3, subset = {placa != 0})
con_placa_1 <- sqldf("select dia,driver_id,placa from con_placa group by dia,driver_id")
con_placa_1[,"llave"] <- paste0(con_placa_1[,"dia"],con_placa_1[,"driver_id"])
sin_placa <- subset(ganancias_asociados_3, subset = {placa == 0})
sin_placa[,"llave"] <- paste0(sin_placa[,"dia"],sin_placa[,"driver_id"])
sin_placa_1 <- sqldf("select a.*,b.placa as placa1 from sin_placa as a left join con_placa_1 as b on a.llave = b.llave")
sin_placa_1[,"placa1"][is.na(sin_placa_1[,"placa1"])] <- 0
sin_placa_1[,"placa1"][sin_placa_1[,"driver_id"] == "2cb61a90-04b1-4b22-8fdc-aa32c0f0d562" & sin_placa_1[,"dia"] == "2024-11-05"] <- "KXQ156"
sin_placa_1[,"placa1"][sin_placa_1[,"driver_id"] == "ed817f20-8f43-424e-ab8c-dd70026c667b" & sin_placa_1[,"dia"] == "2024-10-07"] <- "KXN689"
sin_placa_1[,"placa1"][sin_placa_1[,"driver_id"] == "017aeeb9-06a5-492b-ad9c-a90c50f63550" & sin_placa_1[,"mes"] == "5. Mayo"] <- "LZV601"
sin_placa_1[,"placa1"][sin_placa_1[,"driver_id"] == "50b331b7-34e8-4767-b66b-72543ab1997d" & sin_placa_1[,"mes"] == "5. Mayo"] <- "LZV602"
sin_placa_1[,"placa1"][sin_placa_1[,"driver_id"] == "6b36b6d7-8ba9-4d10-88ee-a85dd4876017" & sin_placa_1[,"mes"] == "5. Mayo"] <- "LZV603"
sin_placa_1[,"placa1"][sin_placa_1[,"driver_id"] == "98cc329d-fc58-4eee-bb15-3b3acc7be433" & sin_placa_1[,"mes"] == "5. Mayo"] <- "LZV601"
sin_placa_1[,"placa"] <- sin_placa_1[,"placa1"]
sin_placa_2 <- sin_placa_1 %>% select(-placa1,-llave)
ganancias_asociados_4 <- rbind(con_placa,sin_placa_2)
## Energia
energia_1 <- energia
energia_1[,"state"] <- "Energia"
energia_1[,"status"] <- "Carga Vehiculo"
energia_1[,"mes_numero"] <- month(energia_1[,"dia"])
energia_1[,"mes_texto"] <- format(energia_1[,"dia"], "%B")
energia_1[,"mes_texto"] <- str_to_title(energia_1[,"mes_texto"])
energia_1[,"mes"] <- paste0(energia_1[,"mes_numero"],". ",energia_1[,"mes_texto"])
energia_1[,"ano"] <- year(energia_1[,"dia"])
energia_1[,"trimestre"] <- paste0("Q", quarter(energia_1[,"dia"])," ",energia_1[,"ano"])
energia_1[,"driver_id"] <- NA
energia_2 <- sqldf("select ano,trimestre,mes,dia,driver_id,usuario as placa,state,status,sum(energia_wkh) as valor from energia_1 group by ano,trimestre,mes,dia,driver_id,usuario,state,status")
energia_2[,"conductor"] <- NA
energia_3 <- sqldf("select a.*,b.ciudad from energia_2 as a left join placa_ciudad as b on a.placa = b.placa")
# Reporte
operaciones_1 <- rbind(flota_4,veh_operaciones_3,aso_operaciones_4,viajes_asociados_5,viajes_cancel_3,km_3,km_vehiculo_2,horas_disponibles_flota_1,horas_disponibles_aso_1,horas_operacion_3,horas_conectadas_4,ingresos_brutos_4,ganancias_asociados_4,energia_3)
operaciones_1[,"valor"] <- as.numeric(operaciones_1[,"valor"])
operaciones_1[,"ano"] <- as.numeric(operaciones_1[,"ano"])
operaciones_2 <- operaciones_1 %>% arrange(desc(dia))
operaciones_3 <- subset(operaciones_2, subset = {dia != Sys.Date()})
dias <- sqldf("select dia from operaciones_3 group by dia")
calendario[,"fecha_inicio"] <- as.Date(calendario[,"fecha_inicio"], format = "%Y%m%d")
calendario[,"fecha_fin"] <- as.Date(calendario[,"fecha_fin"], format = "%Y%m%d")
dias[,"semana"] <- 0; for (i in 1:dim(dias)[1]) {for(j in 1:dim(calendario)[1]){dias[i,"semana"][dias[i,"dia"] >= calendario[j,"fecha_inicio"] & dias[i,"dia"] <= calendario[j,"fecha_fin"]] <- calendario[j,"semana"]}}
operaciones_4 <- sqldf("select a.*,b.semana from operaciones_3 as a left join dias as b on a.dia = b.dia")
operaciones <- operaciones_4
### Carga
# Excel
save(operaciones,file = "/Users/manuelmateus/Ambiente_Virtual/Voltop/REPORTES/OPERACIONES/reporte_ind_opera_finan.RData")
## PostgreSQL
REPORTE_PROD = dbConnect(Postgres(),user = 'voltopdatauser',password = 'OtC6Vd2BtUOFc7gj',dbname = 'postgres',host = 'psql-voltopdata.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
dbGetQuery(REPORTE_PROD,statement="truncate report_kpi_operations_finance");
dbWriteTable(REPORTE_PROD,name = "report_kpi_operations_finance",value = operaciones,overwrite = F,append = T,row.names = NA)
dbDisconnect(REPORTE_PROD)








 







