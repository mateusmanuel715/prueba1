#### Reporte Siniestralidad
### Librerias
rm(list = ls());gc();library(data.table);library(openxlsx);library(jsonlite);library(tidyverse);library(lubridate);library(readxl);library(sqldf);library(writexl);library(googlesheets4);library(googledrive);library(processx);library(RPostgres);options(sqldf.driver = "SQLite");Sys.setlocale("LC_TIME", "es_ES.UTF-8")
### Extraccion
ruta_json <- "/Users/manuelmateus/Ambiente_Virtual/Voltop/INSUMOS/still-smithy-407213-cbfd93ea82d1.json"
gs4_auth(email = NULL,path = ruta_json,scopes = "https://www.googleapis.com/auth/spreadsheets",cache = gargle::gargle_oauth_cache(),use_oob = gargle::gargle_oob_default(),token = NULL)
siniestros_1 <- data.frame(read_sheet('https://docs.google.com/spreadsheets/d/1TLjdow6hXNk4tFcVgaXoFyhhU8FKstOuaEdQVUv08pg/', sheet = "Reporte siniestralidad"))
### Transformacion
colnames(siniestros_1) <- c("fecha","hora_real","franja","placa","ciudad","reportado_aseguradora_equidad","n_siniestro","valor_paga_aseguradora","estado","pago_conductor_estado","pago_conductor_valor","pago_conductor_nombre","pago_grinest","accidente_reparación","pago_tercero_conductor","dias_inmovilizados","aseguradora_tercero","valor_aseguradora_tercero","nivel_siniestralidad","situacion","estatus_actual_caso","observacion_estatus","encargado_seguimiento","placa_terceros","facturas_1","facturas_2","facturas_3","dias_sin_operar")
siniestros_1[,"n_siniestro"] <- as.character(siniestros_1[,"n_siniestro"])
siniestros_1[,"valor_paga_aseguradora"] <- as.character(siniestros_1[,"valor_paga_aseguradora"])
siniestros_1[,"pago_grinest"] <- as.character(siniestros_1[,"pago_grinest"])
siniestros_1[,"pago_conductor_estado"] <- as.character(siniestros_1[,"pago_conductor_estado"])
siniestros_1[,"aseguradora_tercero"] <- as.character(siniestros_1[,"aseguradora_tercero"])
siniestros_1[,"dias_sin_operar"] <- as.character(siniestros_1[,"dias_sin_operar"])
siniestros_1[,"facturas_1"] <- as.character(siniestros_1[,"facturas_1"])
siniestros_1[,"pago_conductor_valor"] <- as.numeric(as.character(siniestros_1[,"pago_conductor_valor"]))
siniestros_1[,"valor_aseguradora_tercero"] <- as.numeric(as.character(siniestros_1[,"valor_aseguradora_tercero"]))
siniestros_1[,"pago_tercero_conductor"] <- as.numeric(as.character(siniestros_1[,"pago_tercero_conductor"]))
siniestros_1[,"valor_paga_aseguradora"] <- as.numeric(siniestros_1[,"valor_paga_aseguradora"])
siniestros_1[,"dias_sin_operar"] <- as.numeric(siniestros_1[,"dias_sin_operar"])
siniestros_1[,"pago_grinest"] <- as.numeric(siniestros_1[,"pago_grinest"])
siniestros_1[,"dias_inmovilizados"] <- as.numeric(siniestros_1[,"dias_inmovilizados"])
siniestros_1[,"dia"] <- as.Date(siniestros_1[,"fecha"])
siniestros_1[,"mes_numero"] <- month(siniestros_1[,"dia"])
siniestros_1[,"mes_texto"] <- format(siniestros_1[,"dia"], "%B")
siniestros_1[,"mes_texto"] <- str_to_title(siniestros_1[,"mes_texto"])
siniestros_1[,"mes"] <- paste0(siniestros_1[,"mes_numero"],". ",siniestros_1[,"mes_texto"])
siniestros_1[,"ano"] <- year(siniestros_1[,"dia"])
siniestros_1[,"trimestre"] <- paste0("Q", quarter(siniestros_1[,"dia"]), " ",siniestros_1[,"ano"])
siniestros_1[,"estatus_actual_caso"][is.na(siniestros_1[,"estatus_actual_caso"])] <- 0
siniestros_1[,"estado_actual_caso"] <- ifelse(siniestros_1[,"estatus_actual_caso"] == "Cerrado","Cerrado","Abierto")
siniestros_2 <- subset(siniestros_1, subset = {fecha >= "2023-01-01"})
siniestros_2[,"ciudad"][siniestros_2[,"ciudad"] == "BOGOTA"] <- "Bogotá"
siniestros_2[,"ciudad"][siniestros_2[,"ciudad"] == "MEDELLIN"] <- "Medellín"
siniestros_2[,"reportado_aseguradora_equidad"] <- ifelse(siniestros_2[,"reportado_aseguradora_equidad"] == TRUE,"Si","No")
siniestros_2[,"fecha"] <- as.Date(siniestros_2[,"fecha"])
siniestros_2[,"hora_real"] <- format(siniestros_2[,"hora_real"], "%H:%M:%S")
siniestros_2[,"valor_paga_aseguradora"][is.na(siniestros_2[,"valor_paga_aseguradora"])] <- 0
siniestros_2[,"pago_conductor_valor"][is.na(siniestros_2[,"pago_conductor_valor"])] <- 0
siniestros_2[,"pago_grinest"][is.na(siniestros_2[,"pago_grinest"])] <- 0
siniestros_2[,"valor_aseguradora_tercero"][is.na(siniestros_2[,"valor_aseguradora_tercero"])] <- 0
siniestros_2[,"pago_tercero_conductor"][is.na(siniestros_2[,"pago_tercero_conductor"])] <- 0
siniestros <- siniestros_2
# RData
save(siniestros,file = "/Users/manuelmateus/Ambiente_Virtual/Voltop/REPORTES/OPERACIONES/report_accidents.RData")
## PostgreSQL
REPORTE_PROD = dbConnect(Postgres(),user = 'voltopdatauser',password = 'OtC6Vd2BtUOFc7gj',dbname = 'postgres',host = 'psql-voltopdata.cluster-cdv7y65anzkn.us-east-1.rds.amazonaws.com',port = 5432,sslmode = 'require')
dbGetQuery(REPORTE_PROD,statement="truncate report_accidents");
dbWriteTable(REPORTE_PROD,name = "report_accidents",value = siniestros,overwrite = F,append = T,row.names = NA)
dbDisconnect(REPORTE_PROD)



