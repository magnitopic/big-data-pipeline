import pandas as pd
import time
import os

input_user = 0
documento_csv = ""
columna_csv = ""
new_name = ""
año_filtro = 0
mes_filtro = 0
dia_filtro = 0
hora_inicio = 0
hora_fin = 0

os.system('cls' if os.name == 'nt' else 'clear') # Limpiar pantalla

while True :
    os.system('cls' if os.name == 'nt' else 'clear') # Limpiar pantalla
    print("---------------------------------------------------")
    print("1) Muestra los posibles datos de una columna (CSV)")
    print("2) Elimina columnas de bajo valor (LCD)")
    print("3) Filtra los registros por fecha y hora (LCD)")
    print("0) Salir")
    print("---------------------------------------------------")
    input_user = input("Opcion: ")
    match input_user:
        case 1:
            os.system('cls' if os.name == 'nt' else 'clear') # Limpiar pantalla
            print("---------------------------------------------")
            documento_csv = input("Nombre del documento CSV: ")
            columna_csv = input("Nombre de la columna del CSV: ")
            
            df = pd.read_csv(documento_csv)
            unique_causes = df[columna_csv].drop_duplicates()

            print("---------------------------------------------")
            print(f"Datos posibles de la columna {columna_csv}: ")
            print(unique_causes.to_list())
            print("---------------------------------------------")
            input("Continuar...")
        case 2:
            os.system('cls' if os.name == 'nt' else 'clear') # Limpiar pantalla
            print("---------------------------------------------------")
            print("1) Por defecto")
            print("2) Personalizado")
            print("0) Salir")
            print("---------------------------------------------------")
            input_user = input("Opcion: ")
            match input_user:
                case 1:
                    documento_csv = "LCD_AAI0000TNCA_2023.csv"      
                    new_name = "weather_lcd_prueba.csv"  
                case 2:
                    print("---------------------------------------------")
                    documento_csv = input("Nombre del documento CSV: ")
                    columna_csv = input("Nombre del nuevo documento CSV: ")
                case 0:
                    break  
                case _:
                    print("----------------")
                    print("Opcion no valida")
                    print("----------------")
                    time.sleep(1)

            columnas_deseadas = [
                "STATION",
                "NAME",
                "LATITUDE",
                "LONGITUDE",
                "ELEVATION",
                "DATE",
                "HourlyAltimeterSetting",
                "HourlyDewPointTemperature",
                "HourlyDryBulbTemperature",
                "HourlyPrecipitation",
                "HourlyPresentWeatherType",
                "HourlyPressureChange",
                "HourlyPressureTendency",
                "HourlyRelativeHumidity",
                "HourlySkyConditions",
                "HourlySeaLevelPressure",
                "HourlyVisibility",
                "HourlyWetBulbTemperature",
                "HourlyWindDirection",
                "HourlyWindGustSpeed",
                "HourlyWindSpeed"
            ]

            df = pd.read_csv(nombre_actual_csv)
            columnas_existentes = [c for c in columnas_deseadas if c in df.columns]
            df_filtrado = df[columnas_existentes]

            # Guardar CSV resultante
            df_filtrado.to_csv(new_name, index=False)

            print(f"CSV filtrado creado correctamente: {new_name}")
            input("Continuar...")
        case 3:
            os.system('cls' if os.name == 'nt' else 'clear') # Limpiar pantalla
            print("---------------------------------------------------")
            print("1) Por defecto")
            print("2) Personalizado")
            print("0) Salir")
            print("---------------------------------------------------")
            input_user = input("Opcion: ")
            match input_user:
                case 1:
                    documento_csv = "weather_lcd_prueba.csv"
                    año_filtro = 2023
                    mes_filtro = 8
                    dia_filtro = 1
                    hora_inicio = 15
                    hora_fin = 18  
                case 2:
                    print("---------------------------------------------")
                    documento_csv = input("Nombre del CSV: ")
                    año_filtro = input("Año de filtrado: ")
                    mes_filtro = input("Mes de filtrado: ")
                    dia_filtro = input("Dia de filtrado: ")
                    hora_inicio = input("Hora inicial de filtrado: ")
                    hora_fin = input("Hora final de filtrado: ")
                case 0:
                    break  
                case _:
                    print("----------------")
                    print("Opcion no valida")
                    print("----------------")
                    time.sleep(1)

            # Cargar el CSV y convertir 'DATE' a datetime
            df = pd.read_csv(documento_csv, parse_dates=['DATE'])

            # Filtrar por año, mes y día
            df_fecha_hora  = df[
                (df['DATE'].dt.year == año_filtro) &
                (df['DATE'].dt.month == mes_filtro) &
                (df['DATE'].dt.day == dia_filtro) &
                (df['DATE'].dt.hour >= hora_inicio) &
                (df['DATE'].dt.hour <= hora_fin)
            ]
            print(df_fecha_hora)
            input("Continuar...")

            os.system('cls' if os.name == 'nt' else 'clear')

            # Preguntar al usuario si desea crear un CSV
            print("--------------------------------------------------")
            respuesta = input("¿Deseas guardar estos datos en un CSV? (sí/no): ").strip().lower()
            if respuesta == "sí" or respuesta == "si":
                archivo_salida = "resultado_filtrado.csv"
                df_fecha_hora.to_csv(archivo_salida, index=False)
                print(f"Datos filtrados guardados en {archivo_salida}")
                print("--------------------------------------------------")
            else:
                print("No se ha creado ningún CSV.")
                print("--------------------------------------------------")
            input("Continuar...")
        case 0:
            break  
        case _:
            print("----------------")
            print("Opcion no valida")
            print("----------------")
            time.sleep(1)