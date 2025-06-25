import argparse
import os
import pandas as pd
from datetime import datetime, timedelta
from src.scanner import read_csv_to_dataframe, get_last_transactions_day1, get_first_transaction_open_interest_day2, detect_dark_pool_activity

def process_single_pair(file_path_d1: str, file_path_d2: str) -> pd.DataFrame:
    """
    Procesa un par de archivos CSV (Día 1 y Día 2) para detectar actividad de dark pool.
    """
    print(f"\nProcesando par de archivos: {file_path_d1} y {file_path_d2}")
    df_d1 = read_csv_to_dataframe(file_path_d1)
    df_d2 = read_csv_to_dataframe(file_path_d2)

    if df_d1 is None or df_d2 is None:
        print(f"Error al leer uno o ambos archivos. Abortando para este par.")
        return pd.DataFrame()

    processed_d1 = get_last_transactions_day1(df_d1)
    processed_d2 = get_first_transaction_open_interest_day2(df_d2)

    if processed_d1 is None or processed_d2 is None:
        print("Error al procesar los datos de Día 1 o Día 2. Abortando para este par.")
        return pd.DataFrame()

    dark_pool_trades = detect_dark_pool_activity(processed_d1, processed_d2)
    return dark_pool_trades

def find_csv_files_in_directory(directory: str) -> list[str]:
    """
    Encuentra todos los archivos CSV en un directorio, ordenados por nombre.
    Los nombres de archivo se esperan en formato YYYY-MM-DD.csv.
    """
    files = []
    for f_name in os.listdir(directory):
        if f_name.endswith('.csv') and len(f_name) == 14: # YYYY-MM-DD.csv
            try:
                datetime.strptime(f_name.split('.')[0], '%Y-%m-%d')
                files.append(os.path.join(directory, f_name))
            except ValueError:
                print(f"Advertencia: El archivo {f_name} no coincide con el formato de fecha YYYY-MM-DD.csv y será ignorado.")
                continue
    return sorted(files)

def main():
    parser = argparse.ArgumentParser(description="Escáner de actividad de Dark Pool en datos de opciones.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file_d1", help="Ruta al archivo CSV del Día 1 (ej: data/2025-06-12.csv)")
    group.add_argument("--file_d2", help="Ruta al archivo CSV del Día 2 (ej: data/2025-06-13.csv). Usado solo si --file_d1 está presente y --dir no.")
    group.add_argument("--dir", help="Directorio que contiene múltiples archivos CSV nombrados por fecha (YYYY-MM-DD.csv)")

    parser.add_argument("--output", help="Ruta opcional al archivo CSV de salida para los resultados.")

    args = parser.parse_args()

    all_dark_pool_results = []

    if args.dir:
        print(f"Procesando directorio: {args.dir}")
        csv_files = find_csv_files_in_directory(args.dir)
        if len(csv_files) < 1: # Necesitamos al menos un archivo para ser el "Día 1"
            print("No hay archivos CSV en el directorio especificado.")
            return

        # Cache para DataFrames leídos para evitar relecturas múltiples
        df_cache = {}

        for i in range(len(csv_files)):
            file_d1_path = csv_files[i]
            file_d1_date_str = os.path.basename(file_d1_path).split('.')[0]
            print(f"\nProcesando {file_d1_path} como Día de Referencia (D1)...")

            if file_d1_path not in df_cache:
                df_d1_raw = read_csv_to_dataframe(file_d1_path)
                if df_d1_raw is None:
                    print(f"Error al leer {file_d1_path}, saltando.")
                    continue
                df_cache[file_d1_path] = df_d1_raw
            else:
                df_d1_raw = df_cache[file_d1_path]

            processed_d1 = get_last_transactions_day1(df_d1_raw.copy()) # Usar copia para no modificar cache
            if processed_d1 is None or processed_d1.empty:
                print(f"No se pudieron procesar datos de últimas transacciones para {file_d1_path}.")
                continue

            # Iterar sobre cada contrato en processed_d1
            for contract_identifier, d1_data in processed_d1.iterrows():
                found_future_trade = False
                # Buscar en los archivos subsiguientes F_i+1, F_i+2, ...
                for j in range(i + 1, len(csv_files)):
                    file_d_future_path = csv_files[j]
                    file_d_future_date_str = os.path.basename(file_d_future_path).split('.')[0]
                    # print(f"  Buscando {contract_identifier} de {file_d1_date_str} en {file_d_future_path}...")

                    if file_d_future_path not in df_cache:
                        df_d_future_raw = read_csv_to_dataframe(file_d_future_path)
                        if df_d_future_raw is None:
                            # print(f"  Error al leer archivo futuro {file_d_future_path}, saltando este archivo para {contract_identifier}.")
                            continue
                        df_cache[file_d_future_path] = df_d_future_raw
                    else:
                        df_d_future_raw = df_cache[file_d_future_path]

                    # Verificar si el contrato específico existe en este archivo futuro
                    contract_in_future_df = df_d_future_raw[df_d_future_raw['ContractIdentifier'] == contract_identifier]

                    if not contract_in_future_df.empty:
                        # El contrato existe, obtener el OI de su primera transacción en este día futuro
                        # Necesitamos pasar un DataFrame que solo contenga este contrato para get_first_transaction_open_interest_day2
                        # o modificar get_first_transaction_open_interest_day2 para que acepte un contract_id

                        # Re-procesamos solo la parte relevante del df_future para el contrato actual
                        # Esto es un poco ineficiente si hay muchos contratos, pero asegura que la lógica de
                        # get_first_transaction_open_interest_day2 (que espera un df y agrupa) funcione.
                        # Una optimización sería modificar get_first_transaction_open_interest_day2 para tomar un contract_id.

                        # Para simplificar, vamos a obtener el OI de la primera aparición directamente.
                        first_occurrence_in_future = contract_in_future_df.head(1)
                        if 'Open Int' in first_occurrence_in_future.columns:
                            oi_future_val = pd.to_numeric(first_occurrence_in_future['Open Int'].iloc[0], errors='coerce')

                            if pd.notna(oi_future_val):
                                # Crear DataFrames pequeños para detect_dark_pool_activity
                                temp_d1_df = pd.DataFrame([d1_data]) # d1_data ya es una Serie con índice ContractIdentifier
                                temp_d2_df = pd.DataFrame({'OpenInt_D2': [oi_future_val]}, index=pd.Index([contract_identifier], name='ContractIdentifier'))

                                # print(f"    Contrato {contract_identifier} encontrado en {file_d_future_date_str} con OI: {oi_future_val}")
                                # print(f"    Datos D1 para {contract_identifier}: Volume={d1_data['Volume_D1']}, OI={d1_data['OpenInt_D1']}")

                                result_df_contract = detect_dark_pool_activity(temp_d1_df, temp_d2_df)

                                if not result_df_contract.empty:
                                    result_df_contract['FileDate_D1'] = file_d1_date_str
                                    result_df_contract['FileDate_D_Future'] = file_d_future_date_str
                                    all_dark_pool_results.append(result_df_contract)
                                found_future_trade = True
                                break # Pasar al siguiente contrato de file_d1_path
                            # else:
                                # print(f"    OI futuro para {contract_identifier} en {file_d_future_date_str} no es numérico: {first_occurrence_in_future['Open Int'].iloc[0]}")
                        # else:
                            # print(f"    Columna 'Open Int' no encontrada en {file_d_future_path} para {contract_identifier}")
                    # else:
                        # print(f"    Contrato {contract_identifier} NO encontrado en {file_d_future_date_str}")

                # if not found_future_trade:
                    # print(f"  No se encontró actividad futura para {contract_identifier} originado en {file_d1_date_str}.")

    elif args.file_d1 and args.file_d2: # Modo de par de archivos (lógica original)
        print(f"Procesando par de archivos especificado: {args.file_d1} y {args.file_d2}")
        results_df_pair = process_single_pair(args.file_d1, args.file_d2) # process_single_pair usa detect_dark_pool_activity
        if not results_df_pair.empty:
            results_df_pair['FileDate_D1'] = os.path.basename(args.file_d1).split('.')[0]
            # Renombrar FileDate_D2 a FileDate_D_Future para consistencia con el modo directorio
            results_df_pair['FileDate_D_Future'] = os.path.basename(args.file_d2).split('.')[0]
            all_dark_pool_results.append(results_df_pair)
    else:
        # Si --file_d1 se especifica solo, sin --file_d2 y sin --dir.
        # Esto podría ser un caso de uso no deseado o requeriría una aclaración
        # Por ahora, si solo file_d1 está, no hacemos nada o imprimimos ayuda.
        if args.file_d1 and not args.file_d2 and not args.dir:
             print("La opción --file_d1 requiere --file_d2 si no se usa --dir.")
        parser.print_help()
        return

    if not all_dark_pool_results:
        print("\nNo se detectó actividad de dark pool o no se procesaron archivos válidamente.")
        return

    # Concatenar todos los DataFrames de resultados individuales
    final_df = pd.concat(all_dark_pool_results)

    # Asegurarse de que ContractIdentifier, si es índice, se mueva a columna.
    # Esto es crucial porque detect_dark_pool_activity devuelve DF con ContractIdentifier como índice.
    # Y el .iterrows() sobre processed_d1 también usa el índice.
    if final_df.index.name == 'ContractIdentifier' or 'ContractIdentifier' not in final_df.columns:
        final_df = final_df.reset_index()

    # Debug: Imprimir columnas e índice para verificar antes de reordenar
    # print("\nDebug: Columnas de final_df ANTES de reordenar:", final_df.columns)
    # print("Debug: Índice de final_df ANTES de reordenar:", final_df.index)
    # print(final_df.head())

    # Reordenar columnas de una manera más robusta
    cols_order = []

    # Columnas de identificación esperadas
    id_cols = ['FileDate_D1', 'FileDate_D_Future', 'ContractIdentifier']
    for col in id_cols:
        if col in final_df.columns:
            cols_order.append(col)
        # else:
            # print(f"Advertencia de reordenamiento: La columna de ID '{col}' no está en final_df.")

    # Columnas de datos esperadas
    data_cols_expected = ['Volume_D1', 'OpenInt_D1', 'OpenInt_D2', 'DarkPoolActivity']
    for col in data_cols_expected:
        if col in final_df.columns:
            cols_order.append(col)
        # else:
            # print(f"Advertencia de reordenamiento: La columna de datos '{col}' no está en final_df.")

    # Añadir cualquier otra columna que no haya sido explícitamente listada, al final
    for col in final_df.columns:
        if col not in cols_order:
            cols_order.append(col)

    # Aplicar el orden de columnas solo si todas las columnas en cols_order existen en final_df
    # o filtrar cols_order para que solo contenga columnas existentes.
    # Para evitar KeyErrors, es más seguro filtrar cols_order.
    actual_cols_order = [col for col in cols_order if col in final_df.columns]

    try:
        final_df = final_df[actual_cols_order]
    except KeyError as e:
        print(f"\nError al intentar reordenar las columnas. Esto no debería ocurrir si actual_cols_order se filtró correctamente.")
        print(f"Columnas intentadas: {actual_cols_order}")
        print(f"Columnas disponibles: {list(final_df.columns)}")
        print(f"Error original: {e}")
        # Decidir si retornar o continuar con el DataFrame sin el orden preferido
        # Por ahora, continuaremos, pero el orden podría no ser el ideal.
        pass


    print("\n--- Resultados Finales de Actividad de Dark Pool ---")
    print(final_df)

    if args.output:
        try:
            final_df.to_csv(args.output, index=False)
            print(f"\nResultados guardados en: {args.output}")
        except Exception as e:
            print(f"\nError al guardar los resultados en {args.output}: {e}")

if __name__ == "__main__":
    # Para ejecutar desde el directorio raíz del proyecto (darkpool_scanner):
    # python main.py --file_d1 data/2025-06-12.csv --file_d2 data/2025-06-13.csv
    # python main.py --dir data/
    # python main.py --dir data/ --output resultados_darkpool.csv

    # Crear archivos dummy para prueba si no existen
    if not os.path.exists("data"):
        os.makedirs("data")

    # Ejemplo de datos para 2025-06-12.csv
    # AAPL: V=150, OI=1050
    # MSFT: V=50, OI=500
    # GOOG: V=200, OI=300 (No aparecerá en 13, sí en 14)
    # TSLA: V=100, OI=600 (No aparecerá más)
    data_12_content = """Symbol,Symbol,Price~,Type,Strike,Volume,"Open Int",Time
AAPL|20250620|235.00P,AAPL,198.99,Put,235,150,1050,15:14:15 ET
MSFT|20250620|400.00C,MSFT,450.00,Call,400,50,500,15:15:00 ET
GOOG|20250620|150.00C,GOOG,170.00,Call,150,200,300,15:16:00 ET
TSLA|20250620|200.00C,TSLA,210.00,Call,200,100,600,15:17:00 ET
"""
    # Ejemplo de datos para 2025-06-13.csv
    # AAPL: OI=1100
    # MSFT: OI=520 (No hay dark pool aquí si se compara con D12)
    # SPY: Nuevo contrato
    data_13_content = """Symbol,Symbol,Price~,Type,Strike,Volume,"Open Int",Time
AAPL|20250620|235.00P,AAPL,196.39,Put,235,60,1100,09:30:08 ET
MSFT|20250620|400.00C,MSFT,452.00,Call,400,55,520,09:31:00 ET
SPY|20250620|500.00C,SPY,510.00,Call,500,70,1000,09:32:00 ET
"""
    # Ejemplo de datos para 2025-06-14.csv
    # AAPL: OI=1150 (Comparado con D13: (60+1100)-1150 = 10)
    # GOOG: OI=330 (Comparado con D12: (200+300)-330 = 170)
    # MSFT: No está, así que para MSFT de D13, no hay futuro aquí.
    data_14_content = """Symbol,Symbol,Price~,Type,Strike,Volume,"Open Int",Time
AAPL|20250620|235.00P,AAPL,195.00,Put,235,40,1150,09:35:00 ET
GOOG|20250620|150.00C,GOOG,172.00,Call,150,220,330,09:36:00 ET
"""
    # Ejemplo de datos para 2025-06-15.csv
    # MSFT: OI=580 (Comparado con D13: (55+520)-580 = -5, no dark pool)
    #       (Comparado con D12: (50+500)-580 = -30, no dark pool) -> pero D13 es el primer futuro para D12.
    # SPY: OI=1050 (Comparado con D13: (70+1000)-1050=20)
    data_15_content = """Symbol,Symbol,Price~,Type,Strike,Volume,"Open Int",Time
MSFT|20250620|400.00C,MSFT,455.00,Call,400,60,580,09:40:00 ET
SPY|20250620|500.00C,SPY,512.00,Call,500,75,1050,09:41:00 ET
AAPL|20250620|235.00P,AAPL,190.00,Put,235,30,1180,09:42:00 ET
""" # AAPL aquí es para probar que se toma la *primera* aparición futura.

    test_files_data = {
        "data/2025-06-12.csv": data_12_content,
        "data/2025-06-13.csv": data_13_content,
        "data/2025-06-14.csv": data_14_content,
        "data/2025-06-15.csv": data_15_content,
    }

    for file_path, content in test_files_data.items():
        # Asegurarse de que el directorio de datos exista
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        # Crear/sobrescribir archivos de prueba para cada ejecución
        with open(file_path, "w") as f:
            f.write(content)
        print(f"Creado/Actualizado archivo de prueba: {file_path}")

    main()
