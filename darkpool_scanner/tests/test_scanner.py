import unittest
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import os
import sys

# Asegurarse de que el directorio src está en el PYTHONPATH para las importaciones
# Esto es comúnmente necesario cuando se ejecutan tests desde el directorio tests
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from scanner import (
    read_csv_to_dataframe,
    get_last_transactions_day1,
    get_first_transaction_open_interest_day2,
    detect_dark_pool_activity
)

class TestScanner(unittest.TestCase):

    def setUp(self):
        """Configuración inicial para las pruebas; crear archivos CSV temporales."""
        self.test_data_dir = "temp_test_data"
        os.makedirs(self.test_data_dir, exist_ok=True)

        self.csv_d1_content = """Symbol,Symbol,Price~,Type,Strike,Volume,"Open Int",Time
ContractA,A,10,C,100,10,100,09:30:00 ET
ContractA,A,11,C,100,15,110,09:35:00 ET
ContractB,B,20,P,200,5,50,09:40:00 ET
ContractC,C,30,C,300,0,"",10:00:00 ET
ContractD,D,40,C,400,INVALID,500,10:05:00 ET
"""
        self.csv_d1_path = os.path.join(self.test_data_dir, "test_d1.csv")
        with open(self.csv_d1_path, "w") as f:
            f.write(self.csv_d1_content)

        self.csv_d2_content = """Symbol,Symbol,Price~,Type,Strike,Volume,"Open Int",Time
ContractA,A,12,C,100,20,120,09:30:00 ET
ContractA,A,13,C,100,25,130,09:35:00 ET
ContractB,B,22,P,200,8,60,09:45:00 ET
ContractE,E,50,P,500,30,300,10:10:00 ET
"""
        self.csv_d2_path = os.path.join(self.test_data_dir, "test_d2.csv")
        with open(self.csv_d2_path, "w") as f:
            f.write(self.csv_d2_content)

        self.csv_empty_path = os.path.join(self.test_data_dir, "empty.csv")
        with open(self.csv_empty_path, "w") as f:
            f.write("Symbol,Symbol,Price~,Volume,\"Open Int\"\n") # Solo cabeceras


    def tearDown(self):
        """Limpiar después de las pruebas; eliminar archivos y directorio temporal."""
        os.remove(self.csv_d1_path)
        os.remove(self.csv_d2_path)
        os.remove(self.csv_empty_path)
        os.rmdir(self.test_data_dir)

    def test_read_csv_to_dataframe(self):
        df = read_csv_to_dataframe(self.csv_d1_path)
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 4)
        self.assertIn("ContractIdentifier", df.columns)
        self.assertIn("Open Int", df.columns) # Después de la limpieza de ""

        # Probar con archivo no existente
        df_non_existent = read_csv_to_dataframe("non_existent.csv")
        self.assertIsNone(df_non_existent)

        # Probar con archivo vacío (solo cabeceras)
        df_empty = read_csv_to_dataframe(self.csv_empty_path)
        self.assertIsNotNone(df_empty)
        self.assertTrue(df_empty.empty)


    def test_get_last_transactions_day1(self):
        df_d1 = read_csv_to_dataframe(self.csv_d1_path)
        self.assertIsNotNone(df_d1)

        # Convertir 'Open Int' a numérico, ya que read_csv puede leerlo como object si hay strings vacíos
        df_d1['Open Int'] = pd.to_numeric(df_d1['Open Int'], errors='coerce')

        processed_d1 = get_last_transactions_day1(df_d1)
        self.assertIsNotNone(processed_d1)
        self.assertEqual(len(processed_d1), 4) # ContractA, ContractB, ContractC, ContractD

        # Verificar ContractA
        contract_a_data = processed_d1.loc["ContractA"]
        self.assertEqual(contract_a_data["Volume_D1"], 15)
        self.assertEqual(contract_a_data["OpenInt_D1"], 110)

        # Verificar ContractC (Open Int vacío -> NaN)
        contract_c_data = processed_d1.loc["ContractC"]
        self.assertEqual(contract_c_data["Volume_D1"], 0)
        self.assertTrue(pd.isna(contract_c_data["OpenInt_D1"]))

        # Verificar ContractD (Volume inválido -> NaN)
        # La función get_last_transactions_day1 no convierte a numérico, eso pasa en detect_dark_pool
        # Así que aquí el Volume_D1 para ContractD será "INVALID"
        contract_d_data = processed_d1.loc["ContractD"]
        self.assertEqual(contract_d_data["Volume_D1"], "INVALID") # Se mantiene como string
        self.assertEqual(contract_d_data["OpenInt_D1"], 500)


    def test_get_first_transaction_open_interest_day2(self):
        df_d2 = read_csv_to_dataframe(self.csv_d2_path)
        self.assertIsNotNone(df_d2)
        processed_d2 = get_first_transaction_open_interest_day2(df_d2)

        self.assertIsNotNone(processed_d2)
        self.assertEqual(len(processed_d2), 3) # ContractA, ContractB, ContractE

        contract_a_d2 = processed_d2.loc["ContractA"]
        self.assertEqual(contract_a_d2["OpenInt_D2"], 120)

        contract_e_d2 = processed_d2.loc["ContractE"]
        self.assertEqual(contract_e_d2["OpenInt_D2"], 300)

    def test_detect_dark_pool_activity(self):
        # Fórmula corregida: OpenInt_D2 - (Volume_D1 + OpenInt_D1)

        # Caso 1: Datos válidos, algunos con actividad de dark pool
        # ContractA: D2_OI=130, D1_V=10, D1_OI=100. SumaD1=110. Actividad = 130 - 110 = 20 (>0)
        # ContractB: D2_OI=60, D1_V=5, D1_OI=50. SumaD1=55. Actividad = 60 - 55 = 5 (>0)
        # ContractC: D2_OI=200, D1_V=20, D1_OI=170. SumaD1=190. Actividad = 200 - 190 = 10 (>0)
        # ContractX (solo en D1): No aparecerá en 'inner' join.
        # ContractY (OpenInt_D1 es NA): Será dropeado por dropna.
        d1_proc_data = {
            'Volume_D1':  [10.0, 5.0, 20.0, 30.0, 0.0],
            'OpenInt_D1': [100.0, 50.0, 170.0, 200.0, pd.NA]
        }
        idx1 = pd.Index(["ContractA", "ContractB", "ContractC", "ContractX", "ContractY"], name="ContractIdentifier")
        df_d1_processed = pd.DataFrame(d1_proc_data, index=idx1)

        # ContractA: D2_OI=130
        # ContractB: D2_OI=60
        # ContractC: D2_OI=200
        # ContractZ (solo en D2): No aparecerá en 'inner' join.
        # ContractY: D2_OI=50 (pero D1 tiene NA, así que se dropeará)
        d2_proc_data = {
            'OpenInt_D2': [130.0, 60.0, 200.0, 300.0, 50.0]
        }
        idx2 = pd.Index(["ContractA", "ContractB", "ContractC", "ContractZ", "ContractY"], name="ContractIdentifier")
        df_d2_processed = pd.DataFrame(d2_proc_data, index=idx2)

        dark_pool_trades = detect_dark_pool_activity(df_d1_processed, df_d2_processed)
        self.assertIsNotNone(dark_pool_trades)

        # Esperamos 3 trades de dark pool (A, B, C)
        self.assertEqual(len(dark_pool_trades), 3)

        self.assertIn("ContractA", dark_pool_trades.index)
        self.assertEqual(dark_pool_trades.loc["ContractA"]["DarkPoolActivity"], 20)

        self.assertIn("ContractB", dark_pool_trades.index)
        self.assertEqual(dark_pool_trades.loc["ContractB"]["DarkPoolActivity"], 5)

        self.assertIn("ContractC", dark_pool_trades.index)
        self.assertEqual(dark_pool_trades.loc["ContractC"]["DarkPoolActivity"], 10)

        self.assertNotIn("ContractX", dark_pool_trades.index)
        self.assertNotIn("ContractY", dark_pool_trades.index)
        self.assertNotIn("ContractZ", dark_pool_trades.index)


        # Caso 2: Sin actividad de dark pool (resultado negativo o cero)
        # ContractP: D2_OI=100, D1_V=10, D1_OI=100. SumaD1=110. Actividad = 100 - 110 = -10 (no >0)
        # ContractQ: D2_OI=50, D1_V=5, D1_OI=45. SumaD1=50. Actividad = 50 - 50 = 0 (no >0)
        d1_proc_data_no_activity = {
            'Volume_D1': [10.0, 5.0],
            'OpenInt_D1': [100.0, 45.0]
        }
        df_d1_no_activity = pd.DataFrame(d1_proc_data_no_activity, index=pd.Index(["ContractP", "ContractQ"], name="ContractIdentifier"))

        d2_proc_data_no_activity = {
            'OpenInt_D2': [100.0, 50.0]
        }
        df_d2_no_activity = pd.DataFrame(d2_proc_data_no_activity, index=pd.Index(["ContractP", "ContractQ"], name="ContractIdentifier"))

        no_dark_pool = detect_dark_pool_activity(df_d1_no_activity, df_d2_no_activity)
        self.assertTrue(no_dark_pool.empty)

        # Caso 3: DataFrames vacíos o None
        self.assertTrue(detect_dark_pool_activity(pd.DataFrame(), df_d2_no_activity).empty)
        self.assertTrue(detect_dark_pool_activity(df_d1_no_activity, pd.DataFrame()).empty)
        self.assertTrue(detect_dark_pool_activity(None, df_d2_no_activity).empty)
        self.assertTrue(detect_dark_pool_activity(df_d1_no_activity, None).empty)

        # Caso 4: Columnas faltantes (debería devolver DF vacío)
        df_d1_missing_col = pd.DataFrame({'OpenInt_D1': [100.0]}, index=pd.Index(["ContractY"], name="ContractIdentifier"))
        df_d2_ok = pd.DataFrame({'OpenInt_D2': [50.0]}, index=pd.Index(["ContractY"], name="ContractIdentifier"))
        self.assertTrue(detect_dark_pool_activity(df_d1_missing_col, df_d2_ok).empty)

        # Caso 5: Datos no numéricos que no se pueden convertir (manejo de 'coerce')
        # Aquí simulamos la salida de get_last_transactions_day1 donde 'Volume' podría ser un string
        # y 'Open Int' un string vacío. detect_dark_pool_activity hace la conversión y dropna.
        df_d1_raw_processed = read_csv_to_dataframe(self.csv_d1_path)
        df_d2_raw_processed = read_csv_to_dataframe(self.csv_d2_path)

        processed_d1_for_detect = get_last_transactions_day1(df_d1_raw_processed)
        processed_d2_for_detect = get_first_transaction_open_interest_day2(df_d2_raw_processed)

        # ContractA: (15 + 110) - 120 = 5
        # ContractB: (5 + 50) - 60 = -5
        # ContractC: (0 + NaN) - ... -> NaN, se dropea
        # ContractD: (INVALID + 500) -> Volume_D1 se vuelve NaN, se dropea
        dark_pool_real_files = detect_dark_pool_activity(processed_d1_for_detect, processed_d2_for_detect)
        self.assertIsNotNone(dark_pool_real_files)
        self.assertEqual(len(dark_pool_real_files), 1)
        self.assertIn("ContractA", dark_pool_real_files.index)
        self.assertEqual(dark_pool_real_files.loc["ContractA"]["DarkPoolActivity"], 5)


if __name__ == '__main__':
    unittest.main()
