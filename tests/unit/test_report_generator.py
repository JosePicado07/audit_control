import unittest
import pandas as pd
from pathlib import Path
import tempfile
import os
import re
from src.application.use_cases.report_generator import ReportGenerator

class TestReportGenerator(unittest.TestCase):
    """Pruebas unitarias para ReportGenerator"""
    
    def setUp(self):
        """Configuración para las pruebas"""
        # Crear un directorio temporal para los reportes
        self.temp_dir = tempfile.TemporaryDirectory()
        # Inicializar el generador de reportes con el directorio temporal
        self.report_generator = ReportGenerator(output_dir=self.temp_dir.name)
        
    def tearDown(self):
        """Limpieza después de las pruebas"""
        self.temp_dir.cleanup()
        
    def test_normalize_serial_value(self):
        """Prueba la normalización de valores de Serial Control"""
        test_cases = [
            # Formatos para 'Dynamic entry at inventory receipt'
            ("YES", "Dynamic entry at inventory receipt"),
            ("yes", "Dynamic entry at inventory receipt"),
            ("Y", "Dynamic entry at inventory receipt"),
            ("DYNAMIC ENTRY AT INVENTORY RECEIPT", "Dynamic entry at inventory receipt"),
            
            # Formatos para 'No serial number control'
            ("NO", "No serial number control"),
            ("no", "No serial number control"),
            ("N", "No serial number control"),
            ("NO SERIAL NUMBER CONTROL", "No serial number control"),
            
            # Otros valores deben mantenerse igual
            ("OTRO VALOR", "OTRO VALOR"),
            ("12345", "12345")
        ]
        
        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val):
                result = self.report_generator._normalize_serial_value(input_val)
                self.assertEqual(result, expected)
                
    def test_generate_serial_validation_data_excludes_org_01(self):
        """
        Prueba que el algoritmo de detección de discrepancias en Serial Control 
        excluye correctamente la organización "01"
        """
        # Datos de prueba con discrepancias conocidas
        test_data = [
            # Parte con valores diferentes pero iguales entre orgs excepto en "01"
            {"Part Number": "TEST001", "Organization": "01", "Serial Control": "YES", 
             "Manufacturer": "Test Corp", "Description": "Test Part", "Vertex": ""},
            {"Part Number": "TEST001", "Organization": "02", "Serial Control": "NO", 
             "Manufacturer": "Test Corp", "Description": "Test Part", "Vertex": ""},
            {"Part Number": "TEST001", "Organization": "03", "Serial Control": "NO", 
             "Manufacturer": "Test Corp", "Description": "Test Part", "Vertex": ""},
            
            # Parte con valores iguales en todas las orgs
            {"Part Number": "TEST002", "Organization": "01", "Serial Control": "YES", 
             "Manufacturer": "Test Corp", "Description": "Test Part 2", "Vertex": ""},
            {"Part Number": "TEST002", "Organization": "02", "Serial Control": "YES", 
             "Manufacturer": "Test Corp", "Description": "Test Part 2", "Vertex": ""},
            {"Part Number": "TEST002", "Organization": "03", "Serial Control": "YES", 
             "Manufacturer": "Test Corp", "Description": "Test Part 2", "Vertex": ""},
            
            # Parte con valores diferentes entre orgs (no 01)
            {"Part Number": "TEST003", "Organization": "01", "Serial Control": "YES", 
             "Manufacturer": "Test Corp", "Description": "Test Part 3", "Vertex": ""},
            {"Part Number": "TEST003", "Organization": "02", "Serial Control": "NO", 
             "Manufacturer": "Test Corp", "Description": "Test Part 3", "Vertex": ""},
            {"Part Number": "TEST003", "Organization": "03", "Serial Control": "YES", 
             "Manufacturer": "Test Corp", "Description": "Test Part 3", "Vertex": ""}
        ]
        
        # Crear datos de prueba
        df_data = pd.DataFrame(test_data)
        
        # Configurar parámetros
        physical_orgs = ["01", "02", "03"]
        serial_results = {
            "data": df_data,
            "mismatched_parts": []  # Sin partes con mismatch previo
        }
        program_requirements = {}
        
        # Ejecutar el método a probar
        result_df = self.report_generator._generate_serial_validation_data(
            serial_results, physical_orgs, program_requirements
        )
        
        # Verificar resultados - TEST001 no debe ser mismatch porque solo difiere en org 01
        self.assertEqual(
            result_df.loc[result_df['Part Number'] == 'TEST001', 'Serial Control match?'].iloc[0], 
            'Match',
            "TEST001 debe ser 'Match' porque solo difiere en org 01"
        )
        
        # TEST002 debe ser Match porque todas las orgs tienen el mismo valor
        self.assertEqual(
            result_df.loc[result_df['Part Number'] == 'TEST002', 'Serial Control match?'].iloc[0], 
            'Match',
            "TEST002 debe ser 'Match' porque todas las orgs tienen el mismo valor"
        )
        
        # TEST003 debe ser Mismatch porque las orgs 02 y 03 tienen valores diferentes
        self.assertEqual(
            result_df.loc[result_df['Part Number'] == 'TEST003', 'Serial Control match?'].iloc[0], 
            'Mismatch',
            "TEST003 debe ser 'Mismatch' porque las orgs 02 y 03 tienen valores diferentes"
        )
        
    def test_format_validation_dataframe_uses_intersection(self):
        """
        Prueba que el método _format_validation_dataframe usa correctamente 
        la intersección de organizaciones
        """
        # Crear un DataFrame simple con columnas de organizaciones
        test_df = pd.DataFrame({
            'Part Number': ['TEST001', 'TEST002'],
            'Manufacturer': ['Mfg1', 'Mfg2'],
            'Description': ['Desc1', 'Desc2'],
            'Vertex': ['', ''],
            'Serial Control match?': ['Match', 'Mismatch'],
            'org 01 Serial Control': ['YES', 'NO'],
            'org 02 Serial Control': ['YES', 'YES'],
            'org 03 Serial Control': ['YES', 'NO'],
            'org 05 Serial Control': ['NO', 'NO']
        })
        
        # Lista de organizaciones físicas predefinidas (no incluye 05)
        physical_orgs = ['01', '02', '03', '04']
        
        # Ejecutar el método a probar
        result_df = self.report_generator._format_validation_dataframe(
            test_df, physical_orgs, []
        )
        
        # Verificar que solo las organizaciones en la intersección aparecen en el resultado
        expected_cols = set(['org 01 Serial Control', 'org 02 Serial Control', 'org 03 Serial Control'])
        org_cols = set(col for col in result_df.columns if col.startswith('org ') and 'Serial Control' in col)
        
        self.assertEqual(
            org_cols, 
            expected_cols,
            f"Columnas esperadas: {expected_cols}, obtenidas: {org_cols}"
        )
        
        # Verificar que 04 y 05 no están incluidas
        self.assertNotIn('org 04 Serial Control', result_df.columns, 
                        "org 04 no debería estar incluida porque no está en los datos")
        self.assertNotIn('org 05 Serial Control', result_df.columns, 
                        "org 05 no debería estar incluida porque no está en physical_orgs")

if __name__ == '__main__':
    unittest.main()