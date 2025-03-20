import os
import sys
from pathlib import Path
import time
import gc

# Configurar path para encontrar módulos
current_dir = Path(__file__).parent.absolute()
src_path = current_dir / 'src'
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(src_path))

# Importar el profiler simplificado
from profiler_audit import SimpleProfiler, profile_execution

# Importar componentes del sistema
from src.presentation.controllers.audit_controller import AuditController
from src.application.services.audit_service import AuditService
from src.infrastructure.persistence.excel_repository import ExcelRepository
from src.application.use_cases.audit_processor import AuditProcessor
from src.application.use_cases.report_generator import ReportGenerator

def main():
    """Análisis de rendimiento enfocado en cuellos de botella"""
    print("=" * 80)
    print("ANÁLISIS DE RENDIMIENTO DEL SISTEMA DE AUDITORÍA")
    print("=" * 80)
    
    # Configuración - Ajustar a tu entorno
    contract = "MERCK"
    file_path = r"C:\Users\picadocj\Desktop\wwt_gah_direct_scheduleable MERCK AUDIT.xlsx"
    inventory_file = None  # Completar si es necesario
    
    print(f"Archivo: {file_path}")
    print(f"Contrato: {contract}")
    
    # Validar archivo
    if not os.path.exists(file_path):
        print(f"ERROR: Archivo no encontrado: {file_path}")
        file_path = input("Por favor, introduce la ruta completa al archivo: ")
        if not os.path.exists(file_path):
            print(f"ERROR: Archivo no encontrado. Terminando.")
            return
    
    # Crear profiler maestro para todo el proceso
    main_profiler = SimpleProfiler("analisis_completo.txt")
    main_profiler.start()
    
    try:
        # Análisis 1: Optimización de lectura de Excel
        print("\n[1/4] Analizando rendimiento de lectura de Excel...")
        step_excel = main_profiler.start_step("analisis_excel")
        
        try:
            # Inicializar repositorio
            excel_repository = ExcelRepository()
            
            # Probar lecturas independientes
            def read_excel_test():
                """Prueba de lectura específica"""
                start = time.time()
                df = excel_repository.read_excel_file(Path(file_path))
                duration = time.time() - start
                print(f"  - Lectura completada en {duration:.2f}s - Shape: {df.shape}")
                return df
            
            # Ejecutar test de lectura con profiling específico
            df = main_profiler.profile_method(read_excel_test)
        finally:
            main_profiler.end_step(step_excel)
        
        # Análisis 2: Memoria usada por el DataFrame
        print("\n[2/4] Analizando estructura del DataFrame...")
        step_df = main_profiler.start_step("analisis_dataframe")
        
        try:
            # Medir memoria del DataFrame
            df_size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
            print(f"  - Tamaño del DataFrame: {df_size_mb:.2f} MB")
            
            # Analizar columnas y tipos
            print("  - Análisis de columnas:")
            for col in df.columns:
                col_size_mb = df[col].memory_usage(deep=True) / (1024 * 1024)
                print(f"    * {col} ({df[col].dtype}): {col_size_mb:.2f} MB")
        finally:
            main_profiler.end_step(step_df)
        
        # Liberar memoria del análisis previo
        del df
        gc.collect()
        
        # Análisis 3: Inicialización de componentes
        print("\n[3/4] Analizando inicialización de componentes...")
        step_init = main_profiler.start_step("inicializacion_componentes")
        
        try:
            # Inicializar todos los componentes con medición
            audit_processor = main_profiler.profile_method(
                AuditProcessor,
                repository=excel_repository
            )
            
            report_generator = main_profiler.profile_method(
                ReportGenerator
            )
            
            audit_service = main_profiler.profile_method(
                AuditService,
                audit_processor=audit_processor,
                report_generator=report_generator,
                excel_repository=excel_repository
            )
            
            controller = main_profiler.profile_method(
                AuditController,
                audit_service
            )
        finally:
            main_profiler.end_step(step_init)
        
        # Análisis 4: Ejecución completa
        print("\n[4/4] Ejecutando auditoría completa con profiling...")
        step_audit = main_profiler.start_step("auditoria_completa")
        
        try:
            # Ejecutar auditoría
            result = main_profiler.profile_method(
                controller.execute_audit,
                contract,
                file_path,
                inventory_file
            )
            
            # Validar resultado
            if result["status"] == "success":
                print("\nAuditoría completada exitosamente:")
                print(f"  - Reporte externo: {result['data']['external_report_path']}")
                print(f"  - Reporte interno: {result['data']['internal_report_path']}")
            else:
                print(f"\nError en auditoría: {result.get('message', 'Error desconocido')}")
        finally:
            main_profiler.end_step(step_audit)
            
    except Exception as e:
        import traceback
        print(f"\nError durante el análisis: {str(e)}")
        traceback.print_exc()
        
    finally:
        # Finalizar profiling y generar reporte
        main_profiler.stop()
        
        print("\n" + "=" * 80)
        print(f"Análisis completo finalizado. Reporte disponible en: {main_profiler.output_file}")
        print("=" * 80)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"Error crítico: {str(e)}")
        traceback.print_exc()
    finally:
        input("\nPresione Enter para salir...")