import sys
from pathlib import Path
import logging
import time
from datetime import datetime

# Configurar PYTHONPATH
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

# Configuración de logging
logging.basicConfig(
   level=logging.INFO,
   format='%(asctime)s - %(levelname)s: %(message)s',
   handlers=[
       logging.FileHandler('audit_profiling.log'),
       logging.StreamHandler(sys.stdout)
   ]
)
logger = logging.getLogger(__name__)

# Importaciones de componentes
from application.use_cases.audit_processor import AuditProcessor
from infrastructure.persistence.excel_repository import ExcelRepository
from paste import AuditProfiler

def run_performance_audit(file_path, contract, inventory_file=None):
   """
   Ejecuta auditoría de rendimiento con y sin inventario
   
   Args:
       file_path (str): Ruta del archivo de auditoría
       contract (str): Contrato a procesar
       inventory_file (str, opcional): Ruta del archivo de inventario
   
   Returns:
       dict: Resultados de rendimiento
   """
   try:
       # Crear componentes necesarios
       repository = ExcelRepository()
       processor = AuditProcessor(repository=repository)
       profiler = AuditProfiler()

       # Resultados de ejecución sin inventario
       logger.info("Procesando sin inventario...")
       no_inventory_profile = profiler.comprehensive_audit_profile(
           processor, 
           file_path, 
           contract, 
           only_steps=True
       )

       # Resultados de ejecución con inventario
       inventory_profile = None
       if inventory_file:
           logger.info(" Procesando con inventario...")
           inventory_profile = profiler.comprehensive_audit_profile(
               processor, 
               file_path, 
               contract, 
               inventory_file=inventory_file,
               only_steps=True
           )

       return {
           'no_inventory_profile': no_inventory_profile,
           'inventory_profile': inventory_profile
       }

   except Exception as e:
       logger.error(f"Error en procesamiento de auditoría: {e}")
       raise

def generate_performance_report(results, file_path, contract, inventory_file=None):
    """
    Generates a detailed performance report with Unicode support
    
    Args:
        results (dict): Performance results
        file_path (str): Path to the audit file
        contract (str): Processed contract
        inventory_file (str, optional): Path to inventory file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f'audit_performance_report_{timestamp}.txt'

    try:
        # Use UTF-8 encoding explicitly to support Unicode characters
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write("=== DETAILED PERFORMANCE REPORT ===\n\n")
            f.write(f"Audit File: {file_path}\n")
            f.write(f"Contract: {contract}\n")
            f.write(f"Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # No Inventory Processing Report
            f.write("🔹 PROCESSING WITHOUT INVENTORY\n")
            no_inv_profile = results['no_inventory_profile']
            f.write(f"- Total Time: {no_inv_profile['full_profile']['total_time']:.2f} seconds\n")
            f.write(f"- Memory Used: {no_inv_profile['full_profile']['memory_used_mb']:.2f} MB\n\n")

            f.write("Step-by-Step Analysis (Without Inventory):\n")
            for step in no_inv_profile['step_analysis']['step_results']:
                f.write(f"- {step['step']}: {step['time']:.2f}s, {step['memory_mb']:.2f} MB\n")

            f.write(f"\nBottleneck (Without Inventory):\n")
            f.write(f"- Slowest Step: {no_inv_profile['step_analysis']['bottleneck']['time']['step']} ")
            f.write(f"({no_inv_profile['step_analysis']['bottleneck']['time']['time']:.2f}s)\n")
            f.write(f"- Highest Memory Step: {no_inv_profile['step_analysis']['bottleneck']['memory']['step']} ")
            f.write(f"({no_inv_profile['step_analysis']['bottleneck']['memory']['memory_mb']:.2f} MB)\n")

            # Inventory Processing Report (if applicable)
            if results.get('inventory_profile'):
                f.write("\n🔹 PROCESSING WITH INVENTORY\n")
                inv_profile = results['inventory_profile']
                f.write(f"- Total Time: {inv_profile['full_profile']['total_time']:.2f} seconds\n")
                f.write(f"- Memory Used: {inv_profile['full_profile']['memory_used_mb']:.2f} MB\n\n")

                f.write("Step-by-Step Analysis (With Inventory):\n")
                for step in inv_profile['step_analysis']['step_results']:
                    f.write(f"- {step['step']}: {step['time']:.2f}s, {step['memory_mb']:.2f} MB\n")

                f.write(f"\nBottleneck (With Inventory):\n")
                f.write(f"- Slowest Step: {inv_profile['step_analysis']['bottleneck']['time']['step']} ")
                f.write(f"({inv_profile['step_analysis']['bottleneck']['time']['time']:.2f}s)\n")
                f.write(f"- Highest Memory Step: {inv_profile['step_analysis']['bottleneck']['memory']['step']} ")
                f.write(f"({inv_profile['step_analysis']['bottleneck']['memory']['memory_mb']:.2f} MB)\n")

                # Performance Comparison
                f.write("\n🔹 PERFORMANCE COMPARISON\n")
                f.write("Time Difference:\n")
                tiempo_dif = abs(no_inv_profile['full_profile']['total_time'] - 
                                 inv_profile['full_profile']['total_time'])
                f.write(f"- Temporal Variation: {tiempo_dif:.2f} seconds\n")

                f.write("Memory Difference:\n")
                memoria_dif = abs(no_inv_profile['full_profile']['memory_used_mb'] - 
                                  inv_profile['full_profile']['memory_used_mb'])
                f.write(f"- Memory Variation: {memoria_dif:.2f} MB\n")

        # Use system logger to log success
        logging.getLogger(__name__).info(f"Performance report generated: {report_filename}")
        return report_filename

    except Exception as e:
        # Log the error with full traceback
        logging.getLogger(__name__).error(f"Error generating performance report: {e}", exc_info=True)
        raise

def main():
   try:
       # Configuración de paths 
       audit_file = r"C:\Users\picadocj\Desktop\Trial files\wwt_gah_direct_scheduleable - 2025-03-05T114029.215.xlsx"
       inventory_file = r"C:\Users\picadocj\Desktop\Trial files\wwt_gah_direct_scheduleable - 2025-02-28T150605.792 1.xlsx"
       contract = "VES"

       logger.info("Iniciando análisis de rendimiento de auditoría")

       # Ejecutar auditoría y generar reporte
       start_time = time.time()
       results = run_performance_audit(audit_file, contract, inventory_file)
       generate_performance_report(results, audit_file, contract, inventory_file)
       
       total_execution_time = time.time() - start_time
       logger.info(f" Análisis completado en {total_execution_time:.2f} segundos")

   except Exception as e:
       logger.error(f"Error crítico en el análisis: {e}")
       sys.exit(1)

if __name__ == "__main__":
   main()