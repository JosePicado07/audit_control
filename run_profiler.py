# run_profiler.py - versión modificada
import sys
from pathlib import Path
import logging
import os
import time

# Configurar PYTHONPATH
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))  # Si tienes estructura src/

# Configurar logging básico
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('profile_performance.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Importar componentes necesarios
from application.use_cases.audit_processor import AuditProcessor
from infrastructure.persistence.excel_repository import ExcelRepository

# Importar la clase AuditProfiler
from paste import AuditProfiler  # Ajusta la ruta según sea necesario

def main():
    """Script mejorado para diagnóstico de rendimiento de auditoría"""
    try:
        # Archivo y contrato a analizar
        file_path = r"C:\Users\picadocj\Desktop\wwt_gah_direct_scheduleable MERCK AUDIT.xlsx"
        contract = "MERCK"  # Ajusta según corresponda
        
        print(f"=== INICIANDO ANÁLISIS DE RENDIMIENTO MEJORADO ===")
        print(f"Archivo: {file_path}")
        print(f"Contrato: {contract}")
        print(f"Fecha/Hora: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Verificar existencia del archivo
        if not os.path.exists(file_path):
            print(f"❌ ERROR: El archivo {file_path} no existe")
            return
        
        # Crear componentes necesarios
        repository = ExcelRepository()
        processor = AuditProcessor(repository=repository)
        
        # Crear profiler
        profiler = AuditProfiler()
        
        # Ejecutar análisis paso a paso (para evitar doble ejecución)
        print("\nEjecutando análisis de rendimiento por pasos...")
        start_time = time.time()
        report = profiler.comprehensive_audit_profile(processor, file_path, contract, only_steps=True)
        total_time = time.time() - start_time
        
        # Mostrar resultados detallados
        print("\n=== RESULTADOS DE RENDIMIENTO ===")
        print(f"Tiempo total de análisis: {total_time:.2f} segundos")
        print(f"Tiempo del proceso (suma de pasos): {report['full_profile']['total_time']:.2f} segundos")
        print(f"Memoria usada total: {report['full_profile']['memory_used_mb']:.2f} MB")
        
        print("\n=== ANÁLISIS POR PASOS ===")
        for step in report['step_analysis']['step_results']:
            print(f"- {step['step']}: {step['time']:.2f}s, {step['memory_mb']:.2f} MB")
        
        print(f"\n=== CUELLO DE BOTELLA PRINCIPAL ===")
        time_bottleneck = report['step_analysis']['bottleneck']['time']
        memory_bottleneck = report['step_analysis']['bottleneck']['memory']
        
        print(f"Paso más lento: {time_bottleneck['step']} ({time_bottleneck['time']:.2f}s)")
        print(f"Paso con más memoria: {memory_bottleneck['step']} ({memory_bottleneck['memory_mb']:.2f} MB)")
        
        # Guardar reporte detallado
        report_filename = f'merck_audit_performance_{time.strftime("%Y%m%d_%H%M%S")}.txt'
        with open(report_filename, 'w') as f:
            f.write("=== INFORME DETALLADO DE RENDIMIENTO ===\n\n")
            f.write(f"Archivo: {file_path}\n")
            f.write(f"Contrato: {contract}\n")
            f.write(f"Fecha/Hora: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("RESUMEN GENERAL:\n")
            f.write(f"- Tiempo total (suma de pasos): {report['full_profile']['total_time']:.2f} segundos\n")
            f.write(f"- Memoria usada total: {report['full_profile']['memory_used_mb']:.2f} MB\n\n")
            
            f.write("ANÁLISIS POR PASOS:\n")
            for step in report['step_analysis']['step_results']:
                f.write(f"- {step['step']}: {step['time']:.2f}s, {step['memory_mb']:.2f} MB\n")
            
            f.write(f"\nCUELLO DE BOTELLA PRINCIPAL:\n")
            f.write(f"- Paso más lento: {time_bottleneck['step']} ({time_bottleneck['time']:.2f}s)\n")
            f.write(f"- Paso con más memoria: {memory_bottleneck['step']} ({memory_bottleneck['memory_mb']:.2f} MB)\n\n")
            
            if 'detailed_stats' in report:
                f.write("ESTADÍSTICAS DETALLADAS:\n")
                f.write(report['detailed_stats'])
        
        print(f"\nInforme detallado guardado en: {report_filename}")
        
    except Exception as e:
        logging.error(f"Error en diagnóstico: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        print(f"\n❌ ERROR: {str(e)}")

if __name__ == "__main__":
    main()