import time
import os
import psutil
import traceback
import logging
from pathlib import Path
import gc
import inspect
from datetime import datetime
from typing import Dict, List, Callable, Any, Optional


# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('profiling_details.log', mode='w'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("profiler")

class SimpleProfiler:
    """
    Profiler simplificado que genera un único reporte de texto detallado.
    Se enfoca en identificar métodos específicos que consumen más tiempo y memoria.
    """
    
    def __init__(self, output_file: str = "profiling_report.txt"):
        """Inicializa el profiler con un archivo de salida"""
        self.output_file = output_file
        self.process = psutil.Process(os.getpid())
        
        # Métricas y pasos
        self.steps = []
        self.step_details = {}
        self.current_step_stack = []
        self.start_time = None
        
        # Contador de método
        self.method_counter = {}
        
        # Estado
        self.is_running = False
        
        logger.info(f"Profiler iniciado. Reporte se guardará en: {self.output_file}")
    
    def start(self):
        """Inicia la sesión de profiling"""
        self.is_running = True
        self.start_time = time.time()
        self.memory_start = self._get_memory_mb()
        
        logger.info(f"Iniciando sesión de profiling. Memoria inicial: {self.memory_start:.2f} MB")
    
    def stop(self):
        """Detiene la sesión y genera el reporte"""
        if not self.is_running:
            return
            
        self.is_running = False
        end_time = time.time()
        memory_end = self._get_memory_mb()
        
        total_duration = end_time - self.start_time
        memory_diff = memory_end - self.memory_start
        
        logger.info(f"Finalizando sesión de profiling. Duración total: {total_duration:.2f}s")
        logger.info(f"Memoria final: {memory_end:.2f} MB (Incremento: {memory_diff:.2f} MB)")
        
        # Generar reporte
        self._generate_report(total_duration)
    
    def start_step(self, step_name: str, details: Optional[Dict] = None):
        """Marca el inicio de un paso con detalles opcionales"""
        if not self.is_running:
            self.start()
            
        step_info = {
            'name': step_name,
            'start_time': time.time(),
            'start_memory': self._get_memory_mb(),
            'details': details or {},
            'parent': self.current_step_stack[-1] if self.current_step_stack else None
        }
        
        # Detalles del método que llamó a start_step
        frame = inspect.currentframe().f_back
        if frame:
            caller_info = inspect.getframeinfo(frame)
            step_info['caller_file'] = caller_info.filename
            step_info['caller_line'] = caller_info.lineno
            step_info['caller_function'] = caller_info.function
            
        self.steps.append(step_info)
        step_idx = len(self.steps) - 1
        self.current_step_stack.append(step_idx)
        
        logger.info(f"Paso iniciado: {step_name}")
        return step_idx
    
    def end_step(self, step_name_or_idx: Any):
        """Marca el final de un paso"""
        if not self.current_step_stack:
            logger.warning(f"No hay pasos activos para finalizar: {step_name_or_idx}")
            return
            
        # Determinar índice del paso
        step_idx = None
        if isinstance(step_name_or_idx, int):
            step_idx = step_name_or_idx
        else:
            # Buscar por nombre comenzando desde el último
            for i in range(len(self.steps) - 1, -1, -1):
                if self.steps[i]['name'] == step_name_or_idx:
                    step_idx = i
                    break
                    
        if step_idx is None or step_idx not in self.current_step_stack:
            logger.warning(f"Paso no encontrado o no activo: {step_name_or_idx}")
            return
            
        # Finalizar paso
        end_time = time.time()
        end_memory = self._get_memory_mb()
        
        step = self.steps[step_idx]
        step['end_time'] = end_time
        step['end_memory'] = end_memory
        step['duration'] = end_time - step['start_time']
        step['memory_diff'] = end_memory - step['start_memory']
        
        # Quitar del stack
        if self.current_step_stack and self.current_step_stack[-1] == step_idx:
            self.current_step_stack.pop()
            
        # Forzar un GC siempre 
        collected = gc.collect()
        logger.debug(f"GC después de paso {step['name']}: {collected} objetos")
        
        logger.info(f"Paso finalizado: {step['name']} - Duración: {step['duration']:.4f}s")
    
    def profile_method(self, method, *args, **kwargs):
        """Perfila un método específico con sus argumentos"""
        method_name = self._get_method_name(method)
        details = {
            'method': method_name,
            'args_count': len(args),
            'kwargs_keys': list(kwargs.keys())
        }
        
        # Contar llamadas a este método
        self.method_counter[method_name] = self.method_counter.get(method_name, 0) + 1
        count = self.method_counter[method_name]
        step_name = f"{method_name}_{count}"
        
        # Si es una función de un módulo, obtener más detalles
        if hasattr(method, '__module__'):
            details['module'] = method.__module__
        
        # Iniciar paso
        step_idx = self.start_step(step_name, details)
        
        try:
            # Ejecutar método
            result = method(*args, **kwargs)
            
            # Capturar información sobre el resultado
            self.steps[step_idx]['result_type'] = type(result).__name__
            
            # Para DataFrames, capturar forma
            if hasattr(result, 'shape'):
                self.steps[step_idx]['result_shape'] = str(result.shape)
                
            return result
            
        except Exception as e:
            # Capturar información sobre la excepción
            self.steps[step_idx]['error'] = str(e)
            self.steps[step_idx]['error_type'] = type(e).__name__
            raise
            
        finally:
            # Finalizar paso
            self.end_step(step_idx)
    
    def _get_method_name(self, method) -> str:
        """Obtiene un nombre legible del método"""
        if hasattr(method, '__name__'):
            name = method.__name__
            if hasattr(method, '__self__'):
                # Método de instancia
                cls = method.__self__.__class__.__name__
                return f"{cls}.{name}"
            elif hasattr(method, '__module__') and method.__module__ != '__main__':
                # Función de módulo
                module = method.__module__.split('.')[-1]
                return f"{module}.{name}"
            return name
        return str(method)
    
    def _get_memory_mb(self) -> float:
        """Obtiene el uso actual de memoria en MB"""
        return self.process.memory_info().rss / (1024 * 1024)
    
    def _generate_report(self, total_duration: float):
        """Genera un reporte detallado en formato de texto"""
        try:
            # Verificar pasos incompletos
            active_steps = []
            for i, step in enumerate(self.steps):
                if 'duration' not in step:
                    # Verificar si es un paso activo sin cerrar
                    if i in self.current_step_stack:
                        # Cerrarlo automáticamente para el reporte
                        logger.warning(f"Paso incompleto detectado: {step['name']} - cerrando automáticamente")
                        end_time = time.time()
                        end_memory = self._get_memory_mb()
                        
                        step['end_time'] = end_time
                        step['end_memory'] = end_memory
                        step['duration'] = end_time - step['start_time']
                        step['memory_diff'] = end_memory - step['start_memory']
                        active_steps.append(step['name'])
            
            if active_steps:
                logger.warning(f"Se cerraron automáticamente {len(active_steps)} pasos incompletos: {active_steps}")
            
            # Ordenar pasos por duración
            sorted_steps = sorted(
                [(i, step) for i, step in enumerate(self.steps) if 'duration' in step],
                key=lambda x: x[1]['duration'],
                reverse=True
            )
            
            # Calcular estadísticas por método
            method_stats = {}
            for _, step in sorted_steps:
                if 'details' in step and 'method' in step['details']:
                    method = step['details']['method']
                    if method not in method_stats:
                        method_stats[method] = {
                            'count': 0,
                            'total_duration': 0,
                            'max_duration': 0,
                            'total_memory': 0,
                            'max_memory': 0
                        }
                    
                    stats = method_stats[method]
                    stats['count'] += 1
                    stats['total_duration'] += step['duration']
                    stats['max_duration'] = max(stats['max_duration'], step['duration'])
                    
                    if 'memory_diff' in step:
                        stats['total_memory'] += step['memory_diff']
                        stats['max_memory'] = max(stats['max_memory'], step['memory_diff'])
            
            # Contenido del reporte
            report = []
            report.append("=" * 80)
            report.append(f"INFORME DETALLADO DE RENDIMIENTO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report.append("=" * 80)
            report.append(f"Duración total: {total_duration:.4f} segundos")
            report.append(f"Memoria inicial: {self.memory_start:.2f} MB")
            report.append(f"Memoria final: {self._get_memory_mb():.2f} MB")
            report.append(f"Incremento total de memoria: {self._get_memory_mb() - self.memory_start:.2f} MB")
            report.append(f"Total de pasos analizados: {len(sorted_steps)}")
            report.append("")
            
            # MODIFICACIÓN: Mostrar TODOS los pasos, no solo los 10 más lentos
            report.append("ANÁLISIS COMPLETO DE PASOS (ORDENADOS POR DURACIÓN)")
            report.append("-" * 80)
            report.append(f"{'#':<3} {'Paso':<30} {'Duración (s)':<12} {'% Total':<8} {'Memoria (MB)':<12}")
            report.append("-" * 80)
            
            for i, (_, step) in enumerate(sorted_steps, 1):
                name = step['name']
                duration = step['duration']
                percent = (duration / total_duration) * 100
                memory = step.get('memory_diff', 0)
                
                report.append(f"{i:<3} {name:<30} {duration:<12.4f} {percent:<8.2f} {memory:<12.2f}")
                
                # Incluir detalles si están disponibles
                if 'details' in step and step['details']:
                    for key, value in step['details'].items():
                        report.append(f"    - {key}: {value}")
                
                # Incluir información del llamador
                if 'caller_function' in step:
                    report.append(f"    - Llamado desde: {step.get('caller_function')} ({step.get('caller_file')}:{step.get('caller_line')})")
                
                # Incluir información del resultado
                if 'result_type' in step:
                    result_info = f"    - Resultado: {step['result_type']}"
                    if 'result_shape' in step:
                        result_info += f" con forma {step['result_shape']}"
                    report.append(result_info)
                
                # Separador entre pasos
                report.append("")
            
            # Análisis por método
            report.append("\nANÁLISIS POR MÉTODO")
            report.append("-" * 80)
            report.append(f"{'Método':<40} {'Llamadas':<8} {'Tiempo Total (s)':<16} {'% Total':<8} {'Memoria (MB)':<12}")
            report.append("-" * 80)
            
            # Ordenar métodos por tiempo total
            sorted_methods = sorted(
                method_stats.items(),
                key=lambda x: x[1]['total_duration'],
                reverse=True
            )
            
            for method, stats in sorted_methods:
                count = stats['count']
                total_time = stats['total_duration']
                percent = (total_time / total_duration) * 100
                total_memory = stats['total_memory']
                
                report.append(f"{method:<40} {count:<8} {total_time:<16.4f} {percent:<8.2f} {total_memory:<12.2f}")
            
            # Análisis de memoria
            memory_intensive_steps = sorted(
                [(i, step) for i, step in enumerate(self.steps) if 'memory_diff' in step],
                key=lambda x: x[1]['memory_diff'],
                reverse=True
            )
            
            if memory_intensive_steps:
                report.append("\nPASOS CON MAYOR CONSUMO DE MEMORIA")
                report.append("-" * 80)
                report.append(f"{'#':<3} {'Paso':<30} {'Memoria (MB)':<12} {'Duración (s)':<12}")
                report.append("-" * 80)
                
                for i, (_, step) in enumerate(memory_intensive_steps[:5], 1):
                    name = step['name']
                    memory = step['memory_diff']
                    duration = step['duration']
                    
                    report.append(f"{i:<3} {name:<30} {memory:<12.2f} {duration:<12.4f}")
                    
                    # Incluir detalles relevantes
                    if 'details' in step and 'method' in step['details']:
                        report.append(f"    - Método: {step['details']['method']}")
            
            # Recomendaciones basadas en el análisis
            report.append("\nRECOMENDACIONES DE OPTIMIZACIÓN")
            report.append("-" * 80)
            
            # Identificar métodos que toman más del 10% del tiempo total
            critical_methods = [
                method for method, stats in sorted_methods
                if (stats['total_duration'] / total_duration) * 100 > 10
            ]
            
            if critical_methods:
                report.append("Métodos críticos que consumen más del 10% del tiempo total:")
                for method in critical_methods:
                    stats = method_stats[method]
                    avg_time = stats['total_duration'] / stats['count']
                    report.append(f"  - {method}: {stats['total_duration']:.2f}s total, {avg_time:.2f}s promedio por llamada")
                    report.append(f"    Recomendación: Optimizar implementación o reducir número de llamadas")
            
            # Identificar métodos llamados muchas veces
            frequent_methods = [
                method for method, stats in sorted_methods
                if stats['count'] > 10 and stats['total_duration'] > 1
            ]
            
            if frequent_methods:
                report.append("\nMétodos llamados frecuentemente (>10 veces) con tiempo significativo:")
                for method in frequent_methods:
                    stats = method_stats[method]
                    report.append(f"  - {method}: {stats['count']} llamadas, {stats['total_duration']:.2f}s total")
                    report.append(f"    Recomendación: Considerar caché o consolidar llamadas")
            
            # Identificar problemas de memoria
            memory_issues = [
                step for _, step in memory_intensive_steps[:3]
                if step['memory_diff'] > 100  # Más de 100MB
            ]
            
            if memory_issues:
                report.append("\nProblemas potenciales de memoria:")
                for step in memory_issues:
                    report.append(f"  - {step['name']}: {step['memory_diff']:.2f} MB")
                    if 'details' in step and 'method' in step['details']:
                        report.append(f"    Método: {step['details']['method']}")
                    report.append(f"    Recomendación: Implementar procesamiento por chunks o reducir duplicación de datos")
            
            # Guardar reporte
            with open(self.output_file, 'w') as f:
                f.write('\n'.join(report))
                
            logger.info(f"Reporte generado en: {self.output_file}")
            
            # Imprimir un resumen en la consola
            print("\n" + "=" * 50)
            print("RESUMEN DE RENDIMIENTO")
            print("=" * 50)
            print(f"Tiempo total: {total_duration:.2f} segundos")
            print(f"Incremento de memoria: {self._get_memory_mb() - self.memory_start:.2f} MB")
            
            if critical_methods:
                print("\nMétodos críticos (>10% del tiempo total):")
                for method in critical_methods[:3]:
                    stats = method_stats[method]
                    print(f"  - {method}: {stats['total_duration']:.2f}s ({(stats['total_duration']/total_duration)*100:.1f}%)")
            
            print(f"\nReporte completo en: {self.output_file}")
            print("=" * 50)
            
        except Exception as e:
            logger.error(f"Error generando reporte: {str(e)}")
            logger.error(traceback.format_exc())

# Función auxiliar para usar fácilmente con el sistema de auditoría
def profile_execution(func, *args, **kwargs):
    """
    Perfila la ejecución de una función y genera un reporte detallado.
    
    Args:
        func: Función a perfilar
        *args: Argumentos para la función
        **kwargs: Argumentos con nombre para la función
        
    Returns:
        Resultado de la función
    """
    # Crear profiler
    profiler = SimpleProfiler()
    
    # Iniciar sesión
    profiler.start()
    
    try:
        # Ejecutar función con profiling
        result = profiler.profile_method(func, *args, **kwargs)
        return result
    finally:
        # Finalizar y generar reporte
        profiler.stop()

# Ejemplo de uso para optimización de archivos Excel
def analyze_excel_performance(excel_repository, file_path: Path):
    """
    Analiza específicamente el rendimiento de la lectura de archivos Excel.
    """
    profiler = SimpleProfiler("excel_performance.txt")
    profiler.start()
    
    try:
        # Medir tiempo de lectura estándar
        profiler.start_step("read_excel_standard")
        df_standard = profiler.profile_method(
            excel_repository.read_excel_file,
            file_path
        )
        profiler.end_step("read_excel_standard")
        
        print(f"Lectura estándar completada. Shape: {df_standard.shape}")
        
        # Medir tiempo del método _normalize_columns
        profiler.start_step("normalize_columns")
        normalized_df = profiler.profile_method(
            excel_repository._normalize_columns,
            df_standard.copy()
        )
        profiler.end_step("normalize_columns")
        
        # Medir efecto de la normalización de tipos
        profiler.start_step("optimize_datatypes")
        # Aquí usaríamos una implementación de optimización de tipos
        # Por ejemplo: excel_repository._optimize_datatypes(df_standard.copy())
        profiler.end_step("optimize_datatypes")
        
        return {
            "original_shape": df_standard.shape,
            "normalized_shape": normalized_df.shape,
            "memory_usage_original_mb": df_standard.memory_usage(deep=True).sum() / (1024 * 1024),
            "profiler_report": profiler.output_file
        }
        
    finally:
        profiler.stop()