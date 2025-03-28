import cProfile
import pstats
import io
import time
import traceback
import psutil
import logging
from typing import Any, Callable, Dict, Optional, List

class AuditProfiler:
    def __init__(self, logger=None):
        """
        Inicializa el profiler con capacidades de logging y análisis
        """
        self.logger = logger or logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        
        # Configuración de handlers para logging
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
        console_handler.setFormatter(formatter)
        
        if not self.logger.handlers:
            self.logger.addHandler(console_handler)

    def profile_function(self, func: Callable, *args: Any, **kwargs: Any) -> Dict:
        """
        Perfil integral de una función (CPU y memoria) con mejoras de diagnóstico
        """
        profiler = cProfile.Profile()
        
        try:
            # Logging del método a probar
            self.logger.info(f"Perfilando método: {func.__name__}")
            
            # Múltiples mediciones de memoria inicial
            process = psutil.Process()
            memory_measurements_before = [
                process.memory_info().rss / (1024 * 1024) for _ in range(3)
            ]
            memory_before = sum(memory_measurements_before) / len(memory_measurements_before)
            
            # Iniciar medición de CPU
            start_time = time.time()
            cpu_percent_start = process.cpu_percent(interval=0.1)
            
            # Ejecutar función con perfil
            profiler.enable()
            result = func(*args, **kwargs)
            profiler.disable()
            
            # Medición de CPU final
            end_time = time.time()
            cpu_percent_end = process.cpu_percent(interval=0.1)
            
            # Múltiples mediciones de memoria final
            memory_measurements_after = [
                process.memory_info().rss / (1024 * 1024) for _ in range(3)
            ]
            memory_after = sum(memory_measurements_after) / len(memory_measurements_after)
            
            # Calcular memoria usada de manera más robusta
            memory_used = max(0, memory_after - memory_before)
            
            # Capturar estadísticas detalladas
            s = io.StringIO()
            ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
            ps.print_stats(30)  # Mostrar las 30 funciones que más tiempo consumen
            
            # Logging detallado
            self.logger.info(f" Resultados de {func.__name__}:")
            self.logger.info(f"Tiempo total: {end_time - start_time:.2f} segundos")
            self.logger.info(f" Uso de CPU: {cpu_percent_end - cpu_percent_start:.2f}%")
            self.logger.info(f" Memoria inicial: {memory_before:.2f} MB")
            self.logger.info(f" Memoria final: {memory_after:.2f} MB")
            self.logger.info(f"Memoria usada: {memory_used:.2f} MB")
            
            return {
                'result': result,
                'total_time': end_time - start_time,
                'cpu_percent': max(0, cpu_percent_end - cpu_percent_start),
                'memory_before_mb': memory_before,
                'memory_after_mb': memory_after,
                'memory_used_mb': memory_used,
                'detailed_stats': s.getvalue(),
                'profiler': profiler,
                'method_name': func.__name__  # Añadir nombre del método
            }
        
        except Exception as e:
            self.logger.error(f" Error en profiling del método {func.__name__}: {e}")
            self.logger.error(traceback.format_exc())
            raise

    def profile_steps(
        self, 
        processor, 
        file_path: str, 
        contract: str, 
        inventory_file: Optional[str] = None
    ) -> Dict:
        """
        Análisis paso a paso del proceso de auditoría con soporte de inventario
        """
        self.logger.info("Iniciando análisis paso a paso")
        
        results = []
        repository = processor.repository
        
        try:
            # 1. Lectura del archivo
            self.logger.info("Perfilando lectura del archivo...")
            read_profile = self.profile_function(processor._read_audit_file, file_path)
            df = read_profile['result']
            results.append({
                'step': 'Lectura del archivo',
                'time': read_profile['total_time'],
                'memory_mb': read_profile['memory_used_mb']
            })
            
            # 2. Obtener requisitos del programa
            self.logger.info(" Perfilando obtención de requisitos del programa...")
            reqs_profile = self.profile_function(repository.get_program_requirements, contract)
            program_reqs = reqs_profile['result']
            results.append({
                'step': 'Obtener requisitos del programa',
                'time': reqs_profile['total_time'],
                'memory_mb': reqs_profile['memory_used_mb']
            })
            
            # 3. Procesamiento de archivo de inventario (si está presente)
            if inventory_file:
                self.logger.info(" Perfilando lectura de archivo de inventario...")
                inventory_read_profile = self.profile_function(
                    processor._read_inventory_file, 
                    inventory_file
                )
                inventory_df = inventory_read_profile['result']
                results.append({
                    'step': 'Lectura de archivo de inventario',
                    'time': inventory_read_profile['total_time'],
                    'memory_mb': inventory_read_profile['memory_used_mb']
                })
            
            # 4. Auditoría de Serial Control
            self.logger.info("🔍 Perfilando auditoría de Serial Control...")
            # Pasar inventory_df si está disponible
            serial_profile = self.profile_function(
                processor._process_serial_control_audit, 
                df, 
                program_reqs, 
                inventory_df if inventory_file else None
            )
            results.append({
                'step': 'Auditoría de Serial Control',
                'time': serial_profile['total_time'],
                'memory_mb': serial_profile['memory_used_mb']
            })
            
            # 4. Auditoría de Organization Mismatch
            self.logger.info(" Perfilando auditoría de Organization Mismatch...")
            org_profile = self.profile_function(processor._process_org_mismatch_audit, df, program_reqs)
            results.append({
                'step': 'Auditoría de Organization Mismatch',
                'time': org_profile['total_time'],
                'memory_mb': org_profile['memory_used_mb']
            })
            
            # 5. Auditoría de otros atributos
            self.logger.info(" Perfilando auditoría de otros atributos...")
            other_profile = self.profile_function(processor._process_other_attributes_audit, df, program_reqs)
            results.append({
                'step': 'Auditoría de otros atributos',
                'time': other_profile['total_time'],
                'memory_mb': other_profile['memory_used_mb']
            })
            
            
            
            # 6. Combinar resultados de auditoría
            self.logger.info(" Perfilando combinación de resultados...")
            combined_df = self.profile_function(
                processor._combine_audit_results,
                serial_profile['result']['data'],
                org_profile['result']['data'],
                other_profile['result']['data']
            )
            results.append({
                'step': 'Combinar resultados',
                'time': combined_df['total_time'],
                'memory_mb': combined_df['memory_used_mb']
            })
            
            # Identificar paso más costoso
            max_time_step = max(results, key=lambda x: x['time'])
            max_memory_step = max(results, key=lambda x: x['memory_mb'])
            
            return {
                'step_results': results,
                'bottleneck': {
                    'time': max_time_step,
                    'memory': max_memory_step
                },
                'total_time': sum(step['time'] for step in results),
                'total_memory_mb': sum(step['memory_mb'] for step in results if step['memory_mb'] > 0)
            }
            
        except Exception as e:
            self.logger.error(f"Error en análisis paso a paso: {e}")
            self.logger.error(traceback.format_exc())
            raise

    def analyze_process_steps(self, processor, file_path: str, contract: str,inventory_file: Optional[str]) -> Dict:
        """
        Análisis por pasos SIN ejecutar el proceso completo
        """
        self.logger.info(" Iniciando análisis detallado por pasos")
        
        try:
            # Ejecutar solo el análisis paso a paso para evitar la doble ejecución
            steps_profile = self.profile_steps(processor, file_path, contract, inventory_file)
            
            # Generar informe consolidado
            report = {
                'step_analysis': steps_profile,
                'full_profile': {
                    'total_time': steps_profile['total_time'],
                    'memory_used_mb': steps_profile['total_memory_mb'],
                    'cpu_percent': 0,  # No podemos calcular esto correctamente sin el proceso completo
                    'memory_before_mb': 0,
                    'memory_after_mb': 0
                }
            }
            
            # Logging de resultados principales
            self.logger.info(f" Tiempo total (suma de pasos): {report['full_profile']['total_time']:.2f} segundos")
            self.logger.info(f" Memoria total (suma de pasos): {report['full_profile']['memory_used_mb']:.2f} MB")
            self.logger.info(f" Paso más lento: {steps_profile['bottleneck']['time']['step']} ({steps_profile['bottleneck']['time']['time']:.2f}s)")
            
            return report
        
        except Exception as e:
            self.logger.error(f"Error en análisis paso a paso: {e}")
            self.logger.error(traceback.format_exc())
            raise

    def comprehensive_audit_profile(
        self, 
        processor, 
        file_path: str, 
        contract: str, 
        inventory_file: Optional[str] = None, 
        only_steps: bool = True
    ) -> Dict:
        """
        Análisis completo y optimizado del proceso de auditoría con soporte opcional de inventario
        """
        self.logger.info(" Iniciando análisis completo de rendimiento")
        
        try:
            # Si se proporciona inventario, modificar la llamada a process_audit
            if inventory_file:
                process_method = lambda: processor.process_audit(file_path, contract, inventory_file=inventory_file)
            else:
                process_method = lambda: processor.process_audit(file_path, contract)

            if only_steps:
                # Ejecutar solo el análisis paso a paso
                return self.analyze_process_steps(processor, file_path, contract, inventory_file)
            else:
                # Ejecutar análisis completo + paso a paso (doble ejecución)
                # 1. Perfil del proceso completo
                self.logger.info(" Perfilando proceso de auditoría completo...")
                full_profile = self.profile_function(process_method)
                
                # 2. Análisis paso a paso
                self.logger.info(" Realizando análisis paso a paso...")
                steps_profile = self.profile_steps(processor, file_path, contract, inventory_file)
                
                # 3. Extraer los 5 cuellos de botella principales - CORREGIDO
                s = io.StringIO()
                if 'profiler' in full_profile:
                    ps = pstats.Stats(full_profile['profiler'], stream=s)
                    ps.sort_stats('cumulative')
                    ps.print_stats(5)
                    top_bottlenecks = s.getvalue()
                else:
                    top_bottlenecks = "No se pudieron extraer los cuellos de botella principales"
                
                # Generar informe consolidado
                report = {
                    'full_profile': {
                        'total_time': full_profile['total_time'],
                        'cpu_percent': full_profile['cpu_percent'],
                        'memory_before_mb': full_profile['memory_before_mb'],
                        'memory_after_mb': full_profile['memory_after_mb'],
                        'memory_used_mb': full_profile['memory_used_mb']
                    },
                    'step_analysis': steps_profile,
                    'detailed_stats': full_profile['detailed_stats'],
                    'top_bottlenecks': top_bottlenecks
                }
                
                # Logging de resultados principales
                self.logger.info(f"Tiempo total: {report['full_profile']['total_time']:.2f} segundos")
                self.logger.info(f" Uso de CPU: {report['full_profile']['cpu_percent']:.1f}%")
                self.logger.info(f" Memoria usada: {report['full_profile']['memory_used_mb']:.2f} MB")
                self.logger.info(f" Paso más lento: {steps_profile['bottleneck']['time']['step']} ({steps_profile['bottleneck']['time']['time']:.2f}s)")
                
                return report
        
        except Exception as e:
            self.logger.error(f"Error en análisis completo: {e}")
            self.logger.error(traceback.format_exc())
            raise

# Uso ejemplo
def run_audit_profiling(processor, file_path, contract):
    profiler = AuditProfiler()
    report = profiler.comprehensive_audit_profile(processor, file_path, contract, only_steps=True)
    
    # Guardar informe detallado
    with open('audit_performance_report.txt', 'w') as f:
        f.write("=== INFORME DETALLADO DE RENDIMIENTO ===\n\n")
        f.write(f"Archivo: {file_path}\n")
        f.write(f"Contrato: {contract}\n\n")
        
        f.write("RESUMEN GENERAL:\n")
        f.write(f"- Tiempo total: {report['full_profile']['total_time']:.2f} segundos\n")
        
        if 'cpu_percent' in report['full_profile']:
            f.write(f"- Uso de CPU: {report['full_profile']['cpu_percent']:.1f}%\n")
            
        if 'memory_before_mb' in report['full_profile'] and report['full_profile']['memory_before_mb'] > 0:
            f.write(f"- Memoria antes: {report['full_profile']['memory_before_mb']:.2f} MB\n")
            f.write(f"- Memoria después: {report['full_profile']['memory_after_mb']:.2f} MB\n")
            
        f.write(f"- Memoria usada: {report['full_profile']['memory_used_mb']:.2f} MB\n\n")
        
        f.write("ANÁLISIS POR PASOS:\n")
        for step in report['step_analysis']['step_results']:
            f.write(f"- {step['step']}: {step['time']:.2f}s, {step['memory_mb']:.2f} MB\n")
        
        f.write(f"\nCUELLO DE BOTELLA PRINCIPAL:\n")
        f.write(f"- Paso más lento: {report['step_analysis']['bottleneck']['time']['step']} ({report['step_analysis']['bottleneck']['time']['time']:.2f}s)\n")
        f.write(f"- Paso con más memoria: {report['step_analysis']['bottleneck']['memory']['step']} ({report['step_analysis']['bottleneck']['memory']['memory_mb']:.2f} MB)\n\n")
        
        if 'detailed_stats' in report:
            f.write("ESTADÍSTICAS DETALLADAS:\n")
            f.write(report['detailed_stats'])

    return report