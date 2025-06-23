# ================================================================
#  SIMULADOR DE OPERACIÓN - TEC Base de Datos I
#  Control de Asistencia y Planilla Obrera
#  Estudiantes: Oscar Arturo Acuña Durán (2022049304)
#               Alejandro Umaña Miranda  (2024130345)
# ================================================================

import pyodbc
import xml.etree.ElementTree as ET
import socket
import sys
from datetime import datetime, timedelta

# Conexción con la base de datos hosteada en CloudClusters
SERVER = "mssql-196019-0.cloudclusters.net,10245"
DATABASE = "BASESPROYECTO"
USERNAME = "requeSoftware"
PASSWORD = "#Jaime123"
DRIVER = "ODBC Driver 17 for SQL Server"

def local_ip() -> str:
    """
    Se obtiene la IP desde la que se hacen las consultas para poder llevar un registro en la base de datos
    Retorna 127.0.0.1 en caso de error
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"

def connect():
    """
    Se toman los credenciales y se hace una conexión con la DB de MSSQL 
    """
    connection_string = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(connection_string, autocommit=False)

def call_sp(cur, name, params):
    """
    Ejecuta un procedimiento almacenado con manejo de errores
    La idea es modularizar la llamada a procedimientos almacenados
    """
    placeholders = ','.join(['?'] * len(params))
    sql = f"DECLARE @rc INT; EXEC dbo.{name} {placeholders}, @rc OUTPUT; SELECT @rc"
    
    try:
        cur.execute(sql, params)
        rc = cur.fetchone()[0]
        if rc != 0:
            raise RuntimeError(f"SP {name} devolvió código {rc}")
        return rc
    except Exception as e:
        print(f"Error en SP {name}: {e}")
        raise

def call_sp_with_output(cur, name, params, tolerated=(0, 50015)):
    """
    Ejecuta SP que retorna un valor y un código de resultado.
    """
    ph = ','.join('?' * len(params))
    sql = (f"DECLARE @out INT, @rc INT; EXEC dbo.{name} {ph}, @out OUTPUT, @rc OUTPUT;"
           " SELECT @out, @rc")
    cur.execute(sql, params)
    out, rc = cur.fetchone()

    if rc not in tolerated:
        raise RuntimeError(f"SP {name} devolvió código {rc}")
    return out, rc


def main(xml_file="operacion.xml"):
    print("=" * 70)
    print("   SIMULADOR DE OPERACIÓN - TEC BASE DE DATOS I")
    print("   Control de Asistencia y Planilla Obrera")
    print("=" * 70)
    
    # Cargar y parsear el archivo XML de operaciones
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        print(f"Archivo XML cargado exitosamente: {xml_file}")
    except Exception as e:
        print(f"Error cargando archivo XML: {e}")
        return 1
    
    ip = local_ip()
    by = "SimuladorXML"
    
    print(f"Dirección IP del sistema: {ip}")
    print(f"Usuario del sistema: {by}")
    
    stats = {
        'empleados_insertados': 0,
        'empleados_eliminados': 0,
        'asociaciones_deduccion': 0,
        'desasociaciones_deduccion': 0,
        'jornadas_asignadas': 0,
        'marcas_asistencia': 0,
        'semanas_cerradas': 0,
        'fechas_procesadas': 0
    }
    
    try:
        with connect() as conn:
            cur = conn.cursor()
            print("Conexión a base de datos establecida correctamente")
            
            # Obtener todos los nodos de fecha para procesamiento secuencial
            fechas_nodos = root.findall('FechaOperacion')
            total_fechas = len(fechas_nodos)
            
            # Procesamiento secuencial de cada fecha de operación
            for i, fecha_node in enumerate(fechas_nodos, 1):
                fecha_str = fecha_node.get('Fecha')
                fecha_obj = datetime.strptime(fecha_str, "%Y-%m-%d").date()
                es_jueves = fecha_obj.weekday() == 3  # 0=Lunes, 3=Jueves
                
                print(f"\n[{i:3d}/{total_fechas}] Procesando fecha {fecha_str} {'(JUEVES - Día de cierre)' if es_jueves else ''}")
                stats['fechas_procesadas'] += 1
                
                # OPERACIÓN 1: Inserción de nuevos empleados
                nuevos_empleados = fecha_node.find('./NuevosEmpleados')
                if nuevos_empleados is not None:
                    empleados_dia = 0
                    for nemp in nuevos_empleados.findall('NuevoEmpleado'):
                        try:
                            # Verificar si viene con Usuario y Password
                            username = nemp.get('Usuario')
                            password = nemp.get('Password')
                            
                            if username and password:
                                # SP crea usuario
                                call_sp(cur, 'InsertarEmpleadoOperacionConUsuario', [
                                    nemp.get('Nombre'),
                                    int(nemp.get('IdTipoDocumento')),
                                    nemp.get('ValorTipoDocumento'),
                                    nemp.get('FechaNacimiento'),
                                    int(nemp.get('IdDepartamento')),
                                    nemp.get('NombrePuesto'),
                                    username,
                                    password,
                                    by, ip
                                ])
                            else:
                                # SP considera formato viejo de XML
                                call_sp(cur, 'InsertarEmpleadoOperacion', [
                                    nemp.get('Nombre'),
                                    int(nemp.get('IdTipoDocumento')),
                                    nemp.get('ValorTipoDocumento'),
                                    nemp.get('FechaNacimiento'),
                                    int(nemp.get('IdDepartamento')),
                                    nemp.get('NombrePuesto'),
                                    None,  
                                    by, ip
                                ])
                            
                            empleados_dia += 1
                            stats['empleados_insertados'] += 1
                        except Exception as e:
                            print(f"    Error insertando empleado {nemp.get('Nombre')}: {e}")
                    
                    if empleados_dia > 0:
                        print(f"    {empleados_dia} empleados insertados correctamente")
                
                # OPERACIÓN 2: Eliminación lógica de empleados
                eliminaciones = fecha_node.find('./EliminarEmpleados')
                if eliminaciones is not None:
                    eliminados_dia = 0
                    for dele in eliminaciones.findall('EliminarEmpleado'):
                        try:
                            call_sp(cur, 'EliminarEmpleadoOperacion', [
                                dele.get('ValorTipoDocumento'), by, ip
                            ])
                            eliminados_dia += 1
                            stats['empleados_eliminados'] += 1
                        except Exception as e:
                            print(f"    Error eliminando empleado {dele.get('ValorTipoDocumento')}: {e}")
                    
                    if eliminados_dia > 0:
                        print(f"    {eliminados_dia} empleados eliminados correctamente")
                
                # OPERACIÓN 3: Asociación de deducciones no obligatorias
                asociaciones = fecha_node.find('./AsociacionEmpleadoDeducciones')
                if asociaciones is not None:
                    asociaciones_dia = 0
                    for aso in asociaciones.findall('AsociacionEmpleadoConDeduccion'):
                        try:
                            # Opción 1: monto = None si no viene definido
                            montostr = aso.get('Monto')
                            monto_val = float(montostr) if montostr is not None else None
                
                            call_sp(cur, 'AsociarEmpleadoConDeduccion', [
                                aso.get('ValorTipoDocumento'),
                                int(aso.get('IdTipoDeduccion')),
                                monto_val,
                                by, ip
                            ])
                            asociaciones_dia += 1
                            stats['asociaciones_deduccion'] += 1
                        except Exception as e:
                            print(f"    Error asociando deducción: {e}")
                    if asociaciones_dia > 0:
                        print(f"    {asociaciones_dia} deducciones asociadas correctamente")

                
                # OPERACIÓN 4: Desasociación de deducciones no obligatorias
                desasociaciones = fecha_node.find('./DesasociacionEmpleadoDeducciones')
                if desasociaciones is not None:
                    desasociaciones_dia = 0
                    for des in desasociaciones.findall('DesasociacionEmpleadoConDeduccion'):
                        try:
                            call_sp(cur, 'DesasociarEmpleadoConDeduccion', [
                                des.get('ValorTipoDocumento'),
                                int(des.get('IdTipoDeduccion')),
                                by, ip
                            ])
                            desasociaciones_dia += 1
                            stats['desasociaciones_deduccion'] += 1
                        except Exception as e:
                            print(f"    Error desasociando deducción: {e}")
                    
                    if desasociaciones_dia > 0:
                        print(f"    {desasociaciones_dia} deducciones desasociadas correctamente")
                
                # OPERACIÓN 5: Asignación de jornadas para próxima semana (solo jueves)
                if es_jueves:
                    jornadas_node = fecha_node.find('./JornadasProximaSemana')
                    if jornadas_node is not None:
                        # Calcular fecha de inicio de próxima semana (viernes)
                        viernes = fecha_obj + timedelta(days=1)
                        
                        # Crear o recuperar semana planilla usando SP
                        try:
                            id_semana, rc_sem = call_sp_with_output(cur, 'GetOrCreateSemanaPlanilla', [
                                viernes, by, ip
                            ])
                            
                            # Crear encabezados de planilla para todos los empleados activos
                            call_sp(cur, 'CrearEncabezadosSemanaPlanilla', [
                                id_semana, by, ip
                            ])
                            
                            # Procesar asignaciones de jornada para cada empleado
                            jornadas_dia = 0
                            for jor in jornadas_node.findall('TipoJornadaProximaSemana'):
                                try:
                                    call_sp(cur, 'AsignarTipoJornadaSemana', [
                                        jor.get('ValorTipoDocumento'),
                                        int(jor.get('IdTipoJornada')),
                                        id_semana,
                                        by, ip
                                    ])
                                    jornadas_dia += 1
                                    stats['jornadas_asignadas'] += 1
                                except Exception as e:
                                    print(f"    Error asignando jornada: {e}")
                            
                            if jornadas_dia > 0:
                                print(f"    Semana planilla ID {id_semana} creada exitosamente")
                                print(f"    {jornadas_dia} jornadas asignadas para próxima semana")
                        
                        except Exception as e:
                            print(f"    Error creando semana planilla: {e}")
                
                # OPERACIÓN 6: Procesamiento de marcas de asistencia
                marcas_node = fecha_node.find('./MarcasAsistencia')
                if marcas_node is not None:
                    marcas_dia = 0
                    errores_marca = 0
                    
                    for marca in marcas_node.findall('MarcaDeAsistencia'):
                        try:
                            # El SP InsertarMarcaAsistencia genera automáticamente
                            # todos los movimientos de planilla correspondientes
                            call_sp(cur, 'InsertarMarcaAsistencia', [
                                marca.get('ValorTipoDocumento'),
                                marca.get('HoraEntrada'),
                                marca.get('HoraSalida'),
                                by, ip
                            ])
                            marcas_dia += 1
                            stats['marcas_asistencia'] += 1
                        except RuntimeError as e:
                            if "50013" in str(e):  # Código de error: sin jornada asignada
                                errores_marca += 1
                            else:
                                print(f"    Error marca asistencia: {e}")
                        except Exception as e:
                            print(f"    Error procesando marca: {e}")
                    
                    if marcas_dia > 0:
                        print(f"    {marcas_dia} marcas de asistencia procesadas correctamente")
                        print(f"    Movimientos de planilla generados automáticamente")
                    
                    if errores_marca > 0:
                        print(f"    {errores_marca} marcas sin jornada asignada (omitidas según especificación)")
                
                # OPERACIÓN 7: Cierre semanal de planilla (solo jueves)
                if es_jueves:
                    sem_id, rc_sem = call_sp_with_output(
                        cur, 'SP_ObtenerIdSemanaPorFecha', [fecha_obj])
                
                    if rc_sem == 0:                     # semana encontrada
                        call_sp(cur, 'CierreSemanalPlanilla', [sem_id, by, ip])
                        stats['semanas_cerradas'] += 1
                        print(f"    Cierre semanal completado (Semana ID: {sem_id})")
                
                    elif rc_sem == 50015:               # primera vez: no hay nada que cerrar
                        print("    (Primer jueves: no hay semana que cerrar)")
                
                    else:                               
                        raise RuntimeError(f"SP_ObtenerIdSemanaPorFecha devolvió código {rc_sem}")
                
                # Mostrar progreso cada 20 días procesados (Nada mas para tener una idea de como va)
                if i % 20 == 0:
                    progreso = (i / total_fechas) * 100
                    print(f"\nProgreso de simulación: {i}/{total_fechas} días procesados ({progreso:.1f}%)")
            
            # CONFIRMACIÓN DE TRANSACCIÓN ÚNICA
            print(f"\nConfirmando transacción única para {stats['fechas_procesadas']} días de operación...")
            conn.commit()
            print("TRANSACCIÓN CONFIRMADA EXITOSAMENTE")
            
            # REPORTE DE ESTADÍSTICAS FINALES
            print("\n" + "="*70)
            print("ESTADÍSTICAS FINALES DE SIMULACIÓN")
            print("="*70)
            print(f"Fechas procesadas:          {stats['fechas_procesadas']:,}")
            print(f"Empleados insertados:       {stats['empleados_insertados']:,}")
            print(f"Empleados eliminados:       {stats['empleados_eliminados']:,}")
            print(f"Deducciones asociadas:      {stats['asociaciones_deduccion']:,}")
            print(f"Deducciones desasociadas:   {stats['desasociaciones_deduccion']:,}")
            print(f"Jornadas asignadas:         {stats['jornadas_asignadas']:,}")
            print(f"Marcas de asistencia:       {stats['marcas_asistencia']:,}")
            print(f"Semanas cerradas:           {stats['semanas_cerradas']:,}")
            print("="*70)
            print("SIMULACIÓN COMPLETADA EXITOSAMENTE")
            
    except Exception as e:
        print(f"\nERROR CRÍTICO EN SIMULACIÓN: {e}")
        print("La transacción será revertida automáticamente por la base de datos")
        return 1
    
    return 0


if __name__ == "__main__":
    xml_file = "operacion.xml"
    
    if len(sys.argv) > 1:
        xml_file = sys.argv[1]
    
    print(f"Archivo de operación a procesar: {xml_file}")
    
    try:
        exit_code = main(xml_file)
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nSimulación interrumpida por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\nError fatal durante la ejecución: {e}")
        sys.exit(1)