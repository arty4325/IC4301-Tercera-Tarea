#!/usr/bin/env python3
# ================================================================
#  CARGADOR DE CATÁLOGOS - TEC Base de Datos I
#  Control de Asistencia y Planilla Obrera
#  Estudiante: Oscar Arturo Acuña Durán (2022049304)
#               Alejandro Umaña Miranda  (2024130345)
# ================================================================

import pyodbc
import xml.etree.ElementTree as ET
import socket
from datetime import datetime
import sys
from sys import exit

# Se establece una conexción con la base de datos hosteada en CloudClusters
SERVER = "mssql-196019-0.cloudclusters.net,10245"
DATABASE = "BASESPROYECTO"
USERNAME = "requeSoftware"
PASSWORD = "#Jaime123"
DRIVER = "ODBC Driver 17 for SQL Server"
DATE_FMT = "%Y-%m-%d"

class CargadorCatalogos:
    """
    Clase principal para la carga de catálogos desde XML hacia SQL Server
    """
    
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.post_by = "Sistema"
        self.post_ip = self.get_local_ip()
        
    def get_local_ip(self):
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
    
    def conectar_bd(self):
        """
        Establece conexión con la base de datos SQL Server con los credenciales definidos anteriormente
        """
        try:
            connection_string = (
                f"DRIVER={{{DRIVER}}};"
                f"SERVER={SERVER};"
                f"DATABASE={DATABASE};"
                f"UID={USERNAME};"
                f"PWD={PASSWORD};"
                f"TrustServerCertificate=yes;"
            )
            
            self.connection = pyodbc.connect(connection_string)
            self.cursor = self.connection.cursor()
            print("Conexión a base de datos establecida exitosamente")
            return True
            
        except Exception as e:
            print(f"Error al conectar a la base de datos: {e}")
            return False
    
    def desconectar_bd(self):
        """
        Cierra la conexión con la base de datos de manera segura
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Conexión a base de datos cerrada correctamente")
    
    def ejecutar_sp(self, sp_name, params):
        """
        Ejecuta un procedimiento almacenado con manejo de errores
        Esto con el fin de modularizar la llamada de procedimientos almacenados
        """
        try:
            params.extend([self.post_by, self.post_ip])
            
            param_placeholders = ', '.join(['?'] * len(params))
            sql = f"DECLARE @result INT; EXEC dbo.{sp_name} {param_placeholders}, @result OUTPUT; SELECT @result"
            
            self.cursor.execute(sql, params)
            result = self.cursor.fetchone()
            result_code = result[0] if result else 50008
            
            self.connection.commit()
            return result_code
            
        except Exception as e:
            print(f"Error ejecutando {sp_name}: {e}")
            if self.connection:
                self.connection.rollback()
            return 50008
    
    def cargar_tipos_documento(self, tipos_doc_element):
        """
        Carga los tipos de documento de identidad desde XML
        """
        print("\nCargando Tipos de Documento de Identidad...")
        count = 0
        errors = 0
        
        for tipo_doc in tipos_doc_element.findall('TipoDocuIdentidad'):
            try:
                id_tipo = int(tipo_doc.get('Id'))
                nombre = tipo_doc.get('Nombre')
                
                result_code = self.ejecutar_sp('InsertarTipoDocIdentidad', [id_tipo, nombre])
                
                if result_code == 0:
                    count += 1
                    print(f"   {nombre} (ID: {id_tipo}) - Insertado correctamente")
                else:
                    errors += 1
                    print(f"   Error cargando {nombre}: Código {result_code}")
                    
            except Exception as e:
                errors += 1
                print(f"   Error procesando tipo documento: {e}")
        
        print(f"   Total cargados: {count}, Errores: {errors}")
    
    def cargar_tipos_jornada(self, tipos_jornada_element):
        """
        Carga los tipos de jornada laboral desde XML
        """
        print("\nCargando Tipos de Jornada...")
        count = 0
        errors = 0
        
        for tipo_jornada in tipos_jornada_element.findall('TipoDeJornada'):
            try:
                id_tipo = int(tipo_jornada.get('Id'))
                nombre = tipo_jornada.get('Nombre')
                hora_inicio = tipo_jornada.get('HoraInicio')
                hora_fin = tipo_jornada.get('HoraFin')
                
                result_code = self.ejecutar_sp('InsertarTipoJornada', 
                                             [id_tipo, nombre, hora_inicio, hora_fin])
                
                if result_code == 0:
                    count += 1
                    print(f"   {nombre} ({hora_inicio}-{hora_fin}) - Insertado correctamente")
                else:
                    errors += 1
                    print(f"   Error cargando {nombre}: Código {result_code}")
                    
            except Exception as e:
                errors += 1
                print(f"   Error procesando tipo jornada: {e}")
        
        print(f"   Total cargados: {count}, Errores: {errors}")
    
    def cargar_puestos(self, puestos_element):
        """
        Carga los puestos de trabajo con salarios desde XML
        """
        print("\nCargando Puestos de Trabajo...")
        count = 0
        errors = 0
        
        for puesto in puestos_element.findall('Puesto'):
            try:
                nombre = puesto.get('Nombre')
                salario_hora = float(puesto.get('SalarioXHora'))
                
                result_code = self.ejecutar_sp('InsertarPuesto', [nombre, salario_hora])
                
                if result_code == 0:
                    count += 1
                    print(f"   {nombre} (₡{salario_hora}/hora) - Insertado correctamente")
                else:
                    errors += 1
                    print(f"   Error cargando {nombre}: Código {result_code}")
                    
            except Exception as e:
                errors += 1
                print(f"   Error procesando puesto: {e}")
        
        print(f"   Total cargados: {count}, Errores: {errors}")
    
    def cargar_departamentos(self, departamentos_element):
        """
        Carga los departamentos organizacionales desde XML
        """
        print("\nCargando Departamentos...")
        count = 0
        errors = 0
        
        for departamento in departamentos_element.findall('Departamento'):
            try:
                id_depto = int(departamento.get('Id'))
                nombre = departamento.get('Nombre')
                
                result_code = self.ejecutar_sp('InsertarDepartamento', [id_depto, nombre])
                
                if result_code == 0:
                    count += 1
                    print(f"   {nombre} (ID: {id_depto}) - Insertado correctamente")
                else:
                    errors += 1
                    print(f"   Error cargando {nombre}: Código {result_code}")
                    
            except Exception as e:
                errors += 1
                print(f"   Error procesando departamento: {e}")
        
        print(f"   Total cargados: {count}, Errores: {errors}")
    
    def cargar_feriados(self, feriados_element):
        """
        Carga los días feriados nacionales desde XML
        """
        print("\nCargando Feriados...")
        count = 0
        errors = 0
        
        for feriado in feriados_element.findall('Feriado'):
            try:
                id_feriado = int(feriado.get('Id'))
                nombre = feriado.get('Nombre')
                fecha_str = feriado.get('Fecha')
                
                # Convertir fecha de formato YYYYMMDD a objeto date
                fecha = datetime.strptime(fecha_str, '%Y%m%d').date()
                
                result_code = self.ejecutar_sp('InsertarFeriado', [id_feriado, nombre, fecha])
                
                if result_code == 0:
                    count += 1
                    print(f"   {nombre} ({fecha}) - Insertado correctamente")
                else:
                    errors += 1
                    print(f"   Error cargando {nombre}: Código {result_code}")
                    
            except Exception as e:
                errors += 1
                print(f"   Error procesando feriado: {e}")
        
        print(f"   Total cargados: {count}, Errores: {errors}")
    
    def cargar_tipos_movimiento(self, tipos_mov_element):
        """
        Carga los tipos de movimiento de planilla desde XML
        """
        print("\nCargando Tipos de Movimiento...")
        count = 0
        errors = 0
        
        for tipo_mov in tipos_mov_element.findall('TipoDeMovimiento'):
            try:
                id_tipo = int(tipo_mov.get('Id'))
                nombre = tipo_mov.get('Nombre')
                
                result_code = self.ejecutar_sp('InsertarTipoMovimiento', [id_tipo, nombre])
                
                if result_code == 0:
                    count += 1
                    print(f"   {nombre} (ID: {id_tipo}) - Insertado correctamente")
                else:
                    errors += 1
                    print(f"   Error cargando {nombre}: Código {result_code}")
                    
            except Exception as e:
                errors += 1
                print(f"   Error procesando tipo movimiento: {e}")
        
        print(f"   Total cargados: {count}, Errores: {errors}")
    
    def cargar_tipos_deduccion(self, tipos_ded_element):
        """
        Carga los tipos de deducción (obligatorias y opcionales) desde XML
        """
        print("\nCargando Tipos de Deducción...")
        count = 0
        errors = 0
        
        for tipo_ded in tipos_ded_element.findall('TipoDeDeduccion'):
            try:
                id_tipo = int(tipo_ded.get('Id'))
                nombre = tipo_ded.get('Nombre')
                obligatorio = 1 if tipo_ded.get('Obligatorio', 'No').upper() == 'SI' else 0
                porcentual = 1 if tipo_ded.get('Porcentual', 'No').upper() == 'SI' else 0
                valor = float(tipo_ded.get('Valor', '0'))
                
                result_code = self.ejecutar_sp('InsertarTipoDeduccion', 
                                             [id_tipo, nombre, obligatorio, porcentual, valor])
                
                if result_code == 0:
                    count += 1
                    tipo_desc = "Obligatorio" if obligatorio else "Opcional"
                    valor_desc = f"{valor*100}%" if porcentual else f"₡{valor}"
                    print(f"   {nombre} ({tipo_desc}, {valor_desc}) - Insertado correctamente")
                else:
                    errors += 1
                    print(f"   Error cargando {nombre}: Código {result_code}")
                    
            except Exception as e:
                errors += 1
                print(f"   Error procesando tipo deducción: {e}")
        
        print(f"   Total cargados: {count}, Errores: {errors}")
    
    def cargar_tipos_usuario(self):
        """
        Carga los tipos de usuario predefinidos del sistema
        Incluye: Administrador, Empleado, Sistema
        """
        print("\nCargando Tipos de Usuario")
        tipos_usuario = [
            (1, "Administrador"),
            (2, "Empleado"),
            (3, "Sistema")
        ]
        
        count = 0
        errors = 0
        
        for id_tipo, nombre in tipos_usuario:
            try:
                result_code = self.ejecutar_sp('InsertarTipoUsuario', [id_tipo, nombre])
                
                if result_code == 0:
                    count += 1
                    print(f"   {nombre} (ID: {id_tipo}) - Insertado correctamente")
                else:
                    errors += 1
                    print(f"   Error cargando {nombre}: Código {result_code}")
                    
            except Exception as e:
                errors += 1
                print(f"   Error procesando tipo usuario: {e}")
        
        print(f"   Total cargados: {count}, Errores: {errors}")
    
    def cargar_usuarios(self, usuarios_element):
        """
        Carga los usuarios del sistema desde XML
        """
        print("\nCargando Usuarios...")
        count = 0
        errors = 0
        
        for usuario in usuarios_element.findall('Usuario'):
            try:
                id_usuario = int(usuario.get('Id'))
                username = usuario.get('Username')
                password = usuario.get('Password')
                tipo_usuario = int(usuario.get('Tipo'))
                
                result_code = self.ejecutar_sp('InsertarUsuario', 
                                             [id_usuario, username, password, tipo_usuario])
                
                if result_code == 0:
                    count += 1
                    tipo_desc = {1: "Admin", 2: "Empleado", 3: "Sistema"}.get(tipo_usuario, "Desconocido")
                    print(f"   {username} ({tipo_desc}) - Insertado correctamente")
                else:
                    errors += 1
                    print(f"   Error cargando {username}: Código {result_code}")
                    
            except Exception as e:
                errors += 1
                print(f"   Error procesando usuario: {e}")
        
        print(f"   Total cargados: {count}, Errores: {errors}")
    
    def cargar_tipos_evento(self, tipos_evento_element):
        """
        Carga los tipos de evento para auditoría desde XML
        """
        print("\nCargando Tipos de Evento...")
        count = 0
        errors = 0
        
        for tipo_evento in tipos_evento_element.findall('TipoEvento'):
            try:
                id_tipo = int(tipo_evento.get('Id'))
                nombre = tipo_evento.get('Nombre')
                
                result_code = self.ejecutar_sp('InsertarTipoEvento', [id_tipo, nombre])
                
                if result_code == 0:
                    count += 1
                    print(f"   {nombre} (ID: {id_tipo}) - Insertado correctamente")
                else:
                    errors += 1
                    print(f"   Error cargando {nombre}: Código {result_code}")
                    
            except Exception as e:
                errors += 1
                print(f"   Error procesando tipo evento: {e}")
        
        print(f"   Total cargados: {count}, Errores: {errors}")
    
    def cargar_errores(self, errores_element):
        """
        Carga el catálogo de códigos de error del sistema desde XML
        """
        print("\nCargando Catálogo de Errores...")
        count = 0
        errors = 0
        
        for error in errores_element.findall('Error'):
            try:
                codigo = int(error.get('Codigo'))
                descripcion = error.get('Descripcion')
                
                result_code = self.ejecutar_sp('InsertarError', [codigo, descripcion])
                
                if result_code == 0:
                    count += 1
                    print(f"   {codigo}: {descripcion} - Insertado correctamente")
                else:
                    errors += 1
                    print(f"   Error cargando código {codigo}: Código {result_code}")
                    
            except Exception as e:
                errors += 1
                print(f"   Error procesando error: {e}")
        
        print(f"   Total cargados: {count}, Errores: {errors}")
    
    def cargar_empleados(self, empleados_element):
        """
        Carga los empleados iniciales desde XML
        Asocia automáticamente deducciones obligatorias via trigger
        """
        print("\nCargando Empleados...")
        count = 0
        errors = 0
        
        for empleado in empleados_element.findall('Empleado'):
            try:
                nombre = empleado.get('Nombre')
                id_tipo_doc = int(empleado.get('IdTipoDocumento'))
                valor_doc = empleado.get('ValorDocumento')
                fecha_nac_str = empleado.get('FechaNacimiento')
                id_departamento = int(empleado.get('IdDepartamento'))
                nombre_puesto = empleado.get('NombrePuesto')
                
                # Convertir fecha de nacimiento a formato DATE
                fecha_nacimiento = datetime.strptime(fecha_nac_str, '%Y-%m-%d').date()
                
                result_code = self.ejecutar_sp('InsertarEmpleado', 
                                             [nombre, id_tipo_doc, valor_doc, fecha_nacimiento, 
                                              id_departamento, nombre_puesto, None])
                
                if result_code == 0:
                    count += 1
                    print(f"   {nombre} ({valor_doc}) - Insertado correctamente")
                else:
                    errors += 1
                    print(f"   Error cargando {nombre}: Código {result_code}")
                    
            except Exception as e:
                errors += 1
                print(f"   Error procesando empleado: {e}")
        
        print(f"   Total cargados: {count}, Errores: {errors}")
    
    def cargar_catalogos_desde_xml(self, archivo_xml):
        """
        Método principal para cargar todos los catálogos desde archivo XML
        Respeta el orden de dependencias de llaves foráneas
        """
        try:
            print(f"Leyendo archivo XML: {archivo_xml}")
            tree = ET.parse(archivo_xml)
            root = tree.getroot()
            
            print("Iniciando carga de catálogos del sistema...")
            print(f"Dirección IP del sistema: {self.post_ip}")
            print(f"Usuario del sistema: {self.post_by}")
            
            # Cargar catálogos en orden de dependencias para evitar errores
            tipos_doc = root.find('TiposdeDocumentodeIdentidad')
            if tipos_doc is not None:
                self.cargar_tipos_documento(tipos_doc)
            
            tipos_jornada = root.find('TiposDeJornada')
            if tipos_jornada is not None:
                self.cargar_tipos_jornada(tipos_jornada)
            
            puestos = root.find('Puestos')
            if puestos is not None:
                self.cargar_puestos(puestos)
            
            departamentos = root.find('Departamentos')
            if departamentos is not None:
                self.cargar_departamentos(departamentos)
            
            feriados = root.find('Feriados')
            if feriados is not None:
                self.cargar_feriados(feriados)
            
            tipos_movimiento = root.find('TiposDeMovimiento')
            if tipos_movimiento is not None:
                self.cargar_tipos_movimiento(tipos_movimiento)
            
            tipos_deduccion = root.find('TiposDeDeduccion')
            if tipos_deduccion is not None:
                self.cargar_tipos_deduccion(tipos_deduccion)
            
            tipos_evento = root.find('TiposdeEvento')
            if tipos_evento is not None:
                self.cargar_tipos_evento(tipos_evento)
            
            errores = root.find('Errores')
            if errores is not None:
                self.cargar_errores(errores)
            
            # Nivel 2: Tipos de usuario (requerido antes de usuarios)
            self.cargar_tipos_usuario()
            
            # Nivel 3: Usuarios (referencia a tipos de usuario)
            usuarios = root.find('Usuarios')
            if usuarios is not None:
                self.cargar_usuarios(usuarios)
            
            # Nivel 4: Empleados (depende de múltiples catálogos)
            empleados = root.find('Empleados')
            if empleados is not None:
                self.cargar_empleados(empleados)
            
            print("\nCarga de catálogos completada exitosamente")
            
        except FileNotFoundError:
            print(f"Error: No se encontró el archivo {archivo_xml}")
        except ET.ParseError as e:
            print(f"Error al parsear archivo XML: {e}")
        except Exception as e:
            print(f"Error inesperado durante la carga: {e}")

def main():
    """
    Función principal del programa cargador de catálogos
    Maneja argumentos de línea de comandos y coordina la ejecución
    """
    # Configuración de archivo XML por defecto
    archivo_xml = "catalogos.xml"
    
    # Permitir especificar archivo XML como argumento
    if len(sys.argv) > 1:
        archivo_xml = sys.argv[1]
    
    print("=" * 60)
    print("   CARGADOR DE CATÁLOGOS - TEC BASE DE DATOS I")
    print("   Control de Asistencia y Planilla Obrera")
    print("=" * 60)
    
    cargador = CargadorCatalogos()
    
    try:
        if cargador.conectar_bd():
            cargador.cargar_catalogos_desde_xml(archivo_xml)
        else:
            print("No se pudo establecer conexión con la base de datos")
            return 1
            
    except KeyboardInterrupt:
        print("\nProceso interrumpido por el usuario")
        return 1
    except Exception as e:
        print(f"Error fatal durante la ejecución: {e}")
        return 1
    finally:
        cargador.desconectar_bd()
    
    return 0

if __name__ == "__main__":
    exit(main())