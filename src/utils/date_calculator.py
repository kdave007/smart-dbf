from datetime import datetime, timedelta
import calendar
from pathlib import Path
import sys
import logging

class DateCalculator:
    def __init__(self):
        self.date_format = "%Y %m %d"
        self.api_date_format = "%Y-%m-%d"
    
    def _get_base_path(self):
        """Get base path for exe or development environment - SAME AS CONFIGMANAGER"""
        if getattr(sys, 'frozen', False):
            # Running as exe - ALWAYS use exe directory (external)
            return Path(sys.executable).parent
        else:
            # Running in development - use project root
            return Path(__file__).parent.parent.parent
    
    def get_current_date(self):
        """Obtiene la fecha actual en formato YYYY MM DD"""
        return datetime.now().strftime(self.date_format)
    
    def get_month_range(self, start_date=None):
        """
        Calcula el rango del mes actual o basado en una fecha de inicio
        
        Args:
            start_date (str, optional): Fecha de inicio en formato YYYY MM DD
        
        Returns:
            tuple: (start_date, end_date) en formato YYYY MM DD
        """
        today = datetime.now()
        
        # Si se proporciona una fecha de inicio, usarla
        if start_date:
            try:
                start_dt = datetime.strptime(start_date, self.date_format)
            except ValueError:
                raise ValueError(f"Formato de fecha inválido. Use: {self.date_format}")
        else:
            # Si es día 1, usar mes anterior hasta día 1 actual
            if today.day == 1:
                # Obtener último día del mes anterior
                last_month = today.replace(day=1) - timedelta(days=1)
                start_dt = last_month.replace(day=1)
            else:
                # Usar día 1 del mes actual
                start_dt = today.replace(day=1)
        
        # End date es siempre la fecha actual
        end_dt = today
        
        return start_dt.strftime(self.date_format), end_dt.strftime(self.date_format)
    
    def parse_date(self, date_string):
        """Convierte string a objeto datetime"""
        return datetime.strptime(date_string, self.date_format)
    
    def format_date(self, date_obj):
        """Convierte objeto datetime a string con formato YYYY MM DD"""
        return date_obj.strftime(self.date_format)
    
    def get_previous_month_range(self):
        """Obtiene el rango completo del mes anterior"""
        today = datetime.now()
        first_day_current = today.replace(day=1)
        last_day_previous = first_day_current - timedelta(days=1)
        first_day_previous = last_day_previous.replace(day=1)
        
        return first_day_previous.strftime(self.date_format), last_day_previous.strftime(self.date_format)
    
    def get_custom_range(self, start_date, end_date=None):
        """
        Obtiene un rango personalizado
        
        Args:
            start_date (str): Fecha de inicio en formato YYYY MM DD
            end_date (str, optional): Fecha fin. Si es None, usa fecha actual
        
        Returns:
            tuple: (start_date, end_date) en formato YYYY MM DD
        """
        try:
            start_dt = datetime.strptime(start_date, self.date_format)
        except ValueError:
            raise ValueError(f"Formato de fecha de inicio inválido. Use: {self.date_format}")
        
        if end_date:
            try:
                end_dt = datetime.strptime(end_date, self.date_format)
            except ValueError:
                raise ValueError(f"Formato de fecha fin inválido. Use: {self.date_format}")
        else:
            end_dt = datetime.now()
        
        return start_dt.strftime(self.date_format), end_dt.strftime(self.date_format)
    
    def get_date_range_from_env(self, output_format="api"):
        """
        Lee las fechas START_DATE y END_DATE del archivo .env
        Si CUSTOM_DATE_RANGE es 0 o no existe, usa el rango del mes actual
        Si END_DATE es 0, usa la fecha actual como fecha final
        
        Args:
            output_format (str): Formato de salida - "api" para YYYY-MM-DD o "default" para YYYY MM DD
        
        Returns:
            dict: {"from": start_date, "to": end_date} en el formato especificado
        """
        # USAR LA MISMA LÓGICA DE RUTAS QUE CONFIGMANAGER
        base_path = self._get_base_path()
        env_path = base_path / '.env'
        
        print(f"🔍 DateCalculator searching for .env at: {env_path}")
        
        if not env_path.exists():
            # List available files for debugging
            available_files = list(base_path.glob('*'))
            logging.error(f".env file not found at {env_path}")
            logging.error(f"Available files in {base_path}: {[f.name for f in available_files]}")
            raise FileNotFoundError(f".env file not found at {env_path}")
        
        start_date = None
        end_date = None
        custom_date_range = None
        
        try:
            with open(env_path, 'r', encoding='utf-8') as file:
                for line in file:
                    line = line.strip()
                    
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        continue
                    
                    # Parse key=value pairs
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        if key == 'START_DATE':
                            start_date = value
                        elif key == 'END_DATE':
                            end_date = value
                        elif key == 'CUSTOM_DATE_RANGE':
                            custom_date_range = value
        
        except Exception as e:
            raise Exception(f"Error reading .env file: {str(e)}")
        
        # Check if CUSTOM_DATE_RANGE is 0 or not set - use month range
        if not custom_date_range or custom_date_range == '0':
            # Use current month range (from day 1 of current month to today)
            today = datetime.now()
            start_date_obj = today.replace(day=1)
            end_date_obj = today
        else:
            # Use custom dates from .env
            if not start_date:
                raise ValueError("START_DATE not found in .env file when CUSTOM_DATE_RANGE=1")
            
            # If END_DATE is 0 or not found, use current date
            if not end_date or end_date == '0':
                end_date_obj = datetime.now()
            else:
                try:
                    # Try to parse the end date (assuming YYYY-MM-DD format in .env)
                    end_date_obj = datetime.strptime(end_date, self.api_date_format)
                except ValueError:
                    raise ValueError(f"Invalid END_DATE format in .env. Expected YYYY-MM-DD, got: {end_date}")
            
            # Parse start date
            try:
                start_date_obj = datetime.strptime(start_date, self.api_date_format)
            except ValueError:
                raise ValueError(f"Invalid START_DATE format in .env. Expected YYYY-MM-DD, got: {start_date}")
        
        # Format output based on requested format
        if output_format == "api":
            result = {
                "from": start_date_obj.strftime(self.api_date_format),
                "to": end_date_obj.strftime(self.api_date_format)
            }
        else:
            result = {
                "from": start_date_obj.strftime(self.date_format),
                "to": end_date_obj.strftime(self.date_format)
            }
        
        print(f"✅ DateCalculator loaded date range: {result}")
        return result

# Ejemplo de uso
if __name__ == "__main__":
    calculator = DateCalculator()
    
    print("=== Pruebas de la clase DateCalculator ===")
    
    # 1. Obtener fecha actual
    current_date = calculator.get_current_date()
    print(f"Fecha actual: {current_date}")
    
    # 2. Obtener rango del mes actual
    start, end = calculator.get_month_range()
    print(f"Rango automático: Start={start}, End={end}")
    
    # 3. Probar con fecha de inicio personalizada
    custom_start = "2024 01 15"
    start, end = calculator.get_month_range(custom_start)
    print(f"Rango con inicio personalizado: Start={start}, End={end}")
    
    # 4. Obtener rango del mes anterior
    prev_start, prev_end = calculator.get_previous_month_range()
    print(f"Rango mes anterior: Start={prev_start}, End={prev_end}")
    
    # 5. Rango completamente personalizado
    custom_start = "2024 01 01"
    custom_end = "2024 01 31"
    start, end = calculator.get_custom_range(custom_start, custom_end)
    print(f"Rango personalizado: Start={start}, End={end}")
    
    # 6. Rango personalizado sin fecha fin (usa actual)
    start, end = calculator.get_custom_range(custom_start)
    print(f"Rango personalizado sin fin: Start={start}, End={end}")
    
    # 7. Probar lectura desde .env
    try:
        date_range = calculator.get_date_range_from_env()
        print(f"Rango desde .env: {date_range}")
    except Exception as e:
        print(f"Error leyendo .env: {e}")