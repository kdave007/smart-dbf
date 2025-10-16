import logging
import sys
from pathlib import Path
from datetime import datetime

class LoggingController:
    
    def __init__(self, log_level=logging.INFO, log_to_file=True, log_dir="logs"):
        """
        Initialize logging controller
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_to_file: Whether to log to file
            log_dir: Directory to store log files
        """
        self.log_level = log_level
        self.log_to_file = log_to_file
        self.log_dir = Path(log_dir)
        self.logger = None
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging configuration"""
        # Create logger - ✅ AHORA usa el módulo logging importado
        self.logger = logging.getLogger('smart-dbf')
        self.logger.setLevel(self.log_level)
        
        # ✅ EVITAR DUPLICADOS - Solo configurar si no hay handlers
        if not self.logger.handlers:
            # Create formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(self.log_level)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            
            # File handler (if enabled)
            if self.log_to_file:
                self._setup_file_handler(formatter)
        
        # ✅ EVITAR PROPAGACIÓN al root logger
        self.logger.propagate = False
    
    def _setup_file_handler(self, formatter):
        """Setup file logging handler"""
        try:
            # Create log directory if it doesn't exist
            self.log_dir.mkdir(exist_ok=True)
            
            # Create log filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d')
            log_file = self.log_dir / f"smart-dbf-{timestamp}.log"
            
            # File handler
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(self.log_level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
            # ✅ Usar self.logger en lugar de print
            self.logger.info(f"Logging to file: {log_file}")
            
        except Exception as e:
            # ✅ Usar self.logger en lugar de print
            self.logger.warning(f"Could not setup file logging: {e}")
    
    def debug(self, message):
        """Log debug message"""
        self.logger.debug(message)
    
    def info(self, message):
        """Log info message"""
        self.logger.info(message)
    
    def warning(self, message):
        """Log warning message"""
        self.logger.warning(message)
    
    def error(self, message):
        """Log error message"""
        self.logger.error(message)
    
    def critical(self, message):
        """Log critical message"""
        self.logger.critical(message)
    
    def log_table_operation(self, table_name, operation, duration=None, record_count=None):
        """Log table operation with structured format"""
        msg = f"[TABLE_OP] {operation} on {table_name}"
        if duration:
            msg += f" - Duration: {duration:.2f}s"
        if record_count is not None:
            msg += f" - Records: {record_count}"
        self.info(msg)
    
    def log_filter_operation(self, table_name, filters, result_count=None):
        """Log filter operation"""
        msg = f"[FILTER] Applied to {table_name}: {filters}"
        if result_count is not None:
            msg += f" - Results: {result_count}"
        self.info(msg)
    
    def log_error_with_context(self, error, context=None):
        """Log error with additional context"""
        msg = f"[ERROR] {str(error)}"
        if context:
            msg += f" - Context: {context}"
        self.error(msg)
    
    def set_level(self, level):
        """Change logging level"""
        self.log_level = level
        self.logger.setLevel(level)
        for handler in self.logger.handlers:
            handler.setLevel(level)
    
    @classmethod
    def get_instance(cls, **kwargs):
        """Get singleton instance of logging controller"""
        if not hasattr(cls, '_instance'):
            cls._instance = cls(**kwargs)
        return cls._instance

# ✅ SOLUCIÓN: Cambia el nombre de la instancia global
logger = LoggingController.get_instance()