import os
import sys
from pathlib import Path
import traceback
from typing import Optional

from PyQt6 import QtCore
from PyQt6.QtCore import QTimer
from PyQt6.QtWidgets import QMessageBox
from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QFileDialog,
    QProgressBar, QCheckBox, QApplication, QSizePolicy,
    QGraphicsDropShadowEffect, QSpacerItem
)
from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QPropertyAnimation, 
    QEasingCurve, QPoint, QSize
)
from PyQt6.QtGui import QFont, QPalette, QColor, QIcon

from presentation.controllers.audit_controller import AuditController
from presentation.views.audit_file import FileInfoWidget

class AuditWorker(QThread):
    """Trabajador para procesar auditoría en segundo plano"""
    progress_update = pyqtSignal(int, str)
    audit_completed = pyqtSignal(dict)
    audit_error = pyqtSignal(Exception)

    def __init__(self, controller, contract, file_path, inventory_path=None, use_inventory=True):
        super().__init__()
        self.controller = controller
        self.contract = contract
        self.file_path = file_path
        self.inventory_path = inventory_path
        self.use_inventory = use_inventory

    def run(self):
        try:
            result = self.controller.process_audit(
                self.contract,
                self.file_path,
                inventory_file=self.inventory_path,
                use_inventory=self.use_inventory,
                progress_callback=self.update_progress
            )
            self.audit_completed.emit(result)
        except Exception as e:
            self.audit_error.emit(e)

    def update_progress(self, progress, message):
        self.progress_update.emit(int(progress * 100), message)


class AuditView(QMainWindow):
    def __init__(self, controller: Optional[AuditController] = None):
        super().__init__()
        self.old_pos = None
        self.controller = controller
        self.worker = None
        self.resize_mode = False
        self.move_mode = False
        
        # Define color palette as class attributes
        self.bg_color = QColor("#1A1E2E")            # Azul oscuro profundo (fondo principal)
        self.card_bg_color = QColor("#232741")       # Fondo de tarjeta más claro
        self.fg_color = QColor("#E2E8F0")            # Texto blanco con toque azulado
        self.input_bg_color = QColor("#2A2F44")      # Fondo de entrada más oscuro
        self.primary_color = QColor("#3B82F6")       # Azul vibrante (botón principal)
        self.secondary_color = QColor("#F59E0B")     # Naranja ámbar (botón secundario)
        self.accent_color = QColor("#10B981")        # Verde esmeralda para acentos
        self.border_color = QColor("#374151")        # Gris azulado para bordes
        self.error_color = QColor("#EF4444")         # Rojo para errores
        
        self.initUI()

    def initUI(self):
        # Configuración de ventana
        self.setWindowTitle("Audit Process Tool")
        self.setGeometry(100, 100, 800, 520)
        
        # Permitir redimensionamiento manteniendo FramelessWindowHint
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        
        # Establecer tamaños mínimos y máximos
        self.setMinimumSize(600, 450)
        self.setMaximumSize(1200, 800)
        
        # Configurar paleta global
        self.palette = QPalette()
        self.palette.setColor(QPalette.ColorRole.Window, self.bg_color)
        self.palette.setColor(QPalette.ColorRole.WindowText, self.fg_color)
        self.palette.setColor(QPalette.ColorRole.Base, self.input_bg_color)
        self.palette.setColor(QPalette.ColorRole.Text, self.fg_color)
        self.setPalette(self.palette)
        
        # Fuente principal
        font = QFont("Segoe UI", 10)
        self.setFont(font)
        
        # Contenedor principal
        main_container = QWidget()
        main_container.setObjectName("mainContainer")
        main_container.setStyleSheet(f"""
            #mainContainer {{
                background-color: {self.bg_color.name()};
                border-radius: 12px;
            }}
        """)
        
        main_layout = QVBoxLayout(main_container)
        main_layout.setSpacing(0)
        main_layout.setContentsMargins(15, 15, 15, 15)
        
        # ---- Barra de título personalizada ----
        title_bar = QWidget()
        title_bar.setFixedHeight(40)
        title_bar_layout = QHBoxLayout(title_bar)
        title_bar_layout.setContentsMargins(15, 0, 5, 0)
        
        app_icon = QLabel()
        # Si tienes un icono, podrías cargarlo aquí
        # app_icon.setPixmap(QIcon("icon.png").pixmap(24, 24))
        # app_icon.setFixedSize(24, 24)
        
        app_title = QLabel("Audit Process Tool")
        app_title.setStyleSheet(f"color: {self.fg_color.name()}; font-size: 16px; font-weight: 600;")
        
        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        
        btn_minimize = QPushButton("—")
        btn_minimize.setFixedSize(30, 30)
        btn_minimize.clicked.connect(self.showMinimized)
        
        btn_close = QPushButton("✕")
        btn_close.setFixedSize(30, 30)
        btn_close.clicked.connect(self.close)
        
        for btn in [btn_minimize, btn_close]:
            btn.setStyleSheet(f"""
                QPushButton {{
                    background-color: transparent;
                    color: {self.fg_color.name()};
                    border: none;
                    font-size: 14px;
                    border-radius: 15px;
                }}
                QPushButton:hover {{
                    background-color: rgba(255, 255, 255, 0.1);
                }}
                QPushButton:pressed {{
                    background-color: rgba(255, 255, 255, 0.2);
                }}
            """)
        
        title_bar_layout.addWidget(app_icon)
        title_bar_layout.addWidget(app_title)
        title_bar_layout.addWidget(spacer)
        title_bar_layout.addWidget(btn_minimize)
        title_bar_layout.addWidget(btn_close)
        
        # ---- Tarjeta principal (contenido) ----
        content_card = QWidget()
        content_card.setObjectName("contentCard")
        content_card.setStyleSheet(f"""
            #contentCard {{
                background-color: {self.card_bg_color.name()};
                border-radius: 12px;
                border: 1px solid {self.border_color.name()};
            }}
        """)
        
        # Agregar sombra a la tarjeta
        card_shadow = QGraphicsDropShadowEffect()
        card_shadow.setBlurRadius(20)
        card_shadow.setColor(QColor(0, 0, 0, 60))
        card_shadow.setOffset(0, 4)
        content_card.setGraphicsEffect(card_shadow)
        
        card_layout = QVBoxLayout(content_card)
        card_layout.setContentsMargins(25, 25, 25, 25)
        card_layout.setSpacing(20)
        
        # ---- Estilos globales para controles ----
        self.setStyleSheet(f"""
            QLineEdit {{
                background-color: {self.input_bg_color.name()};
                color: {self.fg_color.name()};
                border: 1px solid {self.border_color.name()};
                border-radius: 6px;
                padding: 10px 12px;
                font-size: 14px;
                selection-background-color: {self.primary_color.name()};
            }}
            QLineEdit:focus {{
                border: 1px solid {self.primary_color.name()};
            }}
            
            QCheckBox {{
                color: {self.fg_color.name()};
                font-size: 14px;
                spacing: 8px;
            }}
            QCheckBox::indicator {{
                width: 20px;
                height: 20px;
                border-radius: 4px;
                border: 1px solid {self.border_color.name()};
            }}
            QCheckBox::indicator:checked {{
                background-color: {self.primary_color.name()};
                border: 1px solid {self.primary_color.name()};
            }}
            QCheckBox::indicator:unchecked {{
                background-color: {self.input_bg_color.name()};
            }}
        """)
        
        # ---- Formulario de entrada ----
        # Contract Input
        contract_layout = QVBoxLayout()
        contract_layout.setSpacing(8)
        contract_label = QLabel("Contract")
        contract_label.setStyleSheet(f"color: {self.fg_color.name()}; font-size: 14px; font-weight: 500;")
        self.contract_input = QLineEdit()
        self.contract_input.setPlaceholderText("Enter contract number")
        self.contract_input.setMinimumHeight(40)
        contract_layout.addWidget(contract_label)
        contract_layout.addWidget(self.contract_input)
        card_layout.addLayout(contract_layout)
        
        # Audit File Input
        audit_file_layout = QVBoxLayout()
        audit_file_layout.setSpacing(8)
        self.audit_file_info_widget = FileInfoWidget()
        card_layout.addWidget(self.audit_file_info_widget)
        audit_label = QLabel("Audit File")
        audit_label.setStyleSheet(f"color: {self.fg_color.name()}; font-size: 14px; font-weight: 500;")
        audit_file_row = QHBoxLayout()
        audit_file_row.setSpacing(10)
        self.audit_file_input = QLineEdit()
        self.audit_file_input.setPlaceholderText("Select audit file")
        self.audit_file_input.setMinimumHeight(40)
        
        audit_browse = QPushButton("Browse")
        audit_browse.setMinimumHeight(40)
        audit_browse.setStyleSheet(f"""
            QPushButton {{
                background-color: {self.primary_color.name()};
                color: white;
                border-radius: 6px;
                padding: 5px 15px;
                font-weight: 500;
                font-size: 14px;
            }}
            QPushButton:hover {{
                background-color: {QColor(self.primary_color).lighter(110).name()};
            }}
            QPushButton:pressed {{
                background-color: {QColor(self.primary_color).darker(110).name()};
            }}
        """)
        audit_browse.clicked.connect(self.browse_audit_file)
        
        audit_file_row.addWidget(self.audit_file_input)
        audit_file_row.addWidget(audit_browse)
        audit_file_layout.addWidget(audit_label)
        audit_file_layout.addLayout(audit_file_row)
        card_layout.addLayout(audit_file_layout)
        
        
        # Inventory Validation
        self.use_inventory_check = QCheckBox("Use Inventory Validation")
        self.use_inventory_check.setChecked(True)
        self.use_inventory_check.clicked.connect(self.toggle_inventory_input)
        card_layout.addWidget(self.use_inventory_check)
        
        # Inventory File Input - MODIFICADO: Usar un contenedor para poder ocultarlo
        self.inventory_container = QWidget()
        inventory_container_layout = QVBoxLayout(self.inventory_container)  # PRIMERO crea el layout
        inventory_container_layout.setContentsMargins(0, 0, 0, 0)
        inventory_container_layout.setSpacing(8)
        self.inventory_file_info_widget = FileInfoWidget()
        inventory_container_layout.addWidget(self.inventory_file_info_widget)
        self.inventory_label = QLabel("Inventory File")
        self.inventory_label.setStyleSheet(f"color: {self.fg_color.name()}; font-size: 14px; font-weight: 500;")
        inventory_file_row = QHBoxLayout()
        inventory_file_row.setSpacing(10)
        self.inventory_file_input = QLineEdit()
        self.inventory_file_input.setPlaceholderText("Select inventory file")
        self.inventory_file_input.setMinimumHeight(40)
        
        inventory_browse = QPushButton("Browse")
        inventory_browse.setMinimumHeight(40)
        inventory_browse.setStyleSheet(f"""
            QPushButton {{
                background-color: {self.primary_color.name()};
                color: white;
                border-radius: 6px;
                padding: 5px 15px;
                font-weight: 500;
                font-size: 14px;
            }}
            QPushButton:hover {{
                background-color: {QColor(self.primary_color).lighter(110).name()};
            }}
            QPushButton:pressed {{
                background-color: {QColor(self.primary_color).darker(110).name()};
            }}
        """)
        inventory_browse.clicked.connect(self.browse_inventory_file)
        
        inventory_file_row.addWidget(self.inventory_file_input)
        inventory_file_row.addWidget(inventory_browse)
        
        inventory_container_layout.addWidget(self.inventory_label)
        inventory_container_layout.addLayout(inventory_file_row)
        
        card_layout.addWidget(self.inventory_container)
        
        # Progress Area 
        progress_section = QVBoxLayout()
        progress_section.setSpacing(10)
        
        # Progress Bar estilizada
        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setFixedHeight(8)
        self.progress_bar.setStyleSheet(f"""
            QProgressBar {{
                background-color: {QColor(self.card_bg_color).darker(110).name()};
                border: none;
                border-radius: 4px;
            }}
            QProgressBar::chunk {{
                background-color: {self.accent_color.name()};
                border-radius: 4px;
            }}
        """)
        
        # Status indicator
        status_layout = QHBoxLayout()
        self.status_icon = QLabel()
        # Inicialmente podríamos poner un icono de información
        # self.status_icon.setPixmap(QIcon("info_icon.png").pixmap(16, 16))
        self.status_icon.setFixedSize(16, 16)
        
        self.status_label = QLabel("Ready to process")
        self.status_label.setStyleSheet(f"color: {self.fg_color.name()}; font-size: 13px;")
        
        status_layout.addWidget(self.status_icon)
        status_layout.addWidget(self.status_label)
        status_layout.addStretch()
        
        progress_section.addWidget(self.progress_bar)
        progress_section.addLayout(status_layout)
        card_layout.addLayout(progress_section)
        
        # Spacer para empujar los botones hacia abajo
        spacer_item = QSpacerItem(20, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)
        card_layout.addItem(spacer_item)
        
        # ---- Botones de acción ----
        button_layout = QHBoxLayout()
        button_layout.setSpacing(15)
        
        start_audit = QPushButton("Start Audit")
        start_audit.setMinimumHeight(45)
        start_audit.setStyleSheet(f"""
            QPushButton {{
                background-color: {self.primary_color.name()};
                color: white;
                border-radius: 6px;
                padding: 10px 25px;
                font-weight: 600;
                font-size: 15px;
            }}
            QPushButton:hover {{
                background-color: {QColor(self.primary_color).lighter(110).name()};
            }}
            QPushButton:pressed {{
                background-color: {QColor(self.primary_color).darker(110).name()};
            }}
        """)
        start_audit.clicked.connect(self.start_audit)
        
        reset_fields = QPushButton("Reset Fields")
        reset_fields.setMinimumHeight(45)
        reset_fields.setStyleSheet(f"""
            QPushButton {{
                background-color: {self.secondary_color.name()};
                color: white;
                border-radius: 6px;
                padding: 10px 25px;
                font-weight: 600;
                font-size: 15px;
            }}
            QPushButton:hover {{
                background-color: {QColor(self.secondary_color).lighter(110).name()};
            }}
            QPushButton:pressed {{
                background-color: {QColor(self.secondary_color).darker(110).name()};
            }}
        """)
        reset_fields.clicked.connect(self.reset_fields)
        
        button_layout.addWidget(start_audit)
        button_layout.addWidget(reset_fields)
        card_layout.addLayout(button_layout)
        
        # ---- Integrar componentes en layout principal ----
        main_layout.addWidget(title_bar)
        main_layout.addWidget(content_card, 1)  # '1' significa que toma todo el espacio disponible
        
        # Establecer el widget principal
        self.setCentralWidget(main_container)
        
        # Comprobar estado inicial de checkbox
        self.toggle_inventory_input()
    
    def toggle_inventory_input(self):
        """Muestra u oculta completamente el contenedor de inventario según el estado del checkbox"""
        enabled = self.use_inventory_check.isChecked()
        self.inventory_container.setVisible(enabled)
        
        # Ajustar la altura de la ventana para evitar espacio vacío
        # self.adjustSize() -> No usar, puede causar problemas con el redimensionamiento
    
    def browse_audit_file(self):
        """Abre diálogo para seleccionar archivo de auditoría"""
        filename, _ = QFileDialog.getOpenFileName(
            self, 
            "Select Audit File", 
            "", 
            "Excel Files (*.xlsx);;All Files (*)"
        )
        if filename:
            self.audit_file_input.setText(filename)
            # Actualizar información del archivo
            self.audit_file_info_widget.update_info(filename)

    def browse_inventory_file(self):
        """Abre diálogo para seleccionar archivo de inventario"""
        filename, _ = QFileDialog.getOpenFileName(
            self, 
            "Select Inventory File", 
            "", 
            "Excel Files (*.xlsx);;All Files (*)"
        )
        if filename:
            self.inventory_file_input.setText(filename)
            # Actualizar información del archivo (si tienes el widget)
            if hasattr(self, 'inventory_file_info_widget'):
                self.inventory_file_info_widget.update_info(filename)
    
    def start_audit(self):
        """Inicia el proceso de auditoría con seguimiento de tiempo"""
        contract = self.contract_input.text().strip()
        audit_file = self.audit_file_input.text().strip()
        inventory_file = self.inventory_file_input.text().strip() if self.use_inventory_check.isChecked() else None

        # Validar campos requeridos
        if not contract or not audit_file:
            self.status_label.setText("Please complete all required fields")
            return

        # Validación adicional para archivo de inventario si está habilitado
        if self.use_inventory_check.isChecked() and not inventory_file:
            self.status_label.setText("Inventory file is required when inventory validation is enabled")
            return

        # Verificar existencia de archivos
        if not Path(audit_file).exists():
            self.status_label.setText(f"Audit file not found: {audit_file}")
            return
            
        if inventory_file and not Path(inventory_file).exists():
            self.status_label.setText(f"Inventory file not found: {inventory_file}")
            return

        # Preparar y comenzar worker thread
        self.worker = AuditWorker(
            self.controller,
            contract,
            audit_file,
            inventory_file,
            self.use_inventory_check.isChecked()
        )
        
        # Conectar señales
        self.worker.progress_update.connect(self.update_progress)
        self.worker.audit_completed.connect(self.handle_audit_completed)
        self.worker.audit_error.connect(self.handle_audit_error)
        
        # Iniciar contador de tiempo
        self.elapsed_seconds = 0
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_timer)
        self.timer.start(1000)  # Actualizar cada segundo
        
        # Deshabilitar campos durante procesamiento
        self.setInputsEnabled(False)
        self.worker.start()
            
    def update_progress(self, value, message, rows_processed=0):
        """Actualiza la barra de progreso y el mensaje de estado"""
        self.progress_bar.setValue(value)
        self.status_label.setText(message)
    
        # Actualizar la interfaz para mostrar cambios inmediatamente
        QApplication.processEvents()

    def update_timer(self):
        """Actualiza el contador de tiempo transcurrido"""
        self.elapsed_seconds += 1
        minutes, seconds = divmod(self.elapsed_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        
        if hours > 0:
            time_str = f"{hours}h {minutes:02d}m {seconds:02d}s"
        else:
            time_str = f"{minutes:02d}m {seconds:02d}s"
        
        # Añadir tiempo al mensaje de estado actual
        current_text = self.status_label.text()
        if " - Time: " in current_text:
            updated_text = current_text.split(" - Time: ")[0] + f" - Time: {time_str}"
        else:
            updated_text = current_text + f" - Time: {time_str}"
        
        self.status_label.setText(updated_text)

    def handle_audit_completed(self, result):
        """Maneja finalización deteniendo timer y actualizando estado"""
        # Detener timer
        if hasattr(self, 'timer') and self.timer.isActive():
            self.timer.stop()
        
        # Actualizar UI
        self.setInputsEnabled(True)
        
        if result['status'] == 'success':
            # Finalizar barra de progreso a 100%
            self.progress_bar.setValue(100)
            
            # Actualizar mensaje con información de tiempo
            minutes, seconds = divmod(self.elapsed_seconds, 60)
            hours, minutes = divmod(minutes, 60)
            
            if hours > 0:
                time_str = f"{hours}h {minutes:02d}m {seconds:02d}s"
            else:
                time_str = f"{minutes:02d}m {seconds:02d}s"
                
            self.status_label.setText(f"Audit completed successfully in {time_str}!")
            
            # Crear mensaje emergente
            msg_box = QMessageBox(self)
            msg_box.setWindowTitle("Audit Completed")
            msg_box.setText(f"Audit completed successfully in {time_str}!")
            msg_box.setInformativeText("Would you like to open the reports folder?")
            msg_box.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            msg_box.setDefaultButton(QMessageBox.StandardButton.Yes)
            msg_box.setIcon(QMessageBox.Icon.Information)
            
            # Personalizar colores del mensaje
            msg_box.setStyleSheet(f"""
                QMessageBox {{
                    background-color: {self.card_bg_color.name()};
                }}
                QLabel {{
                    color: {self.fg_color.name()};
                }}
                QPushButton {{
                    background-color: {self.primary_color.name()};
                    color: white;
                    border-radius: 4px;
                    padding: 6px 12px;
                    font-weight: 500;
                }}
                QPushButton:hover {{
                    background-color: {QColor(self.primary_color).lighter(110).name()};
                }}
            """)
            
            # Mostrar el mensaje y procesar respuesta
            response = msg_box.exec()
            
            # Si usuario quiere abrir la carpeta
            if response == QMessageBox.StandardButton.Yes:
                reports_path = Path('reports')
                if reports_path.exists():
                    os.startfile(str(reports_path))
        else:
            # Manejar error
            error_msg = result.get('message', 'Unknown error')
            self.status_label.setText(f"Audit failed: {error_msg}")
            
            # Mensaje emergente para error
            error_box = QMessageBox(self)
            error_box.setWindowTitle("Audit Failed")
            error_box.setText(f"Audit failed: {error_msg}")
            error_box.setIcon(QMessageBox.Icon.Warning)
            error_box.setStyleSheet(f"""
                QMessageBox {{
                    background-color: {self.card_bg_color.name()};
                }}
                QLabel {{
                    color: {self.fg_color.name()};
                }}
                QPushButton {{
                    background-color: {self.primary_color.name()};
                    color: white;
                    border-radius: 4px;
                    padding: 6px 12px;
                    font-weight: 500;
                }}
            """)
            error_box.exec()

    def handle_audit_error(self, error):
        """Maneja errores deteniendo el timer y actualizando estado"""
        # Detener timer
        if hasattr(self, 'timer') and self.timer.isActive():
            self.timer.stop()
            
        # Actualizar UI
        self.setInputsEnabled(True)
        self.status_label.setText(f"Error: {str(error)}")
        
        # Registro detallado del error
        print(traceback.format_exc())

    def reset_fields(self):
        """Limpia todos los campos y restablece el estado inicial"""
        self.contract_input.clear()
        self.audit_file_input.clear()
        self.inventory_file_input.clear()
        self.progress_bar.setValue(0)
        self.status_label.setText("Ready to process")
        self.use_inventory_check.setChecked(True)
        
        # Activar controles de inventario
        self.toggle_inventory_input()

    def setInputsEnabled(self, enabled):
        """Habilita/deshabilita los campos de entrada durante el procesamiento"""
        self.contract_input.setEnabled(enabled)
        self.audit_file_input.setEnabled(enabled)
        self.use_inventory_check.setEnabled(enabled)
        
        # Asegurarse que el campo de inventario refleje tanto el estado general como el del checkbox
        if enabled:
            self.inventory_file_input.setEnabled(self.use_inventory_check.isChecked())
        else:
            self.inventory_file_input.setEnabled(False)

    def run(self):
        """Muestra la ventana"""
        self.show()
        
    # Funciones para permitir redimensionamiento de la ventana
    def mousePressEvent(self, event):
        """Maneja el evento de presionar el mouse para mover/redimensionar la ventana"""
        if self.isInResizeArea(event.position().x(), event.position().y()):
            self.resize_mode = True
            self.resize_start_position = event.globalPosition().toPoint()
            self.resize_start_size = QSize(self.width(), self.height())
            # Cursor de redimensionado
            self.setCursor(Qt.CursorShape.SizeFDiagCursor)
        # Mover la ventana (cuando se hace clic en la barra de título)
        elif event.button() == Qt.MouseButton.LeftButton and event.position().y() < 40:
            self.old_pos = event.globalPosition().toPoint()
            self.move_mode = True
        else:
            self.resize_mode = False
            self.move_mode = False
    
    def mouseMoveEvent(self, event):
        """Maneja el movimiento del mouse para redimensionar o mover la ventana"""
        # Redimensionar la ventana
        if hasattr(self, 'resize_mode') and self.resize_mode:
            delta = event.globalPosition().toPoint() - self.resize_start_position
            new_width = self.resize_start_size.width() + delta.x()
            new_height = self.resize_start_size.height() + delta.y()
            
            # Respetar tamaños mínimos y máximos
            new_width = max(min(new_width, self.maximumWidth()), self.minimumWidth())
            new_height = max(min(new_height, self.maximumHeight()), self.minimumHeight())
            
            self.resize(new_width, new_height)
        # Mover la ventana
        elif hasattr(self, 'move_mode') and self.move_mode and hasattr(self, 'old_pos'):
            delta = event.globalPosition().toPoint() - self.old_pos
            self.move(self.x() + delta.x(), self.y() + delta.y())
            self.old_pos = event.globalPosition().toPoint()
        else:
            # Cambiar el cursor cuando esté sobre el área de redimensionado
            if self.isInResizeArea(event.position().x(), event.position().y()):
                self.setCursor(Qt.CursorShape.SizeFDiagCursor)
            else:
                self.setCursor(Qt.CursorShape.ArrowCursor)
    
    def mouseReleaseEvent(self, event):
        """Maneja la liberación del mouse después de redimensionar/mover"""
        self.resize_mode = False
        self.move_mode = False
        self.setCursor(Qt.CursorShape.ArrowCursor)
    
    def isInResizeArea(self, x, y):
        """Determina si el cursor está en el área de redimensionamiento (esquina inferior derecha)"""
        resize_border = 15  # Ancho en píxeles del borde para redimensionar
        return (self.width() - resize_border < x < self.width() and 
                self.height() - resize_border < y < self.height())
        
    def isInBorderArea(self, x, y):
        """Determina si el cursor está en cualquier borde para redimensionar"""
        border = 5  # Ancho del borde sensible para redimensionamiento
        # Borde izquierdo
        if 0 <= x <= border:
            return "left"
        # Borde derecho
        elif self.width() - border <= x <= self.width():
            return "right"
        # Borde superior
        elif 0 <= y <= border:
            return "top"
        # Borde inferior
        elif self.height() - border <= y <= self.height():
            return "bottom"
        return None