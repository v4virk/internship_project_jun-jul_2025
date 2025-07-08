import sys
import subprocess
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTextEdit, QMenu, QToolButton, QLabel
)
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QFont, QMovie


class LoadingScreen(QWidget):
    def __init__(self, message="Loading..."):
        super().__init__()
        self.setWindowFlags(Qt.Dialog | Qt.FramelessWindowHint)
        self.setFixedSize(300, 150)

        layout = QVBoxLayout()
        layout.setAlignment(Qt.AlignCenter)

        self.label = QLabel(message)
        self.label.setAlignment(Qt.AlignCenter)
        self.label.setFont(QFont("Segoe UI", 14))

        self.spinner = QLabel()
        self.spinner.setAlignment(Qt.AlignCenter)
        movie = QMovie("Snake.gif")
        self.spinner.setMovie(movie)
        movie.start()

        layout.addWidget(self.spinner)
        layout.addSpacing(10)
        layout.addWidget(self.label)
        self.setLayout(layout)

        self.setStyleSheet("background-color: white; color: black;")


class AnalyticsWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Analytics Window")
        self.resize(600, 400)
        layout = QVBoxLayout()
        layout.addWidget(QLabel("Analytics content goes here..."))
        self.setLayout(layout)


class ReadFileWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Read File Window")
        self.resize(600, 400)
        layout = QVBoxLayout()
        layout.addWidget(QLabel("Read file content goes here..."))
        self.setLayout(layout)


class FileMonitoringWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("File Monitoring Window")
        self.resize(600, 400)
        layout = QVBoxLayout()
        layout.addWidget(QLabel("File monitoring content goes here..."))
        self.setLayout(layout)


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Kafka Event Frontend")
        self.resize(2000, 1000)
        self.processes = []

        # === Layouts ===
        main_layout = QVBoxLayout()

        # Title
        title_label = QLabel("Real-Time E-Commerce Monitoring System")
        title_label.setFont(QFont("Segoe UI", 24, QFont.Bold))
        title_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(title_label)

        # Button Row
        button_layout = QHBoxLayout()

        # Hamburger Menu
        self.menu_button = QToolButton()
        self.menu_button.setText("☰")
        self.menu_button.setPopupMode(QToolButton.InstantPopup)
        self.menu_button.setStyleSheet("""
            QToolButton::menu-indicator {
                image: none;
            }
            QToolButton {
                font-size: 24px;
                padding: 10px;
                background-color: transparent;
                border: none;
            }
        """)

        menu = QMenu()
        menu.addAction("Analytics", self.open_analytics_window)
        menu.addAction("Read File", self.open_read_file_window)
        menu.addAction("File Monitoring", self.open_monitor_window)
        self.menu_button.setMenu(menu)

        button_layout.addWidget(self.menu_button)

        # Control Buttons
        self.start_btn = QPushButton("Start")
        self.stop_btn = QPushButton("Stop")
        self.terminate_btn = QPushButton("Terminate")

        for btn in [self.start_btn, self.stop_btn, self.terminate_btn]:
            btn.setFixedSize(150, 50)
            button_layout.addWidget(btn)

        button_layout.addStretch()
        main_layout.addLayout(button_layout)

        # === Log Area ===
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setFont(QFont("Consolas", 12))
        main_layout.addWidget(self.log_output)

        self.setLayout(main_layout)

        # === Button Functions ===
        self.start_btn.clicked.connect(self.start_sessions)
        self.stop_btn.clicked.connect(self.stop_sessions)
        self.terminate_btn.clicked.connect(self.terminate_sessions)

        # Child Windows
        self.analytics_window = None
        self.read_file_window = None
        self.monitor_window = None

    def log(self, message):
        self.log_output.append(message)

    def start_sessions(self):
        if self.processes:
            self.log("Warning: Processes already running.")
            return
        try:
            for i in range(5):
                process = subprocess.Popen(
                    ["start", "cmd", "/k", "python producer.py"],
                    shell=True
                )
                self.processes.append(process)
            self.log("Started 5 terminal sessions.")
        except FileNotFoundError as e:
            self.log(f"Error: {e}")

    def stop_sessions(self):
        self.log("Stop pressed. (Pausing not implemented)")

    def terminate_sessions(self):
        for proc in self.processes:
            proc.terminate()
        self.processes.clear()
        self.log("Terminated all processes.")

    def open_analytics_window(self):
        self._open_window(AnalyticsWindow, 'analytics_window', "Loading analytics...")

    def open_read_file_window(self):
        self._open_window(ReadFileWindow, 'read_file_window', "Reading file...")

    def open_monitor_window(self):
        self._open_window(FileMonitoringWindow, 'monitor_window', "Starting file monitor...")

    def _open_window(self, window_class, attr_name, message):
        loading = LoadingScreen(message)
        loading.show()
        QTimer.singleShot(2000, lambda: self._show_window(window_class, attr_name, loading))

    def _show_window(self, window_class, attr_name, loading_widget):
        loading_widget.close()
        window = window_class()
        setattr(self, attr_name, window)
        window.show()
        self.log(f"Opened {window.windowTitle()}.")


# === Startup Animation with Spinner ===
app = QApplication(sys.argv)

startup_loading = QWidget()
startup_loading.setWindowFlags(Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint)
startup_loading.resize(2000, 1000)
startup_loading.setStyleSheet("background-color: white; color: black;")
startup_loading.setCursor(Qt.WaitCursor)

startup_layout = QVBoxLayout()

header_label = QLabel("Real-Time E-Commerce Monitoring System")
header_label.setFont(QFont("Segoe UI", 32, QFont.Bold))
header_label.setAlignment(Qt.AlignCenter)

spinner_label = QLabel()
spinner_movie = QMovie("Snake.gif")
spinner_label.setMovie(spinner_movie)
spinner_label.setAlignment(Qt.AlignCenter)
spinner_movie.start()

startup_layout.addStretch()
startup_layout.addWidget(header_label)
startup_layout.addSpacing(20)
startup_layout.addWidget(spinner_label)
startup_layout.addStretch()

startup_loading.setLayout(startup_layout)
startup_loading.show()

# Show main window after delay
window = MainWindow()
def show_main():
    startup_loading.close()
    window.show()

QTimer.singleShot(2000, show_main)

sys.exit(app.exec_())
