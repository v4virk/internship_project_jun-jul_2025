import sys
import subprocess
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTextEdit, QMenu, QToolButton, QLabel
)
from PyQt5.QtCore import Qt


# === Child Windows ===

class AnalyticsWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Analytics")
        self.resize(600, 400)
        layout = QVBoxLayout()
        label = QLabel("Analytics content goes here...")
        layout.addWidget(label)
        self.setLayout(layout)


class ReadFileWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Read File ")
        self.resize(600, 400)
        layout = QVBoxLayout()
        label = QLabel("Read file content goes here...")
        layout.addWidget(label)
        self.setLayout(layout)


class FileMonitoringWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("File Monitoring ")
        self.resize(600, 400)
        layout = QVBoxLayout()
        label = QLabel("File monitoring content goes here...")
        layout.addWidget(label)
        self.setLayout(layout)




class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Kafka Event Frontend")
        self.resize(2000, 1000)

        self.processes = []

        # === Layouts ===
        main_layout = QVBoxLayout()
        top_layout = QHBoxLayout()

        # === Hamburger Menu ===
        self.menu_button = QToolButton()
        self.menu_button.setText("☰")
        self.menu_button.setPopupMode(QToolButton.InstantPopup)
        self.menu_button.setStyleSheet("""
            QToolButton {
                font-size: 24px;
                padding: 5px 15px;
            }
            QToolButton::menu-indicator {
                image: none;
            }
        """)

        menu = QMenu()
        menu.addAction("Analytics", self.open_analytics_window)
        menu.addAction("Read File", self.open_read_file_window)
        menu.addAction("File Monitoring", self.open_monitor_window)
        self.menu_button.setMenu(menu)

        top_layout.addWidget(self.menu_button)

        # === Control Buttons ===
        self.start_btn = QPushButton("Start")
        self.stop_btn = QPushButton("Stop")
        self.terminate_btn = QPushButton("Terminate")

        for btn in [self.start_btn, self.stop_btn, self.terminate_btn]:
            btn.setFixedSize(200, 50)
            top_layout.addWidget(btn)

        top_layout.addStretch()
        main_layout.addLayout(top_layout)

        # === Log Window ===
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet("font-size: 16px;")
        main_layout.addWidget(self.log_output)

        self.setLayout(main_layout)

        # === Connect Buttons ===
        self.start_btn.clicked.connect(self.start_sessions)
        self.stop_btn.clicked.connect(self.stop_sessions)
        self.terminate_btn.clicked.connect(self.terminate_sessions)

        # === Placeholder windows (prevent garbage collection) ===
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

    # === Menu Actions ===
    def open_analytics_window(self):
        self.analytics_window = AnalyticsWindow()
        self.analytics_window.show()
        self.log("Opened Analytics window.")

    def open_read_file_window(self):
        self.read_file_window = ReadFileWindow()
        self.read_file_window.show()
        self.log("Opened Read File window.")

    def open_monitor_window(self):
        self.monitor_window = FileMonitoringWindow()
        self.monitor_window.show()
        self.log("Opened File Monitoring window.")


# === Run App ===
app = QApplication(sys.argv)
window = MainWindow()
window.show()
sys.exit(app.exec_())
