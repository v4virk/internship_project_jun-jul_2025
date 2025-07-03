import sys
import subprocess
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTextEdit
)
from PyQt5.QtCore import Qt


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Kafka Event Frontend")
        self.resize(2500, 1000)

        self.processes = []

        # Main vertical layout
        main_layout = QVBoxLayout()

        # Button layout (top row)
        button_layout = QHBoxLayout()
        self.start_btn = QPushButton("Start")
        self.stop_btn = QPushButton("Stop")
        self.terminate_btn = QPushButton("Terminate")
        self.analytics_btn = QPushButton("Analytics")

        for btn in [self.start_btn, self.stop_btn, self.terminate_btn, self.analytics_btn]:
            btn.setFixedSize(200, 50)
            button_layout.addWidget(btn)

        main_layout.addLayout(button_layout)

        # Log window (below buttons)
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet("font-size: 16px;")
        main_layout.addWidget(self.log_output)

        self.setLayout(main_layout)

        # Connect button actions
        self.start_btn.clicked.connect(self.start_sessions)
        self.stop_btn.clicked.connect(self.stop_sessions)
        self.terminate_btn.clicked.connect(self.terminate_sessions)
        self.analytics_btn.clicked.connect(self.show_analytics)

    def log(self, message):
        self.log_output.append(message)

    def start_sessions(self):
        if self.processes:
            self.log("⚠️ Processes already running.")
            return
        try:
            for i in range(5):
                process = subprocess.Popen(
                    ["start", "cmd", "/k", "python producer.py"],
                    shell=True
                )
                self.processes.append(process)
            self.log(" Started 5 terminal sessions.")
        except FileNotFoundError as e:
            self.log(f" Error: {e}")

    def stop_sessions(self):
        self.log("⏸ Stop pressed. (Pausing not implemented)")

    def terminate_sessions(self):
        for proc in self.processes:
            proc.terminate()
        self.processes.clear()
        self.log(" Terminated all processes.")

    def show_analytics(self):
        self.log(" Opening analytics window... (not yet implemented)")


# Run the application
app = QApplication(sys.argv)
window = MainWindow()
window.show()
sys.exit(app.exec_())