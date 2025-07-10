import sys
import subprocess
import threading
import os
import time
import json
import webbrowser
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTextEdit, QLabel, QFrame
)
from PyQt5.QtCore import Qt, QTimer, QFileSystemWatcher, QEvent, pyqtSignal
from PyQt5.QtGui import QFont, QMovie

# Virtual environment configuration
VENV_PATH = "/Users/harvijaysingh/pyspark-env"
VENV_ACTIVATE = f"{VENV_PATH}/bin/activate"

# === Custom Event for Thread-Safe Text Appending ===
class AppendTextEvent(QEvent):
    EVENT_TYPE = QEvent.Type(QEvent.registerEventType())
    def __init__(self, text):
        super().__init__(AppendTextEvent.EVENT_TYPE)
        self.text = text

class LogTextEdit(QTextEdit):
    def event(self, event):
        if event.type() == AppendTextEvent.EVENT_TYPE:
            self.append(event.text)
            return True
        return super().event(event)

# === Loading Screen Class ===
class LoadingScreen(QWidget):
    def __init__(self, message="Loading..."):
        super().__init__()
        self.setWindowFlags(Qt.Dialog | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint)
        self.setFixedSize(300, 150)
        self.setWindowTitle("Loading...")
        self.setWindowModality(Qt.ApplicationModal)

        layout = QVBoxLayout()
        layout.setAlignment(Qt.AlignCenter)

        self.label = QLabel(message)
        self.label.setAlignment(Qt.AlignCenter)
        self.label.setFont(QFont("Segoe UI", 14))

        layout.addWidget(self.label)
        self.setLayout(layout)

        self.setStyleSheet("""
            QWidget {
                background-color: #2c2c2c;
                color: #eee;
                border: none;
                border-radius: 10px;
            }
            QLabel {
                color: #00BFFF;
                font-weight: bold;
            }
        """)

# === Child Windows ===
class FileMonitoringWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("File Monitoring")
        self.resize(800, 600)
        
        self.file_path = "/Users/harvijaysingh/events_log.json"
        
        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)
        self.text_edit.setStyleSheet("""
            font-family: monospace; 
            font-size: 12px;
            background-color: #333;
            color: #eee;
            border: 1px solid #555;
        """)
        
        self.refresh_btn = QPushButton("Manual Refresh")
        self.refresh_btn.setStyleSheet("""
            QPushButton {
                background-color: #505050;
                color: white;
                border: 1px solid #666;
                padding: 8px 15px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #606060;
            }
        """)
        self.refresh_btn.clicked.connect(self.update_file_content)
        
        layout = QVBoxLayout()
        layout.addWidget(self.text_edit)
        
        button_layout = QHBoxLayout()
        button_layout.addWidget(self.refresh_btn)
        button_layout.addStretch()
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
        
        self.file_watcher = QFileSystemWatcher()
        if os.path.exists(self.file_path):
            self.file_watcher.addPath(self.file_path)
        self.file_watcher.fileChanged.connect(self.update_file_content)
        
        self.update_file_content()
        
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_file_content)
        self.timer.start(1000)

    def update_file_content(self):
        try:
            if not os.path.exists(self.file_path):
                self.text_edit.setPlainText(f"Waiting for file: {self.file_path}")
                if self.file_path not in self.file_watcher.files():
                    if os.path.exists(self.file_path):
                        self.file_watcher.addPath(self.file_path)
                return
                
            with open(self.file_path, 'r') as file:
                content = file.read()
                if not content.strip():
                    self.text_edit.setPlainText("File is empty")
                    return
                    
                try:
                    data = json.loads(content)
                    formatted_json = json.dumps(data, indent=2)
                    
                    scrollbar = self.text_edit.verticalScrollBar()
                    was_at_bottom = scrollbar.value() == scrollbar.maximum()
                    
                    self.text_edit.setPlainText(formatted_json)
                    
                    if was_at_bottom:
                        cursor = self.text_edit.textCursor()
                        cursor.movePosition(cursor.End)
                        self.text_edit.setTextCursor(cursor)
                        self.text_edit.ensureCursorVisible()
                    
                except json.JSONDecodeError:
                    self.text_edit.setPlainText("Waiting for valid JSON data...\nCurrent content:\n" + content)
                    
        except Exception as e:
            self.text_edit.setPlainText(f"Error reading file: {str(e)}")

    def closeEvent(self, event):
        if self.file_path in self.file_watcher.files():
            self.file_watcher.removePath(self.file_path)
        self.timer.stop()
        super().closeEvent(event)

class AnalyticsWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Analytics Window")
        self.resize(600, 400)
        layout = QVBoxLayout()
        layout.addWidget(QLabel("Analytics content goes here..."))
        self.setLayout(layout)

class SystemLogsWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("System Logs Window")
        self.resize(600, 400)
        layout = QVBoxLayout()
        layout.addWidget(QLabel("System logs content goes here..."))
        self.setLayout(layout)

class PortsWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Service Ports")
        self.resize(400, 300)
        
        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)
        
        title = QLabel("Service Ports")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #00BFFF;")
        layout.addWidget(title)
        
        # Service buttons with ports
        services = [
            ("HDFS NameNode", "http://localhost:9870/dfshealth.html#tab-overview"),
            ("YARN ResourceManager", "http://localhost:8088/cluster"),
            ("DataNode UI", "http://localhost:9868/status.html"),
            ("Datadog", "http://localhost:5002")
        ]
        
        for name, url in services:
            btn = QPushButton(name)
            btn.setStyleSheet("""
                QPushButton {
                    padding: 10px;
                    text-align: left;
                    background-color: #3a3a3a;
                    color: #eee;
                    border: 1px solid #444;
                    border-radius: 5px;
                }
                QPushButton:hover {
                    background-color: #4a4a4a;
                }
            """)
            btn.clicked.connect(lambda _, u=url: self.open_service(u))
            layout.addWidget(btn)
        
        self.status_label = QLabel("Click a service to open in browser")
        self.status_label.setStyleSheet("color: #aaa;")
        layout.addWidget(self.status_label)
        
        layout.addStretch()
        self.setLayout(layout)
    
    def open_service(self, url):
        try:
            webbrowser.open(url)
            self.status_label.setText(f"Opened: {url}")
            self.status_label.setStyleSheet("color: #00FF00;")
            QTimer.singleShot(3000, lambda: self.status_label.setText("Click a service to open in browser"))
        except Exception as e:
            self.status_label.setText(f"Error: {str(e)}")
            self.status_label.setStyleSheet("color: #FF0000;")
            QTimer.singleShot(3000, lambda: self.status_label.setText("Click a service to open in browser"))

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Real-Time E-commerce Website Analytics Dashboard")
        self.resize(2000, 1000)
        
        self.setStyleSheet("""
            QWidget {
                background-color: #222;
                color: #eee;
                font-family: Arial, sans-serif;
            }
            QLabel#mainTitle {
                font-size: 28px;
                font-weight: bold;
                color: #00BFFF;
                margin-bottom: 10px;
            }
            QPushButton {
                background-color: #007BFF;
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 5px;
                font-size: 16px;
            }
            QPushButton:hover {
                background-color: #0056b3;
            }
            QPushButton#terminateButton {
                background-color: #DC3545;
            }
            QPushButton#terminateButton:hover {
                background-color: #C82333;
            }
            QTextEdit {
                background-color: #333;
                color: #FFFFFF;
                border: 1px solid #555;
                padding: 10px;
                font-family: "Courier New", Courier, monospace;
                font-size: 14px;
            }
            QFrame#sidebar {
                background-color: #2c2c2c;
                border-right: 1px solid #444;
            }
            QPushButton[class="sidebar_item"] {
                background-color: transparent;
                color: #bbb;
                border-left: 4px solid transparent;
                padding: 12px 10px;
                font-size: 16px;
                text-align: left;
                font-weight: 500;
                font-family: "Segoe UI", sans-serif;
            }
            QPushButton[class="sidebar_item"]:hover {
                background-color: #2e2e2e;
                color: #00BFFF;
                border-left: 4px solid #00BFFF;
            }
        """)

        self.processes = []
        self.terminal_processes = []
        self.services_running = False

        # Main Layout
        main_layout = QHBoxLayout()
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # Left Sidebar
        sidebar_frame = QFrame()
        sidebar_frame.setObjectName("sidebar")
        sidebar_layout = QVBoxLayout(sidebar_frame)
        sidebar_layout.setContentsMargins(10, 20, 10, 20)
        sidebar_layout.setSpacing(15)
        sidebar_frame.setFixedWidth(200)

        controls_label = QLabel("Controls")
        controls_label.setStyleSheet("font-size: 18px; font-weight: bold; color: #eee; padding-left: 5px; background-color: transparent;")
        sidebar_layout.addWidget(controls_label)
        
        self.analytics_btn = QPushButton("Analytics")
        self.analytics_btn.setProperty("class", "sidebar_item")
        self.analytics_btn.clicked.connect(self.open_analytics_window)
        sidebar_layout.addWidget(self.analytics_btn)

        self.file_monitor_btn = QPushButton("Read logs")
        self.file_monitor_btn.setProperty("class", "sidebar_item")
        self.file_monitor_btn.clicked.connect(self.open_monitor_window)
        sidebar_layout.addWidget(self.file_monitor_btn)
        
        self.system_btn = QPushButton("System monitoring")
        self.system_btn.setProperty("class", "sidebar_item")
        self.system_btn.clicked.connect(self.open_system_monitoring) 
        sidebar_layout.addWidget(self.system_btn)

        self.ports_btn = QPushButton("Ports")
        self.ports_btn.setProperty("class", "sidebar_item")
        self.ports_btn.clicked.connect(self.open_ports_window)
        sidebar_layout.addWidget(self.ports_btn)
        
        sidebar_layout.addStretch(1)
        main_layout.addWidget(sidebar_frame)

        # Right Content Area
        right_content_layout = QVBoxLayout()
        right_content_layout.setContentsMargins(20, 20, 20, 20)
        right_content_layout.setSpacing(15)

        # Top Bar
        top_bar_layout = QHBoxLayout()
        
        main_title_label = QLabel("E-COMMERCE ANALYTICS")
        main_title_label.setObjectName("mainTitle")
        top_bar_layout.addWidget(main_title_label)
        top_bar_layout.addStretch(1)

        self.deploy_btn = QPushButton("Deploy")
        self.stop_btn = QPushButton("Stop")
        self.terminate_btn = QPushButton("Terminate")
        self.terminate_btn.setObjectName("terminateButton")

        top_bar_layout.addWidget(self.deploy_btn)
        top_bar_layout.addWidget(self.stop_btn)
        top_bar_layout.addWidget(self.terminate_btn)

        right_content_layout.addLayout(top_bar_layout)

        console_label = QLabel("System logs")
        console_label.setStyleSheet("font-size: 18px; font-weight: bold; color: #aaa; margin-top: 10px;")
        right_content_layout.addWidget(console_label)

        self.log_output = LogTextEdit()
        self.log_output.setReadOnly(True)
        right_content_layout.addWidget(self.log_output)

        main_layout.addLayout(right_content_layout)
        self.setLayout(main_layout)

        # Connect Buttons
        self.deploy_btn.clicked.connect(self.start_sessions)
        self.stop_btn.clicked.connect(self.stop_sessions)
        self.terminate_btn.clicked.connect(self.terminate_sessions)

        # Child Window Instances
        self.analytics_window = None
        self.monitor_window = None
        self.system_logs_window = None
        self.ports_window = None

        if not self.check_virtualenv():
            self.log("WARNING: Virtual environment configuration issues detected")

    def check_virtualenv(self):
        if not os.path.exists(VENV_ACTIVATE):
            self.log(f"ERROR: Virtual environment not found at {VENV_PATH}")
            return False
        
        try:
            result = subprocess.run(
                [f"{VENV_PATH}/bin/python", "--version"],
                capture_output=True, text=True
            )
            self.log(f"Virtualenv Python version: {result.stdout.strip()}")
            return True
        except Exception as e:
            self.log(f"Error checking virtualenv: {str(e)}")
            return False

    def log(self, message):
        QApplication.instance().postEvent(self.log_output, AppendTextEvent(f"[DEBUG] {message}"))

    def read_process_output(self, process, session_name):
        try:
            for line in iter(process.stdout.readline, ''):
                if line:
                    self.log(f"{session_name}: {line.strip()}")
            process.stdout.close()
        except Exception as e:
            self.log(f"Error reading output for {session_name}: {e}")

    def execute_in_terminal(self, command, terminal_number, session_name):
        self.log(f"Starting Terminal {terminal_number} ({session_name}): {command}")

        try:
            script_path = f"/tmp/terminal_script_{terminal_number}.sh"
            with open(script_path, 'w') as f:
                f.write("#!/bin/bash\n")
                f.write(f"{command}\n")
                f.write("if [ $? -ne 0 ]; then\n")
                f.write("    echo 'Error: {command} failed'\n")
                f.write("    read -p 'Press Enter to close this terminal...'\n")
                f.write("    exit 1\n")
                f.write("fi\n")
                f.write("read -p 'Press Enter to close this terminal...'\n")
            os.chmod(script_path, 0o755)
            
            proc = subprocess.Popen(
                ["osascript", "-e", f'tell app "Terminal" to do script "bash {script_path}"'],
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT, 
                text=True, 
                bufsize=1
            )
            self.terminal_processes.append(proc)
            threading.Thread(
                target=self.read_process_output, 
                args=(proc, session_name), 
                daemon=True
            ).start()
            return proc

        except Exception as e:
            self.log(f"Error in terminal execution for {session_name}: {str(e)}")
            return None

    def start_sessions(self):
        loading_daemons = LoadingScreen("Starting essential services...")
        main_rect = self.geometry()
        loading_size = loading_daemons.size()
        x = main_rect.x() + (main_rect.width() - loading_size.width()) // 2
        y = main_rect.y() + (main_rect.height() - loading_size.height()) // 2
        loading_daemons.move(x, y)
        loading_daemons.show()
        QApplication.processEvents()

        try:
            producer_running = False
            consumer_running = False
            
            try:
                ps_output = subprocess.check_output(["ps", "-ax"]).decode('utf-8')
                producer_running = any('generated_events.py' in line and 'python' in line for line in ps_output.split('\n'))
                consumer_running = any('spark_kafka_to_hdfs.py' in line and ('python' in line or 'spark-submit' in line) for line in ps_output.split('\n'))
            except Exception as e:
                self.log(f"Error checking running processes: {str(e)}")
            
            if producer_running or consumer_running:
                self.log("Warning: Producer or Consumer processes already running. Skipping startup.")
                return

            if not self.check_virtualenv():
                self.log("Cannot start sessions - virtual environment not configured properly")
                return

            jps_proc = subprocess.Popen(["jps"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            jps_output, _ = jps_proc.communicate()
            self.log(f"Current jps output:\n{jps_output}")

            running_daemons = {
                "Kafka": "Kafka" in jps_output,
                "QuorumPeerMain": "QuorumPeerMain" in jps_output,
                "NodeManager": "NodeManager" in jps_output,
                "ResourceManager": "ResourceManager" in jps_output,
                "NameNode": "NameNode" in jps_output,
                "DataNode": "DataNode" in jps_output,
                "SecondaryNameNode": "SecondaryNameNode" in jps_output,
                "ConsoleConsumer": "ConsoleConsumer" in jps_output
            }
            self.log(f"Currently running daemons: {running_daemons}\n")

            if not self.services_running:
                terminal_number = 1
                
                if not running_daemons["Kafka"]:
                    self.execute_in_terminal("spenv; brew services start kafka", terminal_number, "Kafka Service")
                    time.sleep(15)
                    terminal_number += 1

                if not any(running_daemons[key] for key in ["NameNode", "DataNode", "SecondaryNameNode"]):
                    self.execute_in_terminal("spenv; start-dfs.sh", terminal_number, "HDFS")
                    time.sleep(5)
                    terminal_number += 1

                if not any(running_daemons[key] for key in ["ResourceManager", "NodeManager"]):
                    self.execute_in_terminal("spenv; start-yarn.sh", terminal_number, "YARN")
                    time.sleep(5)
                    terminal_number += 1

                if not running_daemons["NodeManager"]:
                    self.execute_in_terminal("spenv; yarn-daemon.sh start nodemanager", terminal_number, "NodeManager")
                    time.sleep(15)
                    terminal_number += 1

                if not running_daemons["QuorumPeerMain"]:
                    self.execute_in_terminal(
                        "spenv; /opt/homebrew/Cellar/kafka/3.9.0/libexec/bin/zookeeper-server-start.sh /opt/homebrew/etc/kafka/zookeeper.properties",
                        terminal_number, "Zookeeper"
                    )
                    time.sleep(5)
                    terminal_number += 1

                if not running_daemons["ConsoleConsumer"]:
                    self.execute_in_terminal(
                        "spenv; /opt/homebrew/Cellar/kafka/3.9.0/libexec/bin/kafka-console-consumer.sh --topic test-events --bootstrap-server localhost:9092 --from-beginning",
                        terminal_number, "Kafka Console Consumer"
                    )
                    time.sleep(5)
                    terminal_number += 1

                self.services_running = True

            # Start Producer
            producer_path = "/Users/harvijaysingh/btech cse/3rd year/internship/udated/generated_events.py"
            
            if os.path.exists(producer_path):
                try:
                    script_path = "/tmp/terminal_script_producer.sh"
                    with open(script_path, 'w') as f:
                        f.write("#!/bin/bash\n")
                        f.write("clear\n")
                        f.write("echo '=== Producer Session ==='\n")
                        f.write(f"source {VENV_ACTIVATE}\n")
                        f.write("if [[ -z \"$VIRTUAL_ENV\" ]]; then\n")
                        f.write("    echo 'ERROR: Virtual environment activation failed!'\n")
                        f.write("    echo 'Current Python path: $(which python)'\n")
                        f.write("    read -p 'Press Enter to close this terminal...'\n")
                        f.write("    exit 1\n")
                        f.write("fi\n")
                        f.write("echo 'Virtual environment activated: $VIRTUAL_ENV'\n")
                        f.write("echo 'Python path: $(which python)'\n")
                        f.write(f"python '{producer_path}'\n")
                        f.write("if [ $? -ne 0 ]; then\n")
                        f.write("    echo 'Error: Producer script failed'\n")
                        f.write("    read -p 'Press Enter to close this terminal...'\n")
                        f.write("    exit 1\n")
                        f.write("fi\n")
                        f.write("read -p 'Press Enter to close this terminal...'\n")
                    os.chmod(script_path, 0o755)
                    
                    producer_proc = self.execute_in_terminal(f"bash {script_path}", 9, "Producer")
                    if producer_proc:
                        self.log("Started Producer in Terminal 9")
                        self.log(f"Using virtualenv at: {VENV_PATH}")
                    else:
                        self.log("Failed to start Producer terminal")
                        
                except Exception as e:
                    self.log(f"Error setting up producer execution: {str(e)}")
            else:
                self.log(f"Error: Producer file not found at {producer_path}")

            # Start Consumer
            consumer_path = "/Users/harvijaysingh/btech cse/3rd year/internship/udated/spark_kafka_to_hdfs.py"
            if os.path.exists(consumer_path):
                try:
                    script_path = "/tmp/terminal_script_consumer.sh"
                    with open(script_path, 'w') as f:
                        f.write("#!/bin/bash\n")
                        f.write("clear\n")
                        f.write("echo '=== Consumer Session ==='\n")
                        f.write(f"source {VENV_ACTIVATE}\n")
                        f.write("if [[ -z \"$VIRTUAL_ENV\" ]]; then\n")
                        f.write("    echo 'ERROR: Virtual environment activation failed!'\n")
                        f.write("    echo 'Current Python path: $(which python)'\n")
                        f.write("    read -p 'Press Enter to close this terminal...'\n")
                        f.write("    exit 1\n")
                        f.write("fi\n")
                        f.write("echo 'Virtual environment activated: $VIRTUAL_ENV'\n")
                        f.write("echo 'Python path: $(which python)'\n")
                        f.write("echo 'Starting Spark Consumer...'\n")
                        f.write(f"spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.kafka:kafka-clients:3.3.2 '{consumer_path}'\n")
                        f.write("if [ $? -ne 0 ]; then\n")
                        f.write("    echo 'Error: Consumer script failed'\n")
                        f.write("    read -p 'Press Enter to close this terminal...'\n")
                        f.write("    exit 1\n")
                        f.write("fi\n")
                        f.write("read -p 'Press Enter to close this terminal...'\n")
                    os.chmod(script_path, 0o755)
                    
                    consumer_proc = self.execute_in_terminal(f"bash {script_path}", 10, "Consumer")
                    if consumer_proc:
                        self.log("Started Consumer in Terminal 10")
                        self.log("Spark consumer process started with packages:")
                        self.log("org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
                        self.log("org.apache.kafka:kafka-clients:3.3.2\n")
                    else:
                        self.log("Failed to start Consumer terminal\n")
                        
                except Exception as e:
                    self.log(f"Error setting up consumer execution: {str(e)}\n")
            else:
                self.log(f"Error: Consumer file not found at {consumer_path}")

            self.log("All processes started successfully\n")

        except Exception as e:
            self.log(f"Unexpected error in start_sessions: {str(e)}\n")
        finally:
            loading_daemons.close()

    def stop_sessions(self):
        self.log("Stop pressed. Terminating producer and consumer...\n")
        terminated = False
        
        try:
            ps_output = subprocess.check_output(["ps", "-ax"]).decode('utf-8')
            
            for line in ps_output.split('\n'):
                if 'generated_events.py' in line and 'python' in line:
                    pid = line.strip().split()[0]
                    try:
                        subprocess.run(["kill", "-9", pid])
                        self.log(f"Terminated producer process (PID: {pid})")
                        terminated = True
                    except Exception as e:
                        self.log(f"Error killing producer process {pid}: {str(e)}")
                
                if 'spark_kafka_to_hdfs.py' in line and ('python' in line or 'spark-submit' in line):
                    pid = line.strip().split()[0]
                    try:
                        subprocess.run(["kill", "-9", pid])
                        self.log(f"Terminated consumer process (PID: {pid})\n")
                        terminated = True
                    except Exception as e:
                        self.log(f"Error killing consumer process {pid}: {str(e)}\n")
            
            try:
                subprocess.run(["pkill", "-f", "spark-submit"])
                self.log("Terminated any Spark submit processes")
                terminated = True
            except Exception as e:
                self.log(f"Error terminating Spark processes: {str(e)}")
            
            self.terminal_processes = [proc for proc in self.terminal_processes if proc.poll() is None]
            
            if terminated:
                self.log("Successfully stopped producer and consumer\n")
            else:
                self.log("No producer or consumer processes found to terminate")
                
        except Exception as e:
            self.log(f"Error during stop operation: {str(e)}")

    def terminate_sessions(self):
        loading_termination = LoadingScreen("Terminating all services...")
        main_rect = self.geometry()
        loading_size = loading_termination.size()
        x = main_rect.x() + (main_rect.width() - loading_size.width()) // 2
        y = main_rect.y() + (main_rect.height() - loading_size.height()) // 2
        loading_termination.move(x, y)
        loading_termination.show()
        QApplication.processEvents()

        try:
            self.log("Terminating all sessions and services...\n")
            
            for proc in self.processes + self.terminal_processes:
                try:
                    if proc.poll() is None:
                        proc.terminate()
                        time.sleep(0.5)
                        if proc.poll() is None:
                            proc.kill()
                except Exception as e:
                    self.log(f"Error terminating a managed process: {str(e)}")
            
            self.processes = []
            self.terminal_processes = []
            self.services_running = False
            
            try:
                self.log("Stopping HDFS services...")
                subprocess.run(["stop-dfs.sh"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.log("HDFS services stopped")
            except subprocess.CalledProcessError as e:
                self.log(f"Error stopping HDFS: {e.stdout.decode() + e.stderr.decode()}")
            except Exception as e:
                self.log(f"Unexpected error stopping HDFS: {str(e)}")
            
            try:
                self.log("Stopping YARN services...")
                subprocess.run(["stop-yarn.sh"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.log("YARN services stopped")
            except subprocess.CalledProcessError as e:
                self.log(f"Error stopping YARN: {e.stdout.decode() + e.stderr.decode()}")
            except Exception as e:
                self.log(f"Unexpected error stopping YARN: {str(e)}")
            
            try:
                self.log("Stopping Kafka service...")
                subprocess.run(["brew", "services", "stop", "kafka"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.log("Kafka service stopped")
            except subprocess.CalledProcessError as e:
                self.log(f"Error stopping Kafka: {e.stdout.decode() + e.stderr.decode()}")
            except Exception as e:
                self.log(f"Unexpected error stopping Kafka: {str(e)}")
            
            try:
                self.log("Cleaning up remaining processes...")
                subprocess.run(["pkill", "-f", "generated_events.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                subprocess.run(["pkill", "-f", "spark_kafka_to_hdfs.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                subprocess.run(["pkill", "-f", "terminal_script_"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                subprocess.run(["pkill", "-f", "kafka"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                subprocess.run(["pkill", "-f", "zookeeper"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                subprocess.run(["pkill", "-f", "streamlit"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                subprocess.run(["pkill", "-f", "spark-submit"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.log("Remaining processes cleaned up")
            except Exception as e:
                self.log(f"Cleanup error: {str(e)}")
            
            time.sleep(20)
            try:
                jps_output = subprocess.check_output(["jps"]).decode('utf-8')
                self.log(f"Final jps output:\n{jps_output}")
            except Exception as e:
                self.log(f"Could not verify running processes with jps: {str(e)}")

            self.log("\nAll processes and services terminated.")

        except Exception as e:
            self.log(f"Unexpected error in terminate_sessions: {str(e)}")
        finally:
            loading_termination.close()

    def _open_window_with_loading(self, window_class, attr_name, message):
        loading = LoadingScreen(message)
        main_rect = self.geometry()
        loading_size = loading.size()
        x = main_rect.x() + (main_rect.width() - loading_size.width()) // 2
        y = main_rect.y() + (main_rect.height() - loading_size.height()) // 2
        loading.move(x,y)
        
        loading.show()
        QTimer.singleShot(0, lambda: self._show_window(window_class, attr_name, loading))

    def _show_window(self, window_class, attr_name, loading_widget):
        loading_widget.close()
        existing_window = getattr(self, attr_name)
        if existing_window is None or not existing_window.isVisible():
            window = window_class()
            setattr(self, attr_name, window)
            window.show()
            self.log(f"Opened {window.windowTitle()}.")
        else:
            existing_window.activateWindow()
            existing_window.raise_()
            self.log(f"{existing_window.windowTitle()} is already open.")

    def open_analytics_window(self):
        streamlit_path = '/Users/harvijaysingh/btech cse/3rd year/internship/udated/app.py'
    
        if not os.path.exists(streamlit_path):
            self.log(f"Error: Streamlit app file not found at {streamlit_path}")
            return
    
        try:
            ps_output = subprocess.check_output(["ps", "-ax"]).decode('utf-8')
            if 'streamlit run app.py' in ps_output:
                self.log("Streamlit analytics already running - opening browser")
                webbrowser.open("http://localhost:8501")
                return

            script_path = "/tmp/terminal_script_streamlit.sh"
            with open(script_path, 'w') as f:
                f.write("#!/bin/bash\n")
                f.write("clear\n")
                f.write("echo '=== Streamlit Analytics ==='\n")
                f.write(f"source {VENV_ACTIVATE}\n")
                f.write("if [[ -z \"$VIRTUAL_ENV\" ]]; then\n")
                f.write("    echo 'ERROR: Virtual environment activation failed!'\n")
                f.write("    echo 'Current Python path: $(which python)'\n")
                f.write("    read -p 'Press Enter to close this terminal...'\n")
                f.write("    exit 1\n")
                f.write("fi\n")
                f.write("echo 'Virtual environment activated: $VIRTUAL_ENV'\n")
                f.write("echo 'Python path: $(which python)'\n")
                f.write(f"streamlit run '{streamlit_path}'\n")
                f.write("if [ $? -ne 0 ]; then\n")
                f.write("    echo 'Error: Streamlit app failed'\n")
                f.write("    read -p 'Press Enter to close this terminal...'\n")
                f.write("    exit 1\n")
                f.write("fi\n")
                f.write("read -p 'Press Enter to close this terminal...'\n")
            os.chmod(script_path, 0o755)
        
            terminal_proc = self.execute_in_terminal(
                f"bash {script_path}",
                11,
                "Streamlit Analytics"
            )
        
            if terminal_proc:
                self.terminal_processes.append(terminal_proc)
                self.log("Started Streamlit analytics in Terminal 11")
                QTimer.singleShot(3000, lambda: webbrowser.open("http://localhost:8501"))
            else:
                self.log("Failed to start Streamlit terminal")
            
        except Exception as e:
            self.log(f"Error setting up Streamlit analytics: {str(e)}")

    def open_monitor_window(self):
        self._open_window_with_loading(FileMonitoringWindow, 'monitor_window', "Starting file monitor...")

    def open_system_monitoring(self):
        try:
            url = "https://us5.datadoghq.com/dashboard/gmb-csm-j6n/system-metrics?fromUser=false&refresh_mode=sliding&from_ts=1752079993453&to_ts=1752083593453&live=true"
            webbrowser.open(url)
            self.log(f"Opened System Monitoring dashboard: {url}")
        except Exception as e:
            self.log(f"Error opening System Monitoring: {str(e)}")

    def open_system_logs_window(self):
        self._open_window_with_loading(SystemLogsWindow, 'system_logs_window', "Loading system logs...")

    def open_ports_window(self):
        if self.ports_window is None or not self.ports_window.isVisible():
            self.ports_window = PortsWindow()
            self.ports_window.show()
            self.log("Opened Ports window with service links.")
        else:
            self.ports_window.activateWindow()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())