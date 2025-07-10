import sys
import subprocess
import threading
import os
import time
import json
import webbrowser
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTextEdit, QMenu, QToolButton, QLabel
)
from PyQt5.QtCore import Qt, QTimer, QFileSystemWatcher

# Virtual environment configuration
VENV_PATH = "/Users/harvijaysingh/pyspark-env"
VENV_ACTIVATE = f"{VENV_PATH}/bin/activate"

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
            background-color: #f5f5f5;
        """)
        
        self.refresh_btn = QPushButton("Manual Refresh")
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

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("near real time e-com website analytics")
        self.resize(2000, 1000)

        self.processes = []
        self.terminal_processes = []
        self.services_running = False

        # UI Setup
        main_layout = QVBoxLayout()
        top_layout = QHBoxLayout()

        # Hamburger Menu
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
        menu.addAction("File Monitoring", self.open_monitor_window)
        menu.addAction("System Monitoring", self.open_system_monitoring)
        self.menu_button.setMenu(menu)

        top_layout.addWidget(self.menu_button)

        # Control Buttons
        self.start_btn = QPushButton("Start")
        self.stop_btn = QPushButton("Stop")
        self.terminate_btn = QPushButton("Terminate")

        for btn in [self.start_btn, self.stop_btn, self.terminate_btn]:
            btn.setFixedSize(200, 50)
            top_layout.addWidget(btn)

        top_layout.addStretch()
        main_layout.addLayout(top_layout)

        # Log Window
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet("font-size: 16px;")
        main_layout.addWidget(self.log_output)

        self.setLayout(main_layout)

        # Connect Buttons
        self.start_btn.clicked.connect(self.start_sessions)
        self.stop_btn.clicked.connect(self.stop_sessions)
        self.terminate_btn.clicked.connect(self.terminate_sessions)

        # Windows
        self.monitor_window = None

        # Initial checks
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
        self.log_output.append(f"[DEBUG] {message}")

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
                f.write(f"  echo 'Error: {command} failed'\n")
                f.write("  read -p 'Press Enter to close this terminal...'\n")
                f.write("  exit 1\n")
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
        producer_running = False
        consumer_running = False
        
        try:
            ps_output = subprocess.check_output(["ps", "-ax"]).decode('utf-8')
            producer_running = any('generated_events.py' in line and 'python' in line for line in ps_output.split('\n'))
            consumer_running = any('spark_kafka_to_hdfs.py' in line and ('python' in line or 'spark-submit' in line) for line in ps_output.split('\n'))
        except Exception as e:
            self.log(f"Error checking running processes: {str(e)}")
        
        if producer_running or consumer_running:
            self.log("Warning: Producer or Consumer processes already running.")
            return

        try:
            if not self.check_virtualenv():
                self.log("Cannot start sessions - virtual environment not configured properly")
                return

            # Initial jps check
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
            self.log(f"Currently running daemons: {running_daemons}")

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

            # Start Consumer (improved version)
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
                        self.log("org.apache.kafka:kafka-clients:3.3.2")
                    else:
                        self.log("Failed to start Consumer terminal")
                        
                except Exception as e:
                    self.log(f"Error setting up consumer execution: {str(e)}")
            else:
                self.log(f"Error: Consumer file not found at {consumer_path}")

            self.log("All processes started successfully")

        except Exception as e:
            self.log(f"Unexpected error in start_sessions: {str(e)}")

    def stop_sessions(self):
        self.log("Stop pressed. Terminating producer and consumer...")
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
                        self.log(f"Terminated consumer process (PID: {pid})")
                        terminated = True
                    except Exception as e:
                        self.log(f"Error killing consumer process {pid}: {str(e)}")
            
            # Clean up Spark processes
            try:
                subprocess.run(["pkill", "-f", "spark-submit"])
                self.log("Terminated any Spark submit processes")
                terminated = True
            except Exception as e:
                self.log(f"Error terminating Spark processes: {str(e)}")
            
            # Clear terminal processes
            if len(self.terminal_processes) > 8:
                try:
                    self.terminal_processes[8].terminate()
                    self.log("Terminated producer terminal")
                    terminated = True
                except Exception as e:
                    self.log(f"Error terminating producer terminal: {str(e)}")
            
            if len(self.terminal_processes) > 9:
                try:
                    self.terminal_processes[9].terminate()
                    self.log("Terminated consumer terminal")
                    terminated = True
                except Exception as e:
                    self.log(f"Error terminating consumer terminal: {str(e)}")
            
            if len(self.terminal_processes) > 10:
                self.terminal_processes = self.terminal_processes[:8] + self.terminal_processes[10:]
            elif len(self.terminal_processes) > 9:
                self.terminal_processes = self.terminal_processes[:8]
            
            if terminated:
                self.log("Successfully stopped producer and consumer")
            else:
                self.log("No producer or consumer processes found to terminate")
                
        except Exception as e:
            self.log(f"Error during stop operation: {str(e)}")

    def terminate_sessions(self):
        self.log("Terminating all sessions and services...")
        
        for proc in self.processes + self.terminal_processes:
            try:
                proc.terminate()
                proc.kill()
            except:
                pass
        
        self.processes = []
        self.terminal_processes = []
        self.services_running = False
        
        try:
            self.log("\nStopping HDFS services...")
            subprocess.run(["stop-dfs.sh"], check=True)
            self.log("HDFS services stopped")
        except subprocess.CalledProcessError as e:
            self.log(f"Error stopping HDFS: {str(e)}")
        except Exception as e:
            self.log(f"Unexpected error stopping HDFS: {str(e)}")
        
        try:
            self.log("Stopping YARN services...")
            subprocess.run(["stop-yarn.sh"], check=True)
            self.log("YARN services stopped")
        except subprocess.CalledProcessError as e:
            self.log(f"Error stopping YARN: {str(e)}")
        except Exception as e:
            self.log(f"Unexpected error stopping YARN: {str(e)}")
        
        try:
            self.log("Stopping Kafka service...")
            subprocess.run(["brew", "services", "stop", "kafka"], check=True)
            self.log("Kafka service stopped")
        except subprocess.CalledProcessError as e:
            self.log(f"Error stopping Kafka: {str(e)}")
        except Exception as e:
            self.log(f"Unexpected error stopping Kafka: {str(e)}")
        
        try:
            self.log("Cleaning up remaining processes...")
            subprocess.run(["pkill", "-f", "generated_events.py"])
            subprocess.run(["pkill", "-f", "spark_kafka_to_hdfs.py"])
            subprocess.run(["pkill", "-f", "terminal_script_"])
            subprocess.run(["pkill", "-f", "kafka"])
            subprocess.run(["pkill", "-f", "zookeeper"])
            subprocess.run(["pkill", "-f", "streamlit"])
            subprocess.run(["pkill", "-f", "spark-submit"])
            self.log("Remaining processes cleaned up")
        except Exception as e:
            self.log(f"Cleanup error: {str(e)}")
        
        try:
            jps_output = subprocess.check_output(["jps"]).decode('utf-8')
            self.log(f"Final jps output:\n{jps_output}")
        except:
            self.log("Could not verify running processes with jps")
        
        self.log("All processes and services terminated.")

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
        if not self.monitor_window:
            self.monitor_window = FileMonitoringWindow()
        self.monitor_window.show()

    def open_system_monitoring(self):
        try:
            url = "https://us5.datadoghq.com/dashboard/gmb-csm-j6n/system-metrics?fromUser=false&refresh_mode=sliding&from_ts=1752079993453&to_ts=1752083593453&live=true"
            webbrowser.open(url)
            self.log(f"Opened System Monitoring dashboard: {url}")
        except Exception as e:
            self.log(f"Error opening System Monitoring: {str(e)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
