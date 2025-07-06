import sys
import subprocess
import threading
import os
import time
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
        self.setWindowTitle("Read File")
        self.resize(600, 400)
        layout = QVBoxLayout()
        label = QLabel("Read file content goes here...")
        layout.addWidget(label)
        self.setLayout(layout)

class FileMonitoringWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("File Monitoring")
        self.resize(600, 400)
        layout = QVBoxLayout()
        label = QLabel("File monitoring content goes here...")
        layout.addWidget(label)
        self.setLayout(layout)

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("near real time e-com website analytics")
        self.resize(2000, 1000)

        self.processes = []
        self.terminal_processes = []

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

        # === Placeholder windows ===
        self.analytics_window = None
        self.read_file_window = None
        self.monitor_window = None

    def log(self, message):
        self.log_output.append(f"[DEBUG] {message}")

    def read_process_output(self, process, session_name):
        """Reads output from a process and writes to log window live."""
        try:
            for line in iter(process.stdout.readline, ''):
                if line:
                    self.log(f"{session_name}: {line.strip()}")
            process.stdout.close()
        except Exception as e:
            self.log(f"Error reading output for {session_name}: {e}")

    def execute_in_terminal(self, command, terminal_number, session_name):
        """Executes a single command in a new terminal window on macOS."""
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
                ["open", "-a", "Terminal", script_path],
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
        if self.processes or self.terminal_processes:
            self.log("Warning: Processes already running.")
            return

        try:
            # Initial jps check
            jps_proc = subprocess.Popen(
                ["jps"],
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT, 
                text=True, 
                bufsize=1
            )
            jps_output, _ = jps_proc.communicate()
            self.log(f"Initial jps output:\n{jps_output}")

            # Check running daemons
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
            self.log(f"Running daemons: {running_daemons}")

            # Start required services
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

            # Verify services are running
            for attempt in range(3):
                self.log(f"Service check attempt {attempt + 1}/3")
                jps_proc = subprocess.Popen(
                    ["jps"],
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.STDOUT, 
                    text=True, 
                    bufsize=1
                )
                jps_output, _ = jps_proc.communicate()
                self.log(f"jps output:\n{jps_output}")

                if all(keyword in jps_output for keyword in ["Kafka", "QuorumPeerMain"]) and \
                   any(keyword in jps_output for keyword in ["NodeManager", "ResourceManager", "NameNode", "DataNode"]):
                    break
                time.sleep(10)
            else:
                self.log("Error: Required services not running after retries")
                return

            # Start Producer with automatic virtualenv activation
            producer_path = "/Users/harvijaysingh/btech cse/3rd year/internship/udated/generated_events.py"
            venv_path = "/Users/harvijaysingh/venv"  # UPDATE THIS TO YOUR ACTUAL VENV PATH
            
            if os.path.exists(producer_path):
                try:
                    # Create script with automatic virtualenv activation
                    script_path = "/tmp/terminal_script_producer.sh"
                    with open(script_path, 'w') as f:
                        f.write("#!/bin/bash\n")
                        f.write("echo 'Activating Python virtual environment...'\n")
                        f.write(f"source {venv_path}/bin/activate\n")
                        f.write("if [[ -z \"$VIRTUAL_ENV\" ]]; then\n")
                        f.write("    echo 'ERROR: Failed to activate virtual environment!'\n")
                        f.write("    echo 'Please check the venv path and try again'\n")
                        f.write("    read -p 'Press Enter to close this terminal...'\n")
                        f.write("    exit 1\n")
                        f.write("else\n")
                        f.write("    echo 'Successfully activated virtualenv: $VIRTUAL_ENV'\n")
                        f.write(f"    python '{producer_path}'\n")
                        f.write("    if [ $? -ne 0 ]; then\n")
                        f.write("        echo 'Error: Producer script failed'\n")
                        f.write("        read -p 'Press Enter to close this terminal...'\n")
                        f.write("        exit 1\n")
                        f.write("    fi\n")
                        f.write("fi\n")
                        f.write("read -p 'Press Enter to close this terminal...'\n")
                    os.chmod(script_path, 0o755)
                    
                    # Start producer terminal
                    producer_proc = self.execute_in_terminal(
                        f"source {script_path}",
                        9, "Producer"
                    )
                    if producer_proc:
                        self.log("Started Producer in Terminal 9 (with auto virtualenv activation)")
                    else:
                        self.log("Failed to start Producer terminal")
                        
                except Exception as e:
                    self.log(f"Error setting up producer execution: {str(e)}")
            else:
                self.log(f"Error: Producer file not found at {producer_path}")

            # Start Consumer
            consumer_path = "/Users/harvijaysingh/btech cse/3rd year/internship/udated/spark_kafka_to_hdfs.py"
            if os.path.exists(consumer_path):
                self.execute_in_terminal(
                    f"spenv; spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.kafka:kafka-clients:3.3.2 '{consumer_path}'",
                    10, "Consumer"
                )
                self.log("Started Consumer in Terminal 10")
            else:
                self.log(f"Error: Consumer file not found at {consumer_path}")

            self.log("All processes started successfully")

        except Exception as e:
            self.log(f"Unexpected error in start_sessions: {str(e)}")

    def stop_sessions(self):
        """Terminates only the producer and consumer processes."""
        self.log("Stop pressed. Terminating producer and consumer...")
        terminated = False
        
        try:
            # Find and kill Python processes
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
            
            # Terminate terminal windows (9 and 10)
            for proc in self.terminal_processes[:]:
                try:
                    if len(self.terminal_processes) > 8 and proc == self.terminal_processes[8]:  # Producer terminal
                        proc.terminate()
                        self.terminal_processes.remove(proc)
                        self.log("Terminated producer terminal")
                        terminated = True
                    elif len(self.terminal_processes) > 9 and proc == self.terminal_processes[9]:  # Consumer terminal
                        proc.terminate()
                        self.terminal_processes.remove(proc)
                        self.log("Terminated consumer terminal")
                        terminated = True
                except Exception as e:
                    self.log(f"Error terminating terminal process: {str(e)}")
            
            if terminated:
                self.log("Successfully stopped producer and consumer")
            else:
                self.log("No producer or consumer processes found to terminate")
                
        except Exception as e:
            self.log(f"Error during stop operation: {str(e)}")

    def terminate_sessions(self):
        """Terminates all processes and terminal sessions."""
        for proc in self.processes + self.terminal_processes:
            try:
                proc.terminate()
            except:
                pass
        self.processes = []
        self.terminal_processes = []
        self.log("Terminated all processes and terminal sessions.")

    def open_analytics_window(self):
        if not self.analytics_window:
            self.analytics_window = AnalyticsWindow()
        self.analytics_window.show()

    def open_read_file_window(self):
        if not self.read_file_window:
            self.read_file_window = ReadFileWindow()
        self.read_file_window.show()

    def open_monitor_window(self):
        if not self.monitor_window:
            self.monitor_window = FileMonitoringWindow()
        self.monitor_window.show()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())