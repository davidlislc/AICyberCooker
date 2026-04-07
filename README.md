# AICyberCooker
Core module of AI Cyber System

---

## Deploying AICyberCooker on Ubuntu

### Prerequisites

Update your package list and install the required system packages:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv git docker.io docker-compose-plugin
```

Enable and start Docker:

```bash
sudo systemctl enable --now docker
sudo usermod -aG docker $USER   # log out and back in after this step
```

---

### 1. Clone the Repository

```bash
git clone https://github.com/davidlislc/AICyberCooker.git
cd AICyberCooker
```

---

### 2. Start Kafka

AICyberCooker requires a running Kafka broker. A Docker Compose file is provided under `kafka/`:

```bash
docker compose -f kafka/docker-compose.yml up -d
```

This starts Zookeeper, Kafka (port **9092**), a Kafka UI (port **8080**), and automatically creates the required topics (`aiengine_in`, `llm_in`, and others).

Wait until Kafka is healthy before proceeding:

```bash
docker compose -f kafka/docker-compose.yml ps
```

---

### 3. Create and Activate a Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

### 4. Install Python Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

### 5. Configure the Application

Edit `config.yaml` to match your environment. The defaults work with the bundled Docker Compose Kafka setup:

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  consumer_group: "aiengine_group"
  input_topic: "aiengine_in"
  llm_topic: "llm_in"
  poll_timeout_ms: 1000
  max_poll_records: 10

ml:
  device: "auto"          # "auto" | "cpu" | "cuda"
  anomaly_contamination: 0.05
  batch_size: 32
  sequence_max_length: 512

logging:
  level: "INFO"
  format: "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
```

---

### 6. Run AICyberCooker

```bash
python main.py --config config.yaml
```

The engine will begin consuming jobs from the `aiengine_in` Kafka topic, run ML analysis, and publish findings to the `llm_in` topic.

To stop the process, press **Ctrl+C** or send `SIGTERM`.

---

### 7. Run as a systemd Service (Optional)

To keep AICyberCooker running across reboots, create a systemd unit file.

Replace `/home/<user>/AICyberCooker` with your actual path and `<user>` with your username:

```bash
sudo tee /etc/systemd/system/aicybercooker.service > /dev/null <<EOF
[Unit]
Description=AICyberCooker AI Engine
After=network.target docker.service

[Service]
User=<user>
WorkingDirectory=/home/<user>/AICyberCooker
ExecStart=/home/<user>/AICyberCooker/.venv/bin/python main.py --config config.yaml
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
```

Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now aicybercooker
sudo systemctl status aicybercooker
```

---

### 8. Run Tests

```bash
python -m pytest tests/
```

---

### Stopping Kafka

```bash
docker compose -f kafka/docker-compose.yml down
```

Here is the updated workflow to ensure AICyberCooker runs on 3.11 without messing with your 3.12 setup.

1. Ensure Python 3.11 and the Venv Module are Installed
Even if Python 3.12 is your default, you must have the 3.11 binaries and the specific venv package installed. If you haven't done this yet:

Bash
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.11 python3.11-venv
2. Clone and Enter the Project
Bash
git clone https://github.com/davidlislc/AICyberCooker.git
cd AICyberCooker
3. Create the 3.11 Virtual Environment
This is the critical step. Instead of using python3, call the 3.11 binary directly to initialize the environment:

Bash
python3.11 -m venv venv311
Note: I named it venv311 here just to make it easy to identify, but you can name it venv if you prefer.

4. Activate the Environment
Once activated, any python or pip command you run will stay locked to version 3.11.

Bash
source venv311/bin/activate
Verification: Run python --version. It should now return Python 3.11.x, even though your system default is 3.12.

5. Install AICyberCooker Dependencies
Now that you are safely inside the 3.11 bubble, install the requirements:

Bash
pip install --upgrade pip
pip install -r requirements.txt
6. Deployment & Running
Environment Variables: Create your .env file and add your API keys as required by the project.

Bash
cp .env.example .env
nano .env
Run the App: Based on the project structure for AICyberCooker:

Bash
python run.py 
# OR if it's a streamlit app
streamlit run app.py
