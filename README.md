
# AlgoTradeX-Intelligent-Automated-Trading-System

This is an **automated trading system** designed for live trading and strategy testing using the **[Alpaca](https://alpaca.markets/) API**. It retrieves real-time market data, calculates indicators, generates trade signals and executes orders based on predefined strategies.

It supports:

- **Live trading** using Alpaca's API on AWS EC2, allowing continuous execution in a cloud environment.
- **Local execution** using Docker for development and strategy testing.
- **Automated deployments** to EC2 via a GitHub Actions CD pipeline, ensuring seamless updates when changes are pushed.

⚠️ _Note: A legacy TCP-based simulated trading approach is included in this repo but is no longer the primary method._

## 📂 Project Structure

```plaintext
📂 trading-system/
│── 📜 README.md             # Project documentation & setup instructions
│── 📜 requirements.txt      # Python dependencies
│── 📜 Dockerfile            # 🐳 Docker setup for containerized deployment
│── 📜 main_day_trader.py    # 🚀 Entry point for Alpaca paper trading (Alpaca API)

├── 📂 data/                 # 📊 Historical market data and trade logs

├── 📂 models/               # 🧠 Trained ML models and feature scalers  

├── 📂 notebooks/            # 📓 Strategy research, backtesting and model training  

├── 📂 src/                  # 🏗️ Core trading system  
│   ├── 📂 alpaca_utils/     # 📡 Alpaca API modules
│   ├── 📂 local_trading/    # 🔌 Simulated trading & backtesting (TCP-based)
│   └── 📂 scripts/          # 🛠️ Utility scripts (data fetching, API checks)

├── 📂 .github/workflows/    # 🔄 CI/CD pipeline  
│   └── 📜 deploy.yml        # 🚀 GitHub Actions workflow for EC2 auto-deploy
```

## 🚀 Getting Started

### 1️⃣ Clone the Repository

#### Using SSH (Recommended):
```bash
git clone git@github.com:sohammandal/trading-system.git
cd trading-system
```

### 2️⃣ Create a Virtual Environment & Install Dependencies
```bash
python -m venv venv
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

### 3️⃣ Configure API Keys
Obtain [API Keys](https://alpaca.markets/learn/connect-to-alpaca-api) from Alpaca, create a `.env` file as seen below in the project root and ensure it is included in `.gitignore`:
```bash
ALPACA_API_KEY=your_api_key
ALPACA_SECRET_KEY=your_secret_key
```

### 4️⃣ Verify Setup by Checking Account Balance
Run the following command to confirm your API keys are working correctly:

```bash
python src/scripts/check_balance.py # Checks API Connectivity
```

## 🛠 Running the Trading Bot with Docker

Easily run the trading bot locally using **Alpaca's API** in a containerized environment:  

### 1️⃣ Build the Docker Image
```bash
docker build -t trading-bot .
```

### 2️⃣ Run the Trading Bot
```bash
docker run -d --restart unless-stopped --env-file .env --name trading-bot trading-bot
```

This will start the bot in a **detached mode**, automatically restarting if it stops.  

### 3️⃣ Check Logs
```bash
docker logs -f trading-bot
```

### 4️⃣ Stop & Remove the Container
```bash
docker stop trading-bot && docker rm trading-bot
```

👉 Make sure your `.env` file contains valid API credentials before running the container.

## 🌍 Deploying to AWS EC2 

The trading bot can run **continuously on AWS EC2**, leveraging **GitHub Actions** for **automated deployments** whenever updates are pushed to the repository. This ensures that the bot is always running the latest version without manual intervention. 

### 1️⃣ Set Up an AWS EC2 Instance
- Launch a **t2.micro (free-tier)** EC2 instance running **Ubuntu**.  
- Install **Docker & Git** on the instance.
- Configure security groups to allow **SSH (port 22)**. 
- Generate an **SSH key pair** in the instance for GitHub authentication.
- Clone the repository and configure environment variables (create a `.env` file in the project root).

### 2️⃣ Set Up GitHub Actions for Automated Deployment
- Add the **EC2 SSH private key** as a **GitHub Secret** (`EC2_SSH_PRIVATE_KEY`).  
- Store **EC2 public IP, username (`ubuntu`), and other details** as repo secrets (`EC2_HOST`, `EC2_USER`).  
- Configure the **GitHub Actions workflow** (`.github/workflows/deploy.yml`) to:
  - SSH into EC2.  
  - Pull the latest code from GitHub.  
  - Rebuild and restart the Docker container running the bot.

Once set up, any push to the `main` branch will automatically deploy the latest version to EC2.

### 3️⃣ Monitor & Manage the Bot
- **Check live logs:**
  ```bash
  docker logs -f trading-bot
  ```
- **Restart the bot if needed:**
  ```bash
  docker restart trading-bot
  ```
- **Manually update & restart:**
  ```bash
  git pull origin main
  docker stop trading-bot && docker rm trading-bot
  docker build -t trading-bot .
  docker run -d --restart unless-stopped --env-file .env --name trading-bot trading-bot
  ```

This setup ensures a **reliable, automated, and cloud-based live trading environment** with minimal maintenance.

## ⚡ **Legacy: Simulated Trading (TCP-based, No API Keys Required)**  

This **older testing method** streams historical data from CSV files to simulate real-time trading.  

### **1️⃣ Start the TCP Server**  
Streams market data to connected clients:  
```bash
python src/local_trading/tcp_server.py -p 9999 -f data/historical_stock_data_5min_6months.csv -t 0.1
```

### **2️⃣ Run the Trading Client**  
Executes trades based on the simulated market data:  
```bash
python src/local_trading/four_factor_day_trader.py
```

### **3️⃣ Stop the Client**  
Use `Ctrl + C` to stop execution.  

All trade logs are saved in the `data/` directory. 🚀
=======

