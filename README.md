# Airflow – Encerramento Sirtec

🎯 Orquestra fluxos de encerramento de processos/metas da Sirtec usando Apache Airflow.

## 🔧 Requisitos
Python 3.8+

Apache Airflow 3.0+ (ou 2.6+)

## Instalar dependências (modo local):

### Criar venv
```
python -m venv nome_venv
```

### Ativar venv
Linux
```
source nome_venv/bin/activate
```
Windows
```
nome_venv/Scripts/activate
```

### Instalar dependências de requirements
```
pip install -r requirements.txt
```

## 🚀 Rodando o Airflow Localmente

Iniciar airflow
```
export AIRFLOW_HOME=~/caminho/para/airflow
airflow standalone
```

Criar tunnel
```
cloudflared tunnel --url http://localhost:8080
```

## 🧠 Comandos Úteis

```
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
airflow dags list
airflow dags trigger <dag_id>
airflow tasks test <dag_id> <task_id> <execution_date>
airflow dags pause <dag_id>
```

## ⚙️ Configurar inicio automático

Seter `.sh` como executável
```
sudo apt install tmux
chmod +x ~/airflow/airflow_tunnel.sh
./airflow_tmux_start.sh
```

### Criando serviço

```
sudo nano /etc/systemd/system/airflow_tunnel.service
```

```
[Unit]
Description=Airflow Standalone com Cloudflare Tunnel
After=network.target

[Service]
Type=simple
User=seu_usuario
WorkingDirectory=/home/seu_usuario/airflow
ExecStart=/home/seu_usuario/airflow/airflow_tunnel.sh
Restart=always

[Install]
WantedBy=multi-user.target
```

### Ativar e iniciar

```
sudo systemctl daemon-reload
sudo systemctl enable airflow_tunnel.service
sudo systemctl start airflow_tunnel.service
```

Ver logs:
```
journalctl -u airflow_tunnel.service -f
```

Parar o serviço completo:
```
sudo systemctl stop airflow_tunnel.service
sudo systemctl disable airflow_tunnel.service
```
