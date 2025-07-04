# Airflow com Cloudflare Tunnel via systemd – Encerramento Sirtec

🎯 Orquestra fluxos de encerramento de processos/metas da Sirtec usando Apache Airflow.

Este projeto configura o Apache Airflow rodando em modo standalone e exposto através de um túnel Cloudflare, ambos gerenciados como serviços `systemd` no Linux. Isso permite que o Airflow seja iniciado automaticamente com o sistema e acessado remotamente, mesmo sem IP público.

#### Acesso ao Airflow

- O serviço `tunnel.service` cria um túnel público usando o `trycloudflare.com` e salva o link gerado no arquivo `~/airflow/tunnel_link.txt`. Esse link pode ser acessado de qualquer lugar e redireciona para o `localhost:8080`, onde o Airflow está escutando.

## Estrutura do projeto

A raiz do projeto está localizada em `~/airflow`, que contém os arquivos e scripts necessários:

- `airenv/`: ambiente virtual Python com o Airflow instalado.
- `start_airflow.sh`: script que ativa o ambiente virtual e inicia o Airflow standalone.
- `start_tunnel.sh`: script que ativa o túnel do Cloudflare e salva o link gerado.
- `airflow.log`: arquivo de log gerado pela execução do Airflow.
- `tunnel.log`: log da execução do túnel.
- `tunnel_link.txt`: contém o link público gerado para acessar o Airflow via `trycloudflare.com`.


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

## ⚙️ Configurar scripts para iniciar

Setar `.sh` como executável
```
chmod +x ~/airflow/start_airflow.sh
chmod +x ~/airflow/start_tunnel.sh
```

### Criando serviço
- Tunnel
```
sudo nano /etc/systemd/system/airflow_tunnel.service
```

```
[Unit]
Description=Airflow + Cloudflared em tmux
After=network.target

[Service]
User=sirtec-fechamento
WorkingDirectory=/home/sirtec-fechamento/airflow
ExecStart=/usr/bin/tmux new-session -s airflow_tunnel /home/sirtec-fechamento/airflow/airflow_tunnel.sh
Restart=on-failure
Environment=TERM=xterm-256color
KillMode=process

[Install]
WantedBy=default.target

```

- Airflow
```
sudo nano /etc/systemd/system/airflow_standalone.service
```

```
[Unit]
Description=Airflow Standalone
After=network.target

[Service]
User=sirtec-fechamento
WorkingDirectory=/home/sirtec-fechamento/airflow
ExecStart=/home/sirtec-fechamento/airflow/start_airflow.sh
Environment=PYTHONPATH=/home/sirtec-fechamento/airflow
Restart=on-failure
Environment=TERM=xterm-256color
Environment=AIRFLOW_HOME=/home/sirtec-fechamento/airflow


[Install]
WantedBy=multi-user.target

```
### Ativar e iniciar

```
sudo systemctl daemon-reload
sudo systemctl enable airflow_tunnel.service
sudo systemctl enable airflow_standalone.service
sudo systemctl start airflow_tunnel.service
sudo systemctl start airflow_standalone.service
```

Ver logs:
```
journalctl -u airflow_tunnel.service -f
journalctl -u airflow_standalone.service -f
```

Parar o serviço e desativar:
```
sudo systemctl stop airflow_tunnel.service
sudo systemctl disable airflow_tunnel.service
sudo systemctl stop airflow_standalone.service
sudo systemctl disable airflow_standalone.service
```
