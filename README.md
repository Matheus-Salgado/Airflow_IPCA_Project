# ETL para IPCA e Preço do Boi Gordo com Airflow e Docker

## Introdução
Este projeto implementa um processo de ETL usando Apache Airflow e Docker para coletar, transformar e enriquecer dados 
do IPCA (Índice Nacional de Preços ao Consumidor Amplo) e informações relacionadas ao preço da commodity boi gordo, obtidas a 
partir da API do Banco Central e dos dados disponibilizados pelo CEPEA (Centro de Estudos Avançados em Economia Aplicada).

Para estruturar o pipeline de forma eficiente e escalável, a arquitetura segue o padrão medallion, que é muito utilizada para dividir as transformações em camadas e manter a rastreabilidade das execuções.

## Pré-Requisitos

- **Docker Desktop**: O Docker Desktop possibilita a criação e o gerenciamento de contêineres Docker de maneira isolada e consistente entre diferentes máquinas. Ele já inclui o Docker Compose, possibilitando a orquestração dos múltiplos contêineres necessários para o projeto. O link de download e instruções para instalação estão [disponíveis aqui](https://www.docker.com/get-started/).

## Como Executar o Processo


Para iniciar o projeto e executar o processo de ETL, siga os passos abaixo:

1. **Clonar o repositório**  
   Clone o repositório do GitHub para a sua máquina local usando o comando:
   ```bash
   git clone https://github.com/Matheus-Salgado/Airflow_IPCA_Project.git

2. **Iniciar o Docker Compose**  
   Com o Docker Desktop em execução, navegue até o diretório do projeto e utilize o Docker Compose para subir os contêineres necessários:
   
   ```bash
   docker-compose up --build

Esse comando irá:
- Subir todos os serviços do Apache Airflow necessários para o processo de ETL;
- Carregar e aplicar as variáveis de ambiente definidas no arquivo .env;
- Configurar os diretórios de origem e destino dos dados, garantindo o fluxo correto das informações durante o processo.

3. **Executar a DAG**  
   Acesse a interface do Airflow em seu navegador, disponível em `http://localhost:8080`.
   
 - Faça o login:
   - Usuário: airflow
   - Senha: airflow
 - Execute a DAG dag_boi_gordo.

Após a execução do processo, os arquivos na pasta ./dados serão atualizados.

4. **Parar o Docker Compose**  
   Após a execução e validação do processo, os contêineres podem ser parados através do seguinte código na raiz do projeto:

   ```bash
   docker-compose down


