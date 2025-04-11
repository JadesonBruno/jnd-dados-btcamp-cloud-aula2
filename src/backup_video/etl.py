# Import libs nativas
import os
from typing import List

# Import libs de terceiros
import boto3
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Carrega as variáveis de ambiente
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# Configura o cliente S3
try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
except Exception as e:
    print(f"Erro ao configurar o cliente S3: {e}")
    raise e


# Lê os arquivos de um diretório local
def list_files(folder: str) -> List[str]:
    """
    Lista os arquivos em um diretório local.
    """
    try:
        files: List[str] = []
        for name_file in os.listdir(folder):
            complete_path = os.path.join(folder, name_file)
            if os.path.isfile(complete_path):
                files.append(complete_path)
    except Exception as e:
        print(f"Erro ao listar os arquivos no diretório {folder}: {e}")
        raise
    return files


# Carrega os arquivos no S3
def upload_files_s3(files: List[str]) -> None:
    """
    Faz o upload dos arquivos para o S3.
    """

    for file in files:
        try:
            file_name: str = os.path.basename(file)
            s3_client.upload_file(file, AWS_BUCKET_NAME, file_name)
            print(f"\nArquivo {file_name} enviado com sucesso para o S3.")
        except Exception as e:
            print(f"\nErro ao enviar o arquivo {file_name} para o S3: {e}")
            raise


# Deleta os arquivos do Diretório local
def delete_files(files: List[str]) -> None:
    """
    Deleta os arquivos do diretório local.
    """
    for file in files:
        try:
            os.remove(file)
            print(f"Arquivo {file} deletado com sucesso.")
        except Exception as e:
            print(f"Erro ao deletar o arquivo {file}: {e}")
            raise
