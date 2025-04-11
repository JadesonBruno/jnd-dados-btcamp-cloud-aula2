# Import libs nativas
from typing import List

# Import módulos nativos
from .etl import delete_files, list_files, upload_files_s3


# Pipeline
def pipeline(folder: str) -> None:
    """
    Pipeline para fazer o upload dos arquivos para o S3 e deletar os arquivos locais.
    """
    try:
        print(f"Iniciando o processo de backup para a pasta '{folder}'...")
        files: List[str] = list_files(folder)
        if files:
            upload_files_s3(files)
            delete_files(files)
        else:
            print("Nenhum arquivo encontrado na pasta.")
    except Exception as e:
        print(f"\nErro na pipeline: {e}")
        raise e


if __name__ == "__main__":
    # Define o diretório local onde os arquivos estão armazenados
    FOLDER: str = "download"
    # Executa o pipeline
    try:
        pipeline(FOLDER)
    except Exception as e:
        print(f"\nErro ao executar o pipeline: {e}")
