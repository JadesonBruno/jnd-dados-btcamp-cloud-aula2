from .aws.client import S3Client


def main():
    """
    Função principal para executar o CRUD no S3.
    """
    # Cria uma instância do cliente AWS
    client = S3Client()

    # Cria um bucket S3
    client.create_bucket("bucket-crud-teste")

    # Lista os buckets disponíveis
    client.list_buckets()

    # Faz o upload de um arquivo para o bucket S3
    client.upload_file("download/YHWH.txt", "bucket-crud-teste")

    # Faz o download de um arquivo do bucket S3
    client.download_file("bucket-crud-teste", "michael_lucifer.jpeg", "download/michael_lucifer.jpeg")

    # Deleta um arquivo do bucket S3
    client.delete_file("bucket-crud-teste", "michael_lucifer.jpeg")


if __name__ == "__main__":
    main()
