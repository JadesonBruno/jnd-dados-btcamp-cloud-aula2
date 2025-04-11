# Import libs nativas
import os
import sys

# Import libs terceros
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configurando credenciais AWS
s3 = boto3.client("s3")
bucket_name = "bucket-crud-boto3"


class S3Client:
    """
    Classe para interagir com o serviço S3 da AWS.
    """

    def __init__(self):
        self._envs = {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "aws_region": os.getenv("AWS_REGION"),
            "aws_bucket_name": os.getenv("AWS_BUCKET_NAME"),
        }

        for key, value in self._envs.items():
            if value is None:
                print(f"Erro: variável de ambiente {key} não encontrada.")
                sys.exit(1)

        self.session = boto3.Session(
            aws_access_key_id=self._envs["aws_access_key_id"],
            aws_secret_access_key=self._envs["aws_secret_access_key"],
            region_name=self._envs["aws_region"],
        )

        try:
            # Cria o recurso e cliente S3
            self.s3 = self.session.client("s3")
        except NoCredentialsError:
            print("Erro: credenciais AWS não encontradas.")
            sys.exit(1)
        except Exception as e:
            print(f"Erro ao configurar o cliente S3: {e}")
            sys.exit(1)

    def create_bucket(self, bucket_name):
        """
        Cria um bucket no S3 com o nome especificado.
        """
        try:
            self.s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": self._envs["aws_region"]})
            print(f"Bucket {bucket_name} criado com sucesso.")
        except Exception as e:
            print(f"Erro ao criar o bucket {bucket_name}: {e}")
            raise

    def list_buckets(self):
        """
        Lista todos os buckets disponíveis na conta AWS.
        """
        try:
            response = self.s3.list_buckets()
            print("Buckets disponíveis:")
            for bucket in response["Buckets"]:
                print(f"  - {bucket['Name']}")
        except Exception as e:
            print(f"Erro ao listar os buckets: {e}")
            raise

    def upload_file(self, file_name, bucket_name, object_name=None):
        """
        Faz o upload de um arquivo para um bucket S3.
        """
        if object_name is None:
            object_name = os.path.basename(file_name)

        try:
            self.s3.upload_file(file_name, bucket_name, object_name)
            print(f"Arquivo {file_name} enviado com sucesso para o bucket {bucket_name}.")
        except Exception as e:
            print(f"Erro ao enviar o arquivo {file_name} para o bucket {bucket_name}: {e}")
            raise

    def download_file(self, bucket_name, object_name, file_name):
        """
        Faz o download de um arquivo de um bucket S3.
        """
        try:
            self.s3.download_file(bucket_name, object_name, file_name)
            print(f"Arquivo {object_name} baixado com sucesso do bucket {bucket_name}.")
        except Exception as e:
            print(f"Erro ao baixar o arquivo {object_name} do bucket {bucket_name}: {e}")
            raise

    def delete_file(self, bucket_name, object_name):
        """
        Deleta um arquivo de um bucket S3.
        """
        try:
            self.s3.delete_object(Bucket=bucket_name, Key=object_name)
            print(f"Arquivo {object_name} deletado com sucesso do bucket {bucket_name}.")
        except Exception as e:
            print(f"Erro ao deletar o arquivo {object_name} do bucket {bucket_name}: {e}")
            raise

    def delete_bucket(self, bucket_name):
        """
        Deleta um bucket S3.
        """
        try:
            self, s3.delete_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} deletado com sucesso.")
        except Exception as e:
            print(f"Erro ao deletar o bucket {bucket_name}: {e}")
            raise

    def permissions_acess(self, bucket_name, key, acl):
        """
        Define as permissões de acesso para o bucket S3.
        """
        try:
            self.s3.put_object_acl(Bucket=bucket_name, Key=key, ACL=acl)
            print(f"Permissões de acesso definidas para o bucket {bucket_name}.")
        except Exception as e:
            print(f"Erro ao definir permissões de acesso para o bucket {bucket_name}: {e}")
            raise

    def policy_acess(self, bucket_name, policy):
        """
        Define a política de acesso para o bucket S3.
        """
        try:
            self.s3.put_bucket_policy(Bucket=bucket_name, Policy=policy)
            print(f"Política de acesso definida para o bucket {bucket_name}.")
        except Exception as e:
            print(f"Erro ao definir política de acesso para o bucket {bucket_name}: {e}")
            raise
