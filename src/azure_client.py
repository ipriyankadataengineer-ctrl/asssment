"""
Azure Storage / ADLS Gen2 Client
Handles secure authentication, container initialization, and blob/delta uploads.
"""
import os
import json
from pathlib import Path
from typing import Optional
from azure.storage.blob import BlobServiceClient
from src.config import AZURE_STORAGE_ACCOUNT

class AzureLakehouseClient:
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initializes ADLS Gen2 / Blob client.
        Uses connection string if provided, or environment variable AZURE_STORAGE_CONNECTION_STRING.
        """
        self.conn_str = connection_string or os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.client = None
        if self.conn_str:
            try:
                self.client = BlobServiceClient.from_connection_string(self.conn_str)
            except Exception as e:
                print(f"[Azure Warning] Could not initialize client: {e}")

    def ensure_containers(self, container_names: list) -> bool:
        """Creates medallion containers if they do not exist."""
        if not self.client:
            return False
        for name in container_names:
            try:
                container_client = self.client.get_container_client(name)
                if not container_client.exists():
                    container_client.create_container()
                    print(f"Container created: {name}")
                else:
                    print(f"Container exists: {name}")
            except Exception as e:
                print(f"Error ensuring container {name}: {e}")
                return False
        return True

    def upload_file(self, container_name: str, local_file_path: str, blob_name: str) -> bool:
        """Uploads a local file to the specified Azure container."""
        if not self.client:
            print("[Azure Info] Client not configured. Running in offline/simulation mode.")
            return False
        try:
            blob_client = self.client.get_blob_client(container=container_name, blob=blob_name)
            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            print(f"[Azure Success] Uploaded {local_file_path} -> {container_name}/{blob_name}")
            return True
        except Exception as e:
            print(f"[Azure Error] Upload failed for {blob_name}: {e}")
            return False

    def upload_json_records(self, container_name: str, records: list, blob_name: str) -> bool:
        """Serializes and uploads in-memory records as JSON to Azure container."""
        if not self.client:
            return False
        try:
            payload = json.dumps(records, default=str, indent=2)
            blob_client = self.client.get_blob_client(container=container_name, blob=blob_name)
            blob_client.upload_blob(payload, overwrite=True)
            print(f"[Azure Success] Uploaded table data -> {container_name}/{blob_name}")
            return True
        except Exception as e:
            print(f"[Azure Error] Failed to upload {blob_name}: {e}")
            return False
