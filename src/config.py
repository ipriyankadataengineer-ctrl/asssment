"""
SkyPoints Loyalty Pipeline - Configuration and Schema Registry
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'
SQL_DIR = BASE_DIR / 'sql'

# Azure ADLS Gen2 Configuration
AZURE_SUBSCRIPTION_ID = os.getenv('AZURE_SUBSCRIPTION_ID', '9309ca0f-f25f-47e2-b887-bb4b9af961bf')
AZURE_RESOURCE_GROUP = os.getenv('AZURE_RESOURCE_GROUP', 'rg-skypoints-loyalty')
AZURE_STORAGE_ACCOUNT = os.getenv('AZURE_STORAGE_ACCOUNT', 'stskypointslake')
AZURE_LOCATION = os.getenv('AZURE_LOCATION', 'centralindia')

# Medallion Containers
CONTAINER_LANDING = 'landing'
CONTAINER_BRONZE = 'bronze'
CONTAINER_SILVER = 'silver'
CONTAINER_GOLD = 'gold'

# Mandatory columns for member profiles (Deliverable 5)
MANDATORY_MEMBER_FIELDS = ['member_id', 'member_name', 'enrollment_date']

# Business Logic Parameters
STALE_DAYS_THRESHOLD = 90

# Standardized Country Code Mappings
COUNTRY_NORMALIZATION = {
    'USA': 'USA',
    'IND': 'IND',
    'CAN': 'CAN',
    'PHIL': 'PHIL',
    'AU': 'AU'
}
