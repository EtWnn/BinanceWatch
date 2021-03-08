import json
from typing import Optional, Dict

from src.utils.LoggerGenerator import LoggerGenerator


class CredentialManager:

    logger = LoggerGenerator.get_logger("crendentials_manager")

    @staticmethod
    def get_api_credentials(api_name) -> Optional[Dict]:
        try:
            with open("data/credentials.json") as file:
                credentials = json.load(file)
            return credentials[api_name]
        except FileNotFoundError as ex:
            CredentialManager.logger.error("Could not find the credentials file")
            raise ex
        except KeyError as ex:
            CredentialManager.logger.error(f"there is no key registered in the credentials file  "
                                           f"the name {api_name}")
            raise ex
