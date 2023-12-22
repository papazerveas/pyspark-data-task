from typing import Dict, Union
import opendatasets as od
import yaml


def get_config() -> Dict:
    with open('config.yml', 'r') as file:
        return yaml.safe_load(file)


class Dataset():

    def __init__(self) -> None:
        self.__config: Union[Dict, None] = get_config().get("data")

    def download(self) -> None:
        """kaggle datasets download -d olistbr/brazilian-ecommerce
        """
        od.download(self.__config.get(
            "kaggle", "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"))
