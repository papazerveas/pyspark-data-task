from typing import Dict, Union
import opendatasets as od
import yaml


def get_config(yml_file = 'config.yml') -> Dict:
    with open(yml_file, 'r') as file:
        return yaml.safe_load(file)


class Dataset():

    def __init__(self, config = get_config()) -> None:
        self.__config: Union[Dict, None] = config.get("data")

    def download(self) -> None:
        """kaggle datasets download -d olistbr/brazilian-ecommerce
        """
        od.download(self.__config.get(
            "kaggle", "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"))
