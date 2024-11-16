import configparser
import os.path

from pyspark import SparkConf

def _handle_env(items: list[tuple[str, str]]) -> dict[str, str]:
    d = {}
    for k, v in items:
        if not (v.startswith("${") and v.endswith("}")):
            d[k] = v
            continue

        default_val_idx = v.find(":")

        var_name = v[2:default_val_idx]  # Extract variable name without "${" and "}"
        env_value = os.environ.get(var_name)

        if env_value is not None:
            d[k] = env_value
        else:
            if default_val_idx != -1:
                d[k] = v[default_val_idx + 1:-1]
    return d


class Config:
    def __init__(self):
        util_dir = os.path.dirname(os.path.abspath(__file__))
        print(f"util_dir: {util_dir}")

        conf = configparser.ConfigParser()
        conf.read(util_dir + "/../conf/spark.conf")

        self.conf = conf
        self.spark_conf = self._get_spark_conf()
        self.kafka_conf = self._get_section_conf("KAFKA_INTERNAL")
        self.postgres_conf = self._get_section_conf('POSTGRES')

    def _get_spark_conf(self):
        spark_conf = SparkConf()

        d = self._get_section_conf("SPARK")

        for k, v in d.items():
            spark_conf.set(k, v)

        return spark_conf
        
    def _get_section_conf(self, section: str) -> dict[str, str]:
        items = self.conf.items(section)
        return _handle_env(items)