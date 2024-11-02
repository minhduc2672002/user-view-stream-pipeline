from pyspark.sql import SparkSession


class Log4j:
    def __init__(self, spark: SparkSession):
        root_class = "unigap.spark.examples"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        log4j = spark._jvm.org.apache.log4j
        logger = log4j.LogManager.getLogger(root_class + "." + app_name)
        self.logger = logger

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warn(self, message):
        self.logger.warn(message)

    def error(self, message):
        self.logger.error(message)
