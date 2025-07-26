import logging



pipeline_logger = logging.getLogger("Pipeline")
pipeline_logger.setLevel(logging.INFO)

pipeline_file_handler = logging.FileHandler('Pipeline.log')
pipeline_file_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s [%(filename)s:%(lineno)d] %(message)s"
)
pipeline_file_handler.setFormatter(formatter)

pipeline_logger.handlers = []
pipeline_logger.propagate = False
pipeline_logger.addHandler(pipeline_file_handler)