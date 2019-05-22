#!/usr/bin/python3
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def identify(predictPicture):
    # list all types what the model identified
    # REMEMBER LABELS in your model have to be defined as following in this .py
    # 「tshirt -> 0」; 「shirt -> 1」; 「suit -> 2」; 「dress -> 3」; 「vest -> 4」;
    types = ["T-shirt", "Shirt", "Suit", "Dress", "Vest"]

    # load the "MODEL" what you want
    from keras.models import load_model
    onion = load_model("clothes5_4310.h5")  # "./modules/clothes5_4310.h5"

    import time
    time.sleep(1)

    import base64
    data=base64.b64decode(predictPicture)

    from PIL import Image
    import io
    import numpy as np
    fn=Image.open(io.BytesIO(data)).resize((224,224))

    # 再用的時候所有東西都轉成np array
    from keras.applications.vgg16 import preprocess_input
    imglist = []
    imglist.append(preprocess_input(np.array(fn)))
    xs = np.array(imglist)

    time.sleep(1)  # 休息1秒
    # Identification Result
    return types[onion.predict(xs).argmax(axis=-1)[0]]

if __name__ == "__main__":
    sc = SparkContext()
    ssc = StreamingContext(sc, 5)

    ipkafka4ZK = "172.20.0.2"
    raw_stream = KafkaUtils.createStream(ssc, ipkafka4ZK + ":2181", "consumer-group", {"onionTopic1": 3})

    result = raw_stream.map(lambda x: x[1]).map(identify)
    result.pprint()

    # Start it
    ssc.start()
    ssc.awaitTermination()


#========================= ( CMD 4 running this file(identify4spark.py) in Spark Streaming )
# spark-submit --master spark://172.21.0.2:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 --files modules/clothes5_4310.h5 identify4spark.py
