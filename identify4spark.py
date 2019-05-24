#!/usr/bin/python3
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import time
from kafka import KafkaProducer

def identify(inputKeyValue):
    try:
        predictPicture=inputKeyValue[1]
        # list all types what the model identified
        # REMEMBER LABELS in your model have to be defined as following in this .py
        # 「tshirt -> 0」; 「shirt -> 1」; 「suit -> 2」; 「dress -> 3」; 「vest -> 4」;
        types = ["T-shirt", "Shirt", "Suit", "Dress", "Vest"]

        # load the "MODEL" what you want
        from keras.models import load_model
        global onion
        if onion is None:
            onion = load_model("clothes5_4310.h5")  # "./modules/clothes5_4310.h5"

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
        identifyResult=types[onion.predict(xs).argmax(axis=-1)[0]]
        outputKeyValue=(inputKeyValue[0], identifyResult)
    except:
        outputKeyValue = (inputKeyValue[0], "ERROR VALUES! Please input the value with the picture in base64.b64encode bytes type!")

    return outputKeyValue

def output_partition(partition):
    # Create producer
    producer = KafkaProducer(bootstrap_servers=broker_list)
    for p in partition:
        result = "({},{})".format(p[0], p[1])
        producer.send(output_topic, value=bytes(result, "utf8"))
    producer.close()



if __name__ == "__main__":
    sc = SparkContext()
    ssc = StreamingContext(sc, 1)

    onion=None
    ipkafka4ZK = "172.21.0.5:2181"
    input_topic = "onionTopic1"

    ipkafka4Br1 = "172.21.0.6:9092"
    ipkafka4Br2 = "172.21.0.7:9092"
    ipkafka4Br3 = "172.21.0.8:9092"
    broker_list = [ipkafka4Br1] # broker_list = [ipkafka4Br1, ipkafka4Br2, ipkafka4Br2]
    output_topic = "onionTopic2"


    raw_stream = KafkaUtils.createStream(ssc, ipkafka4ZK, "consumer-group", {input_topic: 3})

    result = raw_stream.map(identify)
    result.cache()

    result.pprint(20)
    result.foreachRDD(lambda rdd: rdd.foreachPartition(output_partition))


    # Start it
    ssc.start()
    ssc.awaitTermination()


#========================= ( CMD 4 running this file(identify4spark.py) in Spark Streaming )
# spark-submit --master spark://172.21.0.2:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 --files modules/clothes5_4310.h5 identify4spark.py
# spark-submit --master spark://172.21.0.2:7077 --executor-memory 5G --executor-cores 3 --driver-memory 5G --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1 --files modules/clothes5_4310.h5 identify4spark.py

