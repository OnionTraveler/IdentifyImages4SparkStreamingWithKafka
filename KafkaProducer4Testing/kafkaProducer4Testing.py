#!/usr/bin/python3
# you must 「pip3 install confluent-kafka」
# REMEMBER change "suitable Kafka_broker_IP(ipkafka4Br1)" & "which TOPIC(topicName) you want send" before running this .py file!!!
from confluent_kafka import Producer
import sys


# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)

# 主程式進入點
if __name__ == '__main__':
    # 步驟0. 從本地端開檔匯入一張圖片
    import base64
    with open("../IdentifyImages4SingleMachine/identifiedData/Brain_suit.jpg","rb") as f:  # /home/onion/onion/pro4clothes/IdentifyImages4SparkStreamingWithKafka/IdentifyImages4SingleMachine/identifiedData/Brain_suit.jpg
        data=base64.b64encode(f.read())

    # 步驟1. 設定要連線到Kafka集群的相關設定
    ipkafka4Br1 = "172.20.0.4:9092"  # "172.20.0.4:9092"
    props = {
        # Kafka集群在那裡?
        'bootstrap.servers': ipkafka4Br1,  # 要連接的Kafka集群 (若為Producer，則須將資料produce到Kafka叢集中的任一brokers; 若為Consumer，則須從Kafka叢集中的zookeeper將資料consume回來)
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)

    # 步驟3. 指定想要發佈訊息的topic名稱
    topicName = 'onionTopic1'

    # 步驟4. 送出(produce)訊息
    produceMessageKey="onion"
    produceMessageValue=data
    try:
        # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])
        producer.produce(topicName, key=produceMessageKey, value=produceMessageValue)

        print("MESSAGE TYPE: {}; MESSAGE CONTENT: {}".format(type(produceMessageValue), produceMessageValue))
        print('Produce the message(Image) to Kafka successfully!')
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'.format(len(producer)))
    except Exception as e:
        print(e)

    # 步驟5. 確認所在Buffer的訊息都己經送出去給Kafka了
    producer.flush()

