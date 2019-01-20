from heronpy.api.bolt.bolt import Bolt
from confluent_kafka import Producer, KafkaError
import json

class OutputKafkaBolt(Bolt):
  outputs = ['data']
  def initialize(self, config, context):
    self.log("[+] Initializing OutputKafkaBolt...")

    # Initializing Kafka Producer, the config here is just copied from Consumer, but changing group.id and client.id
    self.kafkaProducer = Producer({
      'bootstrap.servers': 'kafka1:9091,kafka2:9092', # Kafka broker list
      'security.protocol': 'SSL', # Enables SSL, optional values: 'PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'
      'ssl.ca.location': '/opt/kafka.truststore.pub', # Public key of Kafka server, used for verification
      'ssl.certificate.location': '/opt/heron.keystore.pub', # Public key of Kafka client SSL
      'ssl.key.location' : '/opt/heron.keystore.pem', # Private key of Kafka client SSL
      'ssl.key.password' : 'P@s5w0rD#', # Decryption password of Kafka client SSL private key
      'group.id': 'heron-group-output', # Indicator of consumer group
      'client.id': 'heron-client-output', # Indicator of client name
      'default.topic.config': {
        'auto.offset.reset': 'earliest' # Earliest as in earliest available offset from the last commit offset. optional values: 'earliest' 'latest'
      }
    })

  def delivery_report(self, err, msg):
    """ Called once for each message produced to indicate delivery result.
      Triggered by poll() or flush(). """
    if err is not None:
      self.log('[*] Message Delivery Error: {}'.format(err))
      self.log('[*] Message Delivery: {}'.format(msg))


  def process(self, tup): # Processing incoming data
    outputJson = json.dumps(tup.values[0]) # Transforming incoming data into JSON

    self.kafkaProducer.produce('heron-parsed', outputJson, callback=self.delivery_report) # Produce JSON data into Kafka topic
    self.kafkaProducer.flush() # A flush, to make sure you flush the toilet..
