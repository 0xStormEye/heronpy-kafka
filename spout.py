from heronpy.api.spout.spout import Spout
from confluent_kafka import Consumer, KafkaError

class KafkaSpout(Spout):
  # Default output field name
  outputs = ['data']

  # Spout initialization
  def initialize(self, config, context):
    self.log("[+] Initializing KafkaSpout...")

    # Initializing Kafka consumer handle
    self.CONFLUENT_CONSUMER = Consumer({
      'bootstrap.servers': 'kafka1:9091,kafka2:9092', # Kafka broker list
      'security.protocol': 'SSL', # Enables SSL, optional values: 'PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'
      'ssl.ca.location': '/opt/kafka.truststore.pub', # Public key of Kafka server, used for verification
      'ssl.certificate.location': '/opt/heron.keystore.pub', # Public key of Kafka client SSL
      'ssl.key.location' : '/opt/heron.keystore.pem', # Private key of Kafka client SSL
      'ssl.key.password' : 'P@s5w0rD#', # Decryption password of Kafka client SSL private key
      'group.id': 'heron-group', # Indicator of consumer group
      'client.id': 'heron-client', # Indicator of client name
      'default.topic.config': {
        'auto.offset.reset': 'earliest' # Earliest as in earliest available offset from the last commit offset. optional values: 'earliest' 'latest'
      }
    })

    self.CONFLUENT_CONSUMER.subscribe('heron-topic') # Subscribe to specific Kafka topic
    self.log("[+] Kafka Consumer Initialized.")

  # Looping
  def next_tuple(self):
    msg = self.CONFLUENT_CONSUMER.poll(0.1) # poll() means getting data, 0.1 here is the timeout *second*.

    if msg is None: # No new data to consume
      return # Return to the loop
    if msg.error(): # Error getting data
      if msg.error().code() == KafkaError._PARTITION_EOF: # End Of File of topic, which is fine!
        return
      else:
        self.log("[*] [ERROR] " + str(msg.error())) # Feed Me Error Messages!
      return

    self.emit([msg.value()]) # Emit data for bolts