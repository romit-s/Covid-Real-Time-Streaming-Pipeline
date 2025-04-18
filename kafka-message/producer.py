from confluent_kafka import Producer


class hospital_producer:
    def __init__(self):
        self.topic = 'hospital'
        self.conf = {'bootstrap.servers': '<your-server>',
                     'security.protocol': 'SASL_SSL',
                     'sasl.mechanism': 'PLAIN',
                     'sasl.username': '<your-username>',
                     'sasl.password': '<your-password>',
                     'client.id': 'romit-local'}
        
    def delivery_callback(self, err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            key = msg.key().decode('utf-8')
            print(f"Produced event to : key = {key}")
        
    def produce_cases(self,producer):
        with open("cases_deaths.csv") as lines:
            counter =0
            for line in lines:
                id = line.split(",")[0]+"_"+line.split('"')[0].split(",")[-3]
                producer.produce(self.topic, key = id, value= line, callback= self.delivery_callback)
                counter +=1
                if counter %10:
                    producer.poll(1)

    def start(self):
        kafka_producer = Producer(self.conf)
        self.produce_invoices = self.produce_cases(kafka_producer)
        kafka_producer.flush(10)

if __name__ == "__main__":
    invoice_producer = hospital_producer()
    invoice_producer.start()

        