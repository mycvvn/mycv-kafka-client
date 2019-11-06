<?php

namespace MyCVKafka;

class Producer {
    private static $instance;

    public static function getInstance()
    {
        if (self::$instance) {
            return self::$instance;
        }
        self::$instance = new Producer();
        return self::$instance;
    }

    public function __construct()
    {
        $this->conf = new \RdKafka\Conf();
        $this->conf->set('metadata.broker.list', env('KAFKA_BROKER_LIST'));
        $this->producer = new \RdKafka\Producer($this->conf);
    }

    private function send($partition, $message)
    {
        $sendData = $message;
        if (\is_array($message)) {
            $sendData = \json_encode($message);
        }

        $this->topic->produce(RD_KAFKA_PARTITION_UA, $partition, $sendData);
        $this->producer->poll(0);
    }

    public function sendMessage($topic, $partition, $message)
    {
        $this->topic = $this->producer->newTopic($topic);
        $this->send($partition, $message);

        $result = $this->producer->flush(10000);
        $this->handleException($result);
    }

    public function sendMessages($topic, $partition, $messages)
    {
        if (\is_array($messages)) {
            $totalMessages = count($messages);
            
            if ($totalMessages > 0) {
                $this->topic = $this->producer->newTopic($topic);

                foreach($messages as $message) {
                    $this->send($partition, $message);
                }
                
                for ($flushRetries = 0; $flushRetries < $totalMessages; $flushRetries++) {
                    $result = $this->producer->flush(10000);
                    if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                        break;
                    }
                }
    
                $this->handleException($result);
            }
        }
    }

    private function handleException($result)
    {
        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            throw new \RuntimeException('Was unable to flush, messages might be lost!');
        }
    }
}
