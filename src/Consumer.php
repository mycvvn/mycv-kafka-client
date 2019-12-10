<?php

namespace MyCVKafka;

class Consumer {
    public static $instance;

    public static function getInstance()
    {
        if (self::$instance) {
            return self::$instance;
        }
        self::$instance = new Consumer();
        return self::$instance;
    }

    public function __construct()
    {
        echo "Kafka consumer starting...\n";
        $this->conf = new \RdKafka\Conf();
    }

    public function setGroup($group)
    {
        echo "Registered group.id `$group`\n";
        $this->conf->set('group.id', $group);
        return $this;
    }

    public function configure()
    {
        echo "Consumer & TopicConf configuring...\n";
        $this->rk = new \RdKafka\Consumer($this->conf);
        $this->rk->addBrokers(env('KAFKA_BROKER_LIST'));

        $this->queue = $this->rk->newQueue();

        $this->topicConf = new \RdKafka\TopicConf();
        $this->topicConf->set('auto.commit.interval.ms', 100);
    
        $this->topicConf->set('offset.store.method', 'broker');
        $this->topicConf->set('auto.offset.reset', 'smallest');
        return $this;
    }

    public function setTopic($topic, $alias)
    {
        echo "Registered topic `$topic`\n";
        $this->$alias = $this->rk->newTopic($topic, $this->topicConf);
        $this->$alias->consumeQueueStart(0, \RD_KAFKA_OFFSET_STORED, $this->queue);
        return $this;
    }

    public function listeningMessage(callable $callback)
    {
        while (true) {
            $message = $this->queue->consume(120 * 1000);
            
            if ($message) {
                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        $this->handleListeningCallback($callback, $message);
                        break;
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        echo "No more messages; will wait for more\n";
                        break;
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        echo "Timed out\n";
                        break;
                    default:
                        throw new \Throwable($message->errstr(), $message->err);
                        break;
                }
            }
        }
    }

    private function handleListeningCallback(callable $callback, $message)
    {
        $payload = new \stdClass();
        $payloadJson = \json_decode($message->payload);
        $payload->payload = $payloadJson ? $payloadJson : $message->payload;
        
        $callback($message->topic_name, $message->partition, $payload);
    }
}
