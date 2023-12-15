/* Disk management using Kafka Application*/

#include <stdio.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
#include <errno.h>
#include <stdlib.h>

int main(int argc, char **argv)
{
    rd_kafka_t *rk;             /* Kafka producer instance handle */
    rd_kafka_conf_t *conf;      /* Temporary configuration object */
    char errstr[512];           /* librdkafka API error reporting buffer */
    const char *brokers;        /* Kafka broker(s) */
    const char *topic;          /* Kafka topic to produce to */
    char *msg = "Hello, Kafka!";/* Message to send */
    size_t msg_len = strlen(msg);/* Length of message */
    int partition = RD_KAFKA_PARTITION_UA; /* Kafka partition to produce to */
    int max_msg_size = 100000;  /* Maximum size of Kafka messages */
    char *output_file = "index.txt"; /* Output file name */
    FILE *fp;                   /* File pointer */

    /* Check arguments */
    if (argc != 2)
    {
        fprintf(stderr, "Usage: %s <brokers>\n", argv[0]);
        return 1;
    }
    brokers = argv[1];
    topic = "my_topic";

    /* Set up Kafka configuration */
    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "Error setting broker list: %s\n", errstr);
        return 1;
    }

    /* Create Kafka producer instance */
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk)
    {
        fprintf(stderr, "Error creating Kafka producer: %s\n", errstr);
        return 1;
    }

    /* Start producing messages */
    while (1)
    {
        rd_kafka_resp_err_t err;
        rd_kafka_topic_t *rkt;

        /* Create Kafka topic */
        rkt = rd_kafka_topic_new(rk, topic, NULL);
        if (!rkt)
        {
            fprintf(stderr, "Error creating Kafka topic: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
            rd_kafka_destroy(rk);
            return 1;
        }

        /* Produce message */
        err = rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY, msg, msg_len, NULL, 0, NULL);
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            fprintf(stderr, "Error producing message: %s\n", rd_kafka_err2str(err));
            rd_kafka_topic_destroy(rkt);
            rd_kafka_destroy(rk);
            return 1;
        }

        /* Wait for message delivery */
        rd_kafka_poll(rk, 0);

        /* Destroy Kafka topic */
        rd_kafka_topic_destroy(rkt);

        /* Write message contents to output file */

        FILE *index_file, *file;
        char filename[50], content[100], input[100];
        int file_count = 0;

        // Open the index file for writing
        index_file = fopen("index.txt", "w");

        // Loop to allow the user to input text into multiple files
        while(1)
        {
                // Generate a filename for the new file
                sprintf(filename, "file%d.txt", file_count);

                // Open the new file for writing
                file = fopen(filename, "w");

                // Write the user's input to the new file
                fprintf(file, "%s\n", msg);

                // Close the new file
                fclose(file);

                // Write the new filename to the index file
                fprintf(index_file, "File %d: %s\n", file_count, filename);

                // Increment the file count
                file_count++;
        }

        // Close the index file
        fclose(index_file);

        // Open the index file for reading
        index_file = fopen("index.txt", "r");

        // Read the contents of the index file and display them
        while(fscanf(index_file, "File %*d: %s\n", filename) == 1)
        {
                // Open the referenced file for reading
                file = fopen(filename, "r");

                // Read the contents of the referenced file and display them
                printf("Contents of %s:\n", filename);
                while(fgets(content, 100, file) != NULL)
                {
                         printf("%s", content);
                }

                // Close the referenced file
                fclose(file);
        }

        // Close the index file
        fclose(index_file);

        /* Sleep for a second before sending the next message */
        //sleep(1);
    }

    /* Destroy Kafka producer instance */
    rd_kafka_destroy(rk);

    return 0;
}