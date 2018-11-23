package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sns"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	myBucket = "trifacta-matteo-test"
	maxRetries = 10000
)

var (
	namespace = "MatteoS3Test"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("Time elapsed: %2.2f", float64(elapsed) * 1e-6)
}

func putMetric(sess *session.Session, metricName string, value float64, enable bool) error {
	if !enable {
		return nil
	}
	svc := cloudwatch.New(sess, &aws.Config{
		Region: aws.String("us-west-2"),
	})
	_, err := svc.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: &namespace,
		MetricData: []*cloudwatch.MetricDatum{
			{ MetricName: &metricName, Value: &value },
		},
	})
	return err;
}

func doPut(svc *s3.S3, key string, value int64) error {
	reader := strings.NewReader(strconv.FormatInt(value, 10))
	_, err := svc.PutObject(new(s3.PutObjectInput).SetBucket(myBucket).SetKey(key).SetBody(reader))
	return err
}

func doGet(svc *s3.S3, key string) (int64, error) {
	getObjectResponse, err := svc.GetObject(new(s3.GetObjectInput).SetBucket(myBucket).SetKey(key))
	if err != nil {
		return 0, fmt.Errorf("GET failed: ", err)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(getObjectResponse.Body)
	number, err := strconv.ParseInt(buf.String(), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Conversion error: ", err)
	}
	return number, nil
}

func doList(svc *s3.S3, prefix string, n int) (chan string, error) {
	listObjectsV2Response, err := svc.ListObjectsV2(new(s3.ListObjectsV2Input).SetBucket(myBucket).SetPrefix(prefix))
	if err != nil {
		return nil, fmt.Errorf("LIST failed: ", err)
	}
	keys := make(chan string, n)
	for _, content := range listObjectsV2Response.Contents {
		keys <- *content.Key
	}

	for *listObjectsV2Response.IsTruncated {
		listObjectsV2Response, err = svc.ListObjectsV2(new(s3.ListObjectsV2Input).SetBucket(myBucket).SetPrefix(
			prefix).SetContinuationToken(*listObjectsV2Response.NextContinuationToken))
		if err != nil {
			return nil, fmt.Errorf("LIST failed: ", err)
		}

		for _, content := range listObjectsV2Response.Contents {
			keys <- *content.Key
		}
	}
	close(keys)
	return keys, nil
}

func doDelete(svc *s3.S3, prefix string, howMany int) error {
	keys, err := doList(svc, prefix, howMany)
	if err != nil {
		return fmt.Errorf("LIST failed: ", err)
	}

	objectIdentifiers := make([]*s3.ObjectIdentifier, 0)
	for key := range keys {
		objectIdentifiers = append(objectIdentifiers, new(s3.ObjectIdentifier).SetKey(key))
	}

	deleted := 0
	if deleted < howMany {
		deleteObjectsResponse, err := svc.DeleteObjects(
			new(s3.DeleteObjectsInput).SetBucket(myBucket).SetDelete(new(s3.Delete).SetObjects(objectIdentifiers)))
		if err != nil {
			return fmt.Errorf("DELETE failed: ", err)
		}
		deleted = len(deleteObjectsResponse.Deleted)
	}

	return  nil
}

func createTopic(sess *session.Session, name string) (string, error) {
	svc := sns.New(sess)
	createTopicOutput, err := svc.CreateTopic(new(sns.CreateTopicInput).SetName(name))
	if err != nil {
		return "", fmt.Errorf("Failed creating topic", err)
	}
	return *createTopicOutput.TopicArn, nil
}

func deleteTopic(sess *session.Session, arn string) error {
	svc := sns.New(sess)
	_, err := svc.DeleteTopic(new(sns.DeleteTopicInput).SetTopicArn(arn))
	if err != nil {
		return fmt.Errorf("Failed deleting topic", err)
	}
	return nil
}



func updateAndTest(sess *session.Session, counter int64, delay time.Duration, cw bool, key string) error {
	svc := s3.New(sess, &aws.Config{
		Region: aws.String("us-west-2"),
	})

	if err := doPut(svc, key, counter); err != nil {
		return fmt.Errorf("Put error: ", err)
	}

	time.Sleep(delay)

	number, err := doGet(svc, key);

	if err != nil {
		return fmt.Errorf("Get error", err)
	}

	nViolations := 0

	for ; number != counter && nViolations < maxRetries; nViolations++ {
		nViolations++;

		time.Sleep(100 * time.Millisecond)

		number, err = doGet(svc, key);

		if err != nil {
			return err
		}
	}

	if nViolations > 0 {
		log.Printf("consistency violation number=%d != counter=%d, nViolations=%d\n", number, counter, nViolations)
		if err := putMetric(sess, "consistencyViolation", float64(nViolations), cw); err != nil {
			return fmt.Errorf("Error in uploading metric: ", err)
		}
	}

	return nil
}

func listTest(sess *session.Session, n int, counter int64, delay time.Duration, cw bool, key string) error {
	svc := s3.New(sess, &aws.Config{
		Region: aws.String("us-west-2"),
	})

	var wg sync.WaitGroup
	wg.Add(n)
	errorChannel := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			errorChannel <- doPut(svc, fmt.Sprintf("list-test/%d/key-%d", counter, i), 1)
			wg.Done()
		}(i)
	}
	wg.Wait()
	close(errorChannel)

	for err := range errorChannel {
		if err != nil {
			return fmt.Errorf("Error in one of the PUT ", err)
		}
	}

	for nMissing := 1; nMissing > 0; {

		keys, err := doList(svc, fmt.Sprintf("list-test/%d/", counter), n)
		if err != nil {
			return fmt.Errorf("List error ", err)
		}

		nKeys := 0
		keyMap := make(map[string]bool)
		for key := range keys {
			keyMap[key] = true
			nKeys++;
		}

		nMissing = 0
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("list-test/%d/key-%d", counter, i)
			if _, ok := keyMap[key]; ok == false {
				nMissing++;
			}
		}

		//println("=>", nKeys, nMissing)

		if nMissing > 0 {
			log.Printf("consistency violation missing=%d, counter-%d\n", nMissing, counter)
			if err := putMetric(sess, "missingFromList", float64(nMissing), cw); err != nil {
				return fmt.Errorf("Error in uploading metric: ", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	if counter % 100 == 0 {
		log.Println("counter=", counter)
	}

	err := doDelete(svc, fmt.Sprintf("list-test/%d/", counter), n)
	if err != nil {
		return fmt.Errorf("Error with initial delete", err)
	}

	return nil
}

func main() {
	iterations := flag.Int64("iterations", 1, "number of iterations")
	delay := flag.Int64("delay", 0, "delay in milliseconds")
	cwEnabled := flag.Bool("cw", true, "enable cloudwatch")
	key := flag.String("key", "testKey", "key")
	mode := flag.String("mode", "updateTest", "mode of operation for the test: [updateTest " +
		"(default), listTest]")
	nList := flag.Int("n_list", 100, "number of keys for list test (100)")
	flag.Parse()
	log.Printf("iterations=%d\n", *iterations)
	log.Printf("delay=%d\n", *delay)
	log.Printf("cwEnabled=%t\n", *cwEnabled)
	log.Printf("key=%s\n", *key)
	log.Printf("mode=%s\n", *mode)
	log.Printf("nList=%d\n", *nList)

	conf := aws.Config{Region: aws.String("us-west-2")}
	sess, err:= session.NewSession(&conf)
	if err != nil {
		log.Fatal("failed to create session: ", err)
	}

	//topicArn, err := createTopic(sess, "matte-delete-notifications")
	//if err != nil {
	//	log.Fatal("error creating topic")
	//}
	//defer func() {
	//	err = deleteTopic(sess, topicArn)
	//	if err != nil {
	//		log.Fatal("error deleting topic")
	//	}
	//}()

	if err := putMetric(sess, "start", 1.0, *cwEnabled); err != nil {
		log.Fatal(fmt.Errorf("Error in uploading metric: ", err))
	}

	defer func() {
		if *mode == "listTest" {
			/*for i := int64(0); *iterations == -1 || i < *iterations; i++ {
				err := doDelete(s3.New(sess, &aws.Config{
					Region: aws.String("us-west-2"),
				}), fmt.Sprintf("list-test/%d/", i), *nList)
				if err != nil {
					log.Fatal("error deleting at the end", err)
				}
			}*/
		}

		if err := putMetric(sess, "end", 1.0, *cwEnabled); err != nil {
			log.Fatal(fmt.Errorf("Error in uploading metric: ", err))
		}
	}()

	for i := int64(0); *iterations == -1 || i < *iterations; i++ {
		switch *mode {
		case "updateTest":
			err := updateAndTest(sess, i, time.Duration(*delay * 1e6), *cwEnabled, *key)
			if err != nil {
				log.Fatal("updateAndTest is done with error: ", err)
			}
			break;
		case "listTest":
			err := listTest(sess, *nList, i, time.Duration(*delay * 1e6), *cwEnabled, *key)
			if err != nil {
				log.Fatal("listTest is done with error: ", err)
			}
			break;
		}

	}
}
