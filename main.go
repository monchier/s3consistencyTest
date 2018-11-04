package main

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"strconv"
	"strings"
	"bytes"
	"flag"
	"time"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

const (
	myBucket = "trifacta-matteo-test"
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
	if err != nil {
		return fmt.Errorf("Error uploading metric", err)
	}
	return nil
}

func doPut(svc *s3.S3, key string, value int64) error {
	reader := strings.NewReader(strconv.FormatInt(value, 10))
	_, err := svc.PutObject(new(s3.PutObjectInput).SetBucket(myBucket).SetKey(key).SetBody(reader))
	if err != nil {
		return fmt.Errorf("PUT failed", err)
	}
	return nil
}

func doGet(svc *s3.S3, key string) (int64, error) {
	getObjectResponse, err := svc.GetObject(new(s3.GetObjectInput).SetBucket(myBucket).SetKey(key))
	if err != nil {
		return 0, fmt.Errorf("GET failed", err)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(getObjectResponse.Body)
	number, err := strconv.ParseInt(buf.String(), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Conversion error", err)
	}
	return number, nil
}

func updateAndTest(sess *session.Session, counter int64, delay time.Duration, cw bool, key string) error {
	svc := s3.New(sess, &aws.Config{
		Region: aws.String("us-west-2"),
	})

	if err := doPut(svc, key, counter); err != nil {
		return fmt.Errorf("Put error", err)
	}

	time.Sleep(delay)

	number, err := doGet(svc, key);

	if err != nil {
		return fmt.Errorf("Get error", err)
	}

	nViolations := 0

	for i := 0 ; number != counter && i < 1000; i++ {
		nViolations++;
		time.Sleep(100 * time.Millisecond)
		number, err = doGet(svc, key);

		if err != nil {
			return fmt.Errorf("Get error", err)
		}
	}

	if nViolations > 0 {
		log.Printf("consistency violation number=%d != counter=%d, nViolations=%d\n", number, counter, nViolations)
		if err := putMetric(sess, "consistencyViolation", 1.0, cw); err != nil {
			return fmt.Errorf("Error in uploading metric", err)
		}
	}

	return nil
}

func main() {
	iterations := flag.Int64("iterations", 1, "number of iterations")
	delay := flag.Int64("delay", 0, "delay in milliseconds")
	cwEnabled := flag.Bool("cw", true, "enable cloudwatch")
	key := flag.String("key", "testKey", "key")
	flag.Parse()
	log.Printf("iterations=%d\n", *iterations)
	log.Printf("delay=%d\n", *delay)
	log.Printf("cwEnabled=%t\n", *cwEnabled)
	log.Printf("key=%s\n", *key)

	conf := aws.Config{Region: aws.String("us-west-2")}
	sess, err:= session.NewSession(&conf)
	if err != nil {
		log.Fatal("failed to create session ", err)
	}

	if err := putMetric(sess, "start", 1.0, *cwEnabled); err != nil {
		log.Fatal(fmt.Errorf("Error in uploading metric", err))
	}

	for i := int64(0); *iterations == -1 || i < *iterations; i++ {
		err := updateAndTest(sess, i, time.Duration(*delay * 1e6), *cwEnabled, *key)
		if err != nil {
			log.Fatal("done with error", err)
		}
	}

	if err := putMetric(sess, "end", 1.0, *cwEnabled); err != nil {
		log.Fatal(fmt.Errorf("Error in uploading metric", err))
	}
}
