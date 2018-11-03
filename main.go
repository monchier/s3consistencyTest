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
	"github.com/caio/go-tdigest"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

const (
	myBucket = "trifacta-matteo-test"
)
var (
	namespace = "MatteoS3Test"
)

func timeTrack(start time.Time, name string, digest *tdigest.TDigest) {
	elapsed := time.Since(start)
	digest.Add(float64(elapsed.Nanoseconds()))
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

func updateAndTest(sess *session.Session, counter int, digest *tdigest.TDigest, delay time.Duration, cw bool, key string) error {
	defer timeTrack(time.Now(), "updateAndtest", digest)
	svc := s3.New(sess, &aws.Config{
		Region: aws.String("us-west-2"),
	})
	reader := strings.NewReader(strconv.Itoa(counter))
	_, err := svc.PutObject(new(s3.PutObjectInput).SetBucket(myBucket).SetKey(key).SetBody(reader))
	if err != nil {
		return fmt.Errorf("PUT failed", err)
	}

	time.Sleep(delay)

	getObjectResponse, err := svc.GetObject(new(s3.GetObjectInput).SetBucket(myBucket).SetKey(key))
	if err != nil {
		return fmt.Errorf("GET failed", err)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(getObjectResponse.Body)
	number, err := strconv.Atoi(buf.String())
	if err != nil {
		return fmt.Errorf("Conversion error", err)
	}
	if number != counter {
		log.Printf("consistency violation number=%d != counter=%d ", number, counter)
		if err := putMetric(sess, "consistencyViolation", 1.0, cw); err != nil {
			return fmt.Errorf("Error in uploading metric", err)
		}
	}
	return nil
}

func main() {
	t, _ := tdigest.New()
	iterations := flag.Int("iterations", 1, "number of iterations")
	delay := flag.Int64("delay", 0, "delay in milliseconds")
	cwEnabled := flag.Bool("cw", true, "enable cloudwatch")
	key := flag.String("key", "testKey", "key")
	flag.Parse()
	fmt.Printf("iterations=%d\n", *iterations)
	fmt.Printf("delay=%d\n", *delay)
	fmt.Printf("cwEnabled=%t\n", *cwEnabled)
	fmt.Printf("key=%s\n", *key)

	conf := aws.Config{Region: aws.String("us-west-2")}
	sess, err:= session.NewSession(&conf)
	if err != nil {
		log.Fatal("failed to create session ", err)
	}

	if err := putMetric(sess, "start", 1.0, *cwEnabled); err != nil {
		log.Fatal(fmt.Errorf("Error in uploading metric", err))
	}

	for i := 0; *iterations == -1 || i < *iterations; i++ {
		err := updateAndTest(sess, i, t, time.Duration(*delay * 1e6), *cwEnabled, *key)
		if err != nil {
			log.Fatal("done with error", err)
		}
	}

	fmt.Printf("Count: %d\n", t.Count())
	fmt.Printf("Median: %2.2f ms\n", t.Quantile(0.5) * 1e-6)
	fmt.Printf("Q1: %2.2f ms\n", t.Quantile(0.25) * 1e-6)
	fmt.Printf("Q3: %2.2f ms\n", t.Quantile(0.75) * 1e-6)
	fmt.Printf("Mean: %2.2f ms\n", t.TrimmedMean(0, 1) * 1e-6)

	if err := putMetric(sess, "end", 1.0, *cwEnabled); err != nil {
		log.Fatal(fmt.Errorf("Error in uploading metric", err))
	}
}
