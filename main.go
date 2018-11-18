package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"contrib.go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/trace"
	"github.com/google/uuid"
)

func main() {
	projectID, err := GetProjectID()
	if err != nil {
		panic(err)
	}
	spannerDatabase := os.Getenv("SPANNER_DATABASE")
	fmt.Printf("Env SPANNER_DATABASE:%s\n", spannerDatabase)

	// Create and register a OpenCensus Stackdriver Trace exporter.
	exporter, err := stackdriver.NewExporter(stackdriver.Options{
		ProjectID: projectID,
	})
	if err != nil {
		log.Fatal(err)
	}
	trace.RegisterExporter(exporter)

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()}) // defaultでは10,000回に1回のサンプリングになっているが、リクエストが少ないと出てこないので、とりあえず全部出す

	ctx := context.Background()
	sc := CreateClient(ctx, spannerDatabase)
	ts := TweetStore{
		sc:sc,
	}

	for i := 0; i < 3600; i++ {
		ctx := context.Background()
		id := uuid.New().String()
		if err := ts.Insert(ctx, id); err != nil {
			log.Printf("failed tweet insert, err = %+v", err)
		} else {
			log.Printf("tweet insert id = %s", id)
		}
		time.Sleep(3 * time.Minute)
	}
}

