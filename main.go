package main

import (
	"fmt"
	"time"
	"cloud.google.com/go/bigtable"
	"flag"
	"github.com/satori/go.uuid"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"log"
	"math/rand"
)

var (
	project   = flag.String("project", "snap-tests-217018", "gcp project")
	instance  = flag.String("instance", "testing", "gcp instance")
	tableName = flag.String("tbl", "", "table name")
	populate  = flag.Bool("populate", true, "populate test data")
)

func populateNRows(n int, vs int, btc *bigtable.Client) error {
	log.Print("Generating mutations")
	rowKeys := make([]string, n, n)
	muts := make([]*bigtable.Mutation, n, n)
	for i := 0; i < n; i++ {
		k, err := uuid.NewV4()
		if err != nil {
			return err
		}

		mut := bigtable.NewMutation()
		value := make([]byte, vs)
		rand.Read(value)
		mut.Set("family", "column", bigtable.Now(), value)

		rowKeys[i] = k.String()
		muts[i] = mut
	}

	log.Print("apply mutation")
	tbl := btc.Open(*tableName)
	mutErrs, applyErr := tbl.ApplyBulk(context.Background(), rowKeys, muts)
	if applyErr != nil {
		return applyErr
	}
	for _, mutErr := range mutErrs {
		if mutErr != nil {
			return mutErr
		}
	}
	return nil
}

func populateTable(btac *bigtable.AdminClient, btc *bigtable.Client) error {
	log.Print("Populating table")

	// clean up table if already exists
	log.Print("Deleting old table if exists")
	btac.DeleteTable(context.Background(), *tableName)

	log.Print("Creating new table")
	if err := btac.CreateTable(context.Background(), *tableName); err != nil {
		return err
	}

	log.Print("Creating Column Family")
	if err := btac.CreateColumnFamily(context.Background(), *tableName, "family"); err != nil {
		return err
	}

	// add a bunch of rows
	log.Print("Adding rows")
	batches := 200
	rows := 5000000
	rowsPerBatch := rows / 200
	workers := 25
	bytesPerRow := 2*1024
	jobs := make(chan int, batches)
	results := make(chan error, batches)

	worker := func() {
		for j := range jobs {
			results <- populateNRows(j, bytesPerRow, btc)
		}
	}

	for w := 0; w < workers; w++ {
		go worker()
	}

	start := time.Now()
	for b := 0; b < batches; b++ {
		jobs <- rowsPerBatch
	}
	close(jobs)

	for r := 0; r < batches; r++ {
		if err := <-results; err != nil {
			return err
		}

	}
	elapsed := time.Now().Sub(start)
	fmt.Printf("Writing %d rows for each batches %d times, with %d workers took %d seconds, or %d writes per second\n",
		  rowsPerBatch, batches, workers, elapsed.Seconds(), rows/int(elapsed.Seconds()))
	return nil
}

func main() {
	// Adding line #s to log output
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Parse()
	log.Print("Starting up")

	co := option.WithCredentialsFile("service_account.json")

	log.Print("Creating admin client")
	btac, err := bigtable.NewAdminClient(context.Background(), *project, *instance, co)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("Creating normal client")
	btc, err := bigtable.NewClient(context.Background(), *project, *instance, co)
	if err != nil {
		log.Fatal(err)
	}

	if len(*tableName) == 0 {
		k, err := uuid.NewV4()
		if err != nil {
			log.Fatal(err)
		}

		ks := k.String()
		tableName = &ks
	}
	if *populate {
		if err := populateTable(btac, btc); err != nil {
			log.Fatal(err)
		}
	}

	tbl := btc.Open(*tableName)
	readOptions := []bigtable.ReadOption{
		bigtable.RowFilter(bigtable.LatestNFilter(1)),
	}
	keys := []string{
		"76b8a3923dfb9de5dc36b24227",
		"328f374e887de67fd55b2454a0",
		"328f374e887de67fd55b2454a0",
		"8d053c4cecd1cbb11a2fc8ec5d",
	}

	ctx := context.Background()
	log.Print("Reading rows")
	err = tbl.ReadRows(ctx, bigtable.RowList(keys), func(row bigtable.Row) bool {
		log.Println(row.Key())
		return true
	}, readOptions...)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Print("Done")
	}
}
