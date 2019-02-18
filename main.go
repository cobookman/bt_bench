package main

import (
	"flag"
	"log"
	"math/rand"
	"cloud.google.com/go/bigtable"
	"golang.org/x/net/context"
	"github.com/satori/go.uuid"
)

var (
	project   =flag.String("project", "snap-tests-217018", "gcp project")
	instance  = flag.String("instance", "testing", "gcp instance")
	tableName = flag.String("tbl", "test", "table name")
)

func populateNRows(n int, vs int, btc *bigtable.Client) error {
	rowKeys := make([]string, n, n)
	muts := make([]*bigtable.Mutation, n, n)
	for i := 0; i < n; i++ {
		uuid, err := uuid.NewV4()
		if err != nil {
			return err
		}

		mut := bigtable.NewMutation()
		value := make([]byte, vs)
		rand.Read(value)
		mut.Set("family", "column", bigtable.Now(), value)

		rowKeys[i] = uuid.String()
		muts[i] = mut
	}

	// TODO(bookman): Add saving of bulk rows
	return nil
}

func createTestData(btac *bigtable.AdminClient, btc *bigtable.Client) (*bigtable.Table, error) {
	// re-create table if already exists
	btac.DeleteTable(context.Background(), *tableName)
	if err := btac.CreateTable(context.Background(), *tableName); err != nil {
		return nil, err
	}

	tbl := btc.Open(*tableName)
	// add a bunch of rows
	for i := 0; i < 50; i++ {
		if err := populateNRows(100000, 2*1024, btc); err != nil {
			return nil, err
		}
	}
	return tbl, nil
}

func main() {
	flag.Parse()
	log.Print("Starting up")

	btac, err := bigtable.NewAdminClient(context.Background(), *project, *instance)
	if err != nil {
		log.Fatal(err)
	}
	btc, err := bigtable.NewClient(context.Background(), *project, *instance)
	if err != nil {
		log.Fatal(err)
	}

	tbl, err := createTestData(btac, btc)
	if err != nil {
		log.Fatal(err)
	}

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
